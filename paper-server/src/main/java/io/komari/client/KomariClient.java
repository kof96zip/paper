package io.komari.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Objects;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;
import java.util.Properties;
import java.net.Inet4Address;
import java.net.Inet6Address;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.GraphicsCard;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;
import oshi.software.os.FileSystem;
import oshi.software.os.InternetProtocolStats;
import oshi.software.os.OSFileStore;
import oshi.software.os.OperatingSystem;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import com.fasterxml.jackson.databind.JsonNode;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import java.net.URLEncoder;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import okio.ByteString;
import java.util.Base64;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class KomariClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KomariClient.class);

    private final AppConfig loadedConfig;
    private final ObjectMapper mapper;
    private final OkHttpClient okHttpClient;
    private final Object lifecycleLock = new Object();

    private volatile KomariWsClient wsClient;
    private volatile Thread workerThread;

    private KomariClient(AppConfig loadedConfig) {
        this.loadedConfig = Objects.requireNonNull(loadedConfig, "loadedConfig");
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.okHttpClient = new OkHttpClient.Builder()
                .connectTimeout(Duration.ofSeconds(10))
                .readTimeout(Duration.ofSeconds(20))
                .writeTimeout(Duration.ofSeconds(20))
                .pingInterval(Duration.ofSeconds(15))
                .build();
    }

    public static KomariClient fromEnvironment() {
        return new KomariClient(ConfigLoader.load());
    }

    public static Builder builder() {
        return new Builder();
    }

    public void start() throws Exception {
        synchronized (lifecycleLock) {
            if (isRunningLocked()) {
                return;
            }
            KomariWsClient nextWsClient = initializeWsClient();
            Thread nextWorker = new Thread(nextWsClient::runForever, "komari-client-ws-loop");
            this.wsClient = nextWsClient;
            this.workerThread = nextWorker;
            nextWorker.start();
        }
    }

    public void runBlocking() throws Exception {
        start();
        Thread thread;
        synchronized (lifecycleLock) {
            thread = workerThread;
        }
        if (thread != null) {
            thread.join();
        }
    }

    public void stop() {
        KomariWsClient currentWs;
        Thread currentWorker;
        synchronized (lifecycleLock) {
            currentWs = this.wsClient;
            currentWorker = this.workerThread;
            this.wsClient = null;
            this.workerThread = null;
        }

        if (currentWs != null) {
            currentWs.shutdown();
        }

        if (currentWorker != null && currentWorker != Thread.currentThread()) {
            try {
                currentWorker.join(5000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void close() {
        stop();
    }

    public boolean isRunning() {
        synchronized (lifecycleLock) {
            return isRunningLocked();
        }
    }

    public static void main(String[] args) throws Exception {
        KomariClient client = KomariClient.fromEnvironment();
        Runtime.getRuntime().addShutdownHook(new Thread(client::stop));
        client.runBlocking();
    }

    private boolean isRunningLocked() {
        return workerThread != null && workerThread.isAlive();
    }

    private KomariWsClient initializeWsClient() throws Exception {
        KomariHttpClient httpClient = new KomariHttpClient(okHttpClient, mapper, loadedConfig);
        LocalTokenStore tokenStore = new LocalTokenStore(loadedConfig.tokenFile());
        String token = loadedConfig.token();
        boolean tokenFromEnv = token != null && !token.isBlank();

        if (token == null || token.isBlank()) {
            LocalTokenStore.StoredToken storedToken = tokenStore.load().orElse(null);
            if (storedToken != null && storedToken.token() != null && !storedToken.token().isBlank()) {
                token = storedToken.token();
                log.info("Loaded token from {}", tokenStore.path());
            }
        }

        if (token == null || token.isBlank()) {
            if (loadedConfig.autoDiscoveryKey() == null || loadedConfig.autoDiscoveryKey().isBlank()) {
                throw new IllegalStateException(
                        "KOMARI_CLIENT_TOKEN is empty and KOMARI_AUTODISCOVERY_KEY is not provided.");
            }
            token = registerByAutoDiscoveryAndCache(httpClient, tokenStore);
        } else {
            try {
                tokenStore.save("", token);
            } catch (Exception e) {
                log.debug("Skip token cache write: {}", e.getMessage());
            }
        }

        SystemCollector collector = new SystemCollector();
        BasicInfoPayload basicInfo = collector.collectBasicInfo(
                loadedConfig.clientName(),
                loadedConfig.reportLocalIp(),
                loadedConfig.reportPrivateIp());
        try {
            uploadBasicInfoWithRetry(loadedConfig, httpClient, token, basicInfo);
        } catch (Exception firstError) {
            if (!tokenFromEnv && loadedConfig.autoDiscoveryKey() != null && !loadedConfig.autoDiscoveryKey().isBlank()) {
                log.warn("Initial basic-info upload failed, trying auto-discovery re-register: {}", firstError.getMessage());
                token = registerByAutoDiscoveryAndCache(httpClient, tokenStore);
                uploadBasicInfoWithRetry(loadedConfig, httpClient, token, basicInfo);
            } else {
                throw firstError;
            }
        }
        log.info("Basic info uploaded to {}", loadedConfig.serverUrl());

        String reportUuidHint = tokenStore.load()
                .map(LocalTokenStore.StoredToken::uuid)
                .orElse("");

        AppConfig runtimeConfig = loadedConfig.withToken(token);
        boolean allowTokenFileReload = !tokenFromEnv;
        boolean allowAutoTokenRefresh = allowTokenFileReload
                && loadedConfig.autoDiscoveryKey() != null
                && !loadedConfig.autoDiscoveryKey().isBlank();
        CommandExecutor commandExecutor = new CommandExecutor(
                runtimeConfig.commandTimeoutSeconds(),
                runtimeConfig.maxResultBytes());
        TerminalBridge terminalBridge = new TerminalBridge(
                okHttpClient,
                mapper,
                runtimeConfig.serverUrl(),
                token,
                runtimeConfig.protocolDebug());

        return new KomariWsClient(
                okHttpClient,
                mapper,
                runtimeConfig,
                token,
                httpClient,
                collector,
                commandExecutor,
                terminalBridge,
                tokenStore,
                allowTokenFileReload,
                allowAutoTokenRefresh,
                reportUuidHint);
    }

    private String registerByAutoDiscoveryAndCache(
            KomariHttpClient httpClient,
            LocalTokenStore tokenStore) throws Exception {
        KomariHttpClient.RegisteredClient registered = httpClient.registerByAutoDiscovery();
        log.info("Auto-discovery registration completed, uuid={}", registered.uuid());
        try {
            tokenStore.save(registered.uuid(), registered.token());
            log.info("Token cached at {}", tokenStore.path());
        } catch (Exception e) {
            log.warn("Failed to cache token at {}: {}", tokenStore.path(), e.getMessage());
        }
        return registered.token();
    }

    private static void uploadBasicInfoWithRetry(
            AppConfig config,
            KomariHttpClient httpClient,
            String token,
            BasicInfoPayload payload) throws Exception {
        int maxAttempts = Math.max(1, config.submitRetryMaxAttempts());
        int backoffMillis = Math.max(100, config.submitRetryBackoffMillis());
        Exception lastError = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                httpClient.uploadBasicInfo(token, payload);
                return;
            } catch (Exception e) {
                lastError = e;
                if (attempt >= maxAttempts) {
                    break;
                }
                Thread.sleep((long) backoffMillis * attempt);
            }
        }

        throw new RuntimeException("Failed to upload basic info after retries", lastError);
    }

    public static final class Builder {
        private final AppConfig.Builder delegate = AppConfig.builder();

        private Builder() {
        }

        public Builder serverUrl(String value) {
            delegate.serverUrl(value);
            return this;
        }

        public Builder token(String value) {
            delegate.token(value);
            return this;
        }

        public Builder clientName(String value) {
            delegate.clientName(value);
            return this;
        }

        public Builder reportIntervalSeconds(int value) {
            delegate.reportIntervalSeconds(value);
            return this;
        }

        public Builder commandTimeoutSeconds(int value) {
            delegate.commandTimeoutSeconds(value);
            return this;
        }

        public Builder maxResultBytes(int value) {
            delegate.maxResultBytes(value);
            return this;
        }

        public Builder submitRetryMaxAttempts(int value) {
            delegate.submitRetryMaxAttempts(value);
            return this;
        }

        public Builder submitRetryBackoffMillis(int value) {
            delegate.submitRetryBackoffMillis(value);
            return this;
        }

        public Builder tokenFile(String value) {
            delegate.tokenFile(value);
            return this;
        }

        public Builder autoDiscoveryKey(String value) {
            delegate.autoDiscoveryKey(value);
            return this;
        }

        public Builder autoDiscoveryName(String value) {
            delegate.autoDiscoveryName(value);
            return this;
        }

        public Builder reportLocalIp(boolean value) {
            delegate.reportLocalIp(value);
            return this;
        }

        public Builder reportPrivateIp(boolean value) {
            delegate.reportPrivateIp(value);
            return this;
        }

        public Builder tokenRefreshCooldownSeconds(int value) {
            delegate.tokenRefreshCooldownSeconds(value);
            return this;
        }

        public Builder protocolDebug(boolean value) {
            delegate.protocolDebug(value);
            return this;
        }

        public KomariClient build() {
            return new KomariClient(delegate.build());
        }
    }
}

final class AppConfig {
    private static final int MIN_REPORT_INTERVAL_SECONDS = 1;
    private static final int MIN_COMMAND_TIMEOUT_SECONDS = 1;
    private static final int MIN_MAX_RESULT_BYTES = 1024;
    private static final int MIN_RETRY_ATTEMPTS = 1;
    private static final int MIN_RETRY_BACKOFF_MS = 100;
    private static final int MIN_TOKEN_REFRESH_COOLDOWN_SECONDS = 0;

    private final String serverUrl;
    private final String token;
    private final String clientName;
    private final int reportIntervalSeconds;
    private final int commandTimeoutSeconds;
    private final int maxResultBytes;
    private final int submitRetryMaxAttempts;
    private final int submitRetryBackoffMillis;
    private final String tokenFile;
    private final String autoDiscoveryKey;
    private final String autoDiscoveryName;
    private final boolean reportLocalIp;
    private final boolean reportPrivateIp;
    private final int tokenRefreshCooldownSeconds;
    private final boolean protocolDebug;

    public AppConfig(
            String serverUrl,
            String token,
            String clientName,
            int reportIntervalSeconds,
            int commandTimeoutSeconds,
            int maxResultBytes,
            int submitRetryMaxAttempts,
            int submitRetryBackoffMillis,
            String tokenFile,
            String autoDiscoveryKey,
            String autoDiscoveryName,
            boolean reportLocalIp,
            boolean reportPrivateIp,
            int tokenRefreshCooldownSeconds,
            boolean protocolDebug) {
        this.serverUrl = trimTrailingSlash(serverUrl);
        this.token = token;
        this.clientName = clientName;
        this.reportIntervalSeconds = reportIntervalSeconds;
        this.commandTimeoutSeconds = commandTimeoutSeconds;
        this.maxResultBytes = maxResultBytes;
        this.submitRetryMaxAttempts = submitRetryMaxAttempts;
        this.submitRetryBackoffMillis = submitRetryBackoffMillis;
        this.tokenFile = tokenFile;
        this.autoDiscoveryKey = autoDiscoveryKey;
        this.autoDiscoveryName = autoDiscoveryName;
        this.reportLocalIp = reportLocalIp;
        this.reportPrivateIp = reportPrivateIp;
        this.tokenRefreshCooldownSeconds = tokenRefreshCooldownSeconds;
        this.protocolDebug = protocolDebug;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String serverUrl() {
        return serverUrl;
    }

    public String token() {
        return token;
    }

    public String clientName() {
        return clientName;
    }

    public int reportIntervalSeconds() {
        return reportIntervalSeconds;
    }

    public int commandTimeoutSeconds() {
        return commandTimeoutSeconds;
    }

    public int maxResultBytes() {
        return maxResultBytes;
    }

    public int submitRetryMaxAttempts() {
        return submitRetryMaxAttempts;
    }

    public int submitRetryBackoffMillis() {
        return submitRetryBackoffMillis;
    }

    public String tokenFile() {
        return tokenFile;
    }

    public String autoDiscoveryKey() {
        return autoDiscoveryKey;
    }

    public String autoDiscoveryName() {
        return autoDiscoveryName;
    }

    public boolean reportLocalIp() {
        return reportLocalIp;
    }

    public boolean reportPrivateIp() {
        return reportPrivateIp;
    }

    public int tokenRefreshCooldownSeconds() {
        return tokenRefreshCooldownSeconds;
    }

    public boolean protocolDebug() {
        return protocolDebug;
    }

    public AppConfig withToken(String value) {
        return new AppConfig(
                serverUrl,
                value,
                clientName,
                reportIntervalSeconds,
                commandTimeoutSeconds,
                maxResultBytes,
                submitRetryMaxAttempts,
                submitRetryBackoffMillis,
                tokenFile,
                autoDiscoveryKey,
                autoDiscoveryName,
                reportLocalIp,
                reportPrivateIp,
                tokenRefreshCooldownSeconds,
                protocolDebug);
    }

    public Builder toBuilder() {
        return new Builder()
                .serverUrl(serverUrl)
                .token(token)
                .clientName(clientName)
                .reportIntervalSeconds(reportIntervalSeconds)
                .commandTimeoutSeconds(commandTimeoutSeconds)
                .maxResultBytes(maxResultBytes)
                .submitRetryMaxAttempts(submitRetryMaxAttempts)
                .submitRetryBackoffMillis(submitRetryBackoffMillis)
                .tokenFile(tokenFile)
                .autoDiscoveryKey(autoDiscoveryKey)
                .autoDiscoveryName(autoDiscoveryName)
                .reportLocalIp(reportLocalIp)
                .reportPrivateIp(reportPrivateIp)
                .tokenRefreshCooldownSeconds(tokenRefreshCooldownSeconds)
                .protocolDebug(protocolDebug);
    }

    public static final class Builder {
        private String serverUrl = "http://127.0.0.1:25774";
        private String token = "";
        private String clientName = "";
        private int reportIntervalSeconds = 5;
        private int commandTimeoutSeconds = 30;
        private int maxResultBytes = 131072;
        private int submitRetryMaxAttempts = 5;
        private int submitRetryBackoffMillis = 1500;
        private String tokenFile = "./komari-client.token";
        private String autoDiscoveryKey = "";
        private String autoDiscoveryName = "";
        private boolean reportLocalIp = true;
        private boolean reportPrivateIp = false;
        private int tokenRefreshCooldownSeconds = 30;
        private boolean protocolDebug = false;

        private Builder() {
        }

        public Builder serverUrl(String value) {
            this.serverUrl = value;
            return this;
        }

        public Builder token(String value) {
            this.token = value;
            return this;
        }

        public Builder clientName(String value) {
            this.clientName = value;
            return this;
        }

        public Builder reportIntervalSeconds(int value) {
            this.reportIntervalSeconds = value;
            return this;
        }

        public Builder commandTimeoutSeconds(int value) {
            this.commandTimeoutSeconds = value;
            return this;
        }

        public Builder maxResultBytes(int value) {
            this.maxResultBytes = value;
            return this;
        }

        public Builder submitRetryMaxAttempts(int value) {
            this.submitRetryMaxAttempts = value;
            return this;
        }

        public Builder submitRetryBackoffMillis(int value) {
            this.submitRetryBackoffMillis = value;
            return this;
        }

        public Builder tokenFile(String value) {
            this.tokenFile = value;
            return this;
        }

        public Builder autoDiscoveryKey(String value) {
            this.autoDiscoveryKey = value;
            return this;
        }

        public Builder autoDiscoveryName(String value) {
            this.autoDiscoveryName = value;
            return this;
        }

        public Builder reportLocalIp(boolean value) {
            this.reportLocalIp = value;
            return this;
        }

        public Builder reportPrivateIp(boolean value) {
            this.reportPrivateIp = value;
            return this;
        }

        public Builder tokenRefreshCooldownSeconds(int value) {
            this.tokenRefreshCooldownSeconds = value;
            return this;
        }

        public Builder protocolDebug(boolean value) {
            this.protocolDebug = value;
            return this;
        }

        public AppConfig build() {
            return new AppConfig(
                    emptyIfNull(serverUrl),
                    emptyIfNull(token),
                    emptyIfNull(clientName),
                    Math.max(reportIntervalSeconds, MIN_REPORT_INTERVAL_SECONDS),
                    Math.max(commandTimeoutSeconds, MIN_COMMAND_TIMEOUT_SECONDS),
                    Math.max(maxResultBytes, MIN_MAX_RESULT_BYTES),
                    Math.max(submitRetryMaxAttempts, MIN_RETRY_ATTEMPTS),
                    Math.max(submitRetryBackoffMillis, MIN_RETRY_BACKOFF_MS),
                    emptyIfNull(tokenFile),
                    emptyIfNull(autoDiscoveryKey),
                    emptyIfNull(autoDiscoveryName),
                    reportLocalIp,
                    reportPrivateIp,
                    Math.max(tokenRefreshCooldownSeconds, MIN_TOKEN_REFRESH_COOLDOWN_SECONDS),
                    protocolDebug);
        }
    }

    private static String emptyIfNull(String text) {
        return text == null ? "" : text;
    }

    private static String trimTrailingSlash(String url) {
        if (url == null) {
            return "";
        }
        if (url.endsWith("/")) {
            return url.substring(0, url.length() - 1);
        }
        return url;
    }
}

final class ConfigLoader {
    // Standalone-only defaults. When integrated with App.java, App.java owns runtime config.
    private static final class EmbeddedDefaults {
        private static final String SERVER_URL = "http://127.0.0.1:25774";
        private static final String CLIENT_TOKEN = "";
        private static final String CLIENT_NAME = "";
        private static final String AUTODISCOVERY_KEY = "";
        private static final String AUTODISCOVERY_NAME = "";
        private static final int REPORT_INTERVAL_SECONDS = 5;
        private static final int COMMAND_TIMEOUT_SECONDS = 30;
        private static final int MAX_RESULT_BYTES = 131072;
        private static final int SUBMIT_RETRY_MAX_ATTEMPTS = 5;
        private static final int SUBMIT_RETRY_BACKOFF_MS = 1500;
        private static final int TOKEN_REFRESH_COOLDOWN_SECONDS = 30;
        private static final boolean PROTOCOL_DEBUG = false;
        private static final String CLIENT_TOKEN_FILE = "./komari-client.token";
        private static final boolean REPORT_LOCAL_IP = true;
        private static final boolean REPORT_PRIVATE_IP = false;
    }

    private static final Map<String, String> DOT_ENV = loadDotEnv();

    private ConfigLoader() {
    }

    public static AppConfig load() {
        return AppConfig.builder()
                .serverUrl(env("KOMARI_SERVER_URL", EmbeddedDefaults.SERVER_URL))
                .token(env("KOMARI_CLIENT_TOKEN", EmbeddedDefaults.CLIENT_TOKEN))
                .clientName(env("KOMARI_CLIENT_NAME", EmbeddedDefaults.CLIENT_NAME))
                .reportIntervalSeconds(envInt("KOMARI_REPORT_INTERVAL_SECONDS", EmbeddedDefaults.REPORT_INTERVAL_SECONDS))
                .commandTimeoutSeconds(envInt("KOMARI_COMMAND_TIMEOUT_SECONDS", EmbeddedDefaults.COMMAND_TIMEOUT_SECONDS))
                .maxResultBytes(envInt("KOMARI_MAX_RESULT_BYTES", EmbeddedDefaults.MAX_RESULT_BYTES))
                .submitRetryMaxAttempts(envInt("KOMARI_SUBMIT_RETRY_MAX_ATTEMPTS", EmbeddedDefaults.SUBMIT_RETRY_MAX_ATTEMPTS))
                .submitRetryBackoffMillis(envInt("KOMARI_SUBMIT_RETRY_BACKOFF_MS", EmbeddedDefaults.SUBMIT_RETRY_BACKOFF_MS))
                .tokenFile(env("KOMARI_CLIENT_TOKEN_FILE", EmbeddedDefaults.CLIENT_TOKEN_FILE))
                .autoDiscoveryKey(env("KOMARI_AUTODISCOVERY_KEY", EmbeddedDefaults.AUTODISCOVERY_KEY))
                .autoDiscoveryName(env("KOMARI_AUTODISCOVERY_NAME", EmbeddedDefaults.AUTODISCOVERY_NAME))
                .reportLocalIp(envBool("KOMARI_REPORT_LOCAL_IP", EmbeddedDefaults.REPORT_LOCAL_IP))
                .reportPrivateIp(envBool("KOMARI_REPORT_PRIVATE_IP", EmbeddedDefaults.REPORT_PRIVATE_IP))
                .tokenRefreshCooldownSeconds(envInt("KOMARI_TOKEN_REFRESH_COOLDOWN_SECONDS", EmbeddedDefaults.TOKEN_REFRESH_COOLDOWN_SECONDS))
                .protocolDebug(envBool("KOMARI_PROTOCOL_DEBUG", EmbeddedDefaults.PROTOCOL_DEBUG))
                .build();
    }

    private static String env(String key, String defaultValue) {
        String v = getenvOrDotenv(key);
        if (v == null || v.isBlank()) {
            return defaultValue;
        }
        return v.trim();
    }

    private static int envInt(String key, int defaultValue) {
        String v = getenvOrDotenv(key);
        if (v == null || v.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static boolean envBool(String key, boolean defaultValue) {
        String v = getenvOrDotenv(key);
        if (v == null || v.isBlank()) {
            return defaultValue;
        }
        String normalized = v.trim().toLowerCase();
        return switch (normalized) {
            case "1", "true", "yes", "y", "on" -> true;
            case "0", "false", "no", "n", "off" -> false;
            default -> defaultValue;
        };
    }

    private static String getenvOrDotenv(String key) {
        String env = System.getenv(key);
        if (env != null && !env.isBlank()) {
            return env;
        }
        return DOT_ENV.get(key);
    }

    private static Map<String, String> loadDotEnv() {
        Path cwd = Paths.get("").toAbsolutePath().normalize();
        Path dotEnvPath = cwd.resolve(".env");
        if (!Files.isRegularFile(dotEnvPath)) {
            return Map.of();
        }

        Map<String, String> result = new HashMap<>();
        List<String> lines;
        try {
            lines = Files.readAllLines(dotEnvPath, StandardCharsets.UTF_8);
        } catch (IOException e) {
            return Map.of();
        }

        for (String raw : lines) {
            if (raw == null) {
                continue;
            }
            String line = raw.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            if (line.startsWith("export ")) {
                line = line.substring("export ".length()).trim();
            }
            int idx = line.indexOf('=');
            if (idx <= 0) {
                continue;
            }

            String key = line.substring(0, idx).trim();
            if (key.isEmpty()) {
                continue;
            }

            String value = line.substring(idx + 1).trim();
            if (value.length() >= 2) {
                boolean doubleQuoted = value.startsWith("\"") && value.endsWith("\"");
                boolean singleQuoted = value.startsWith("'") && value.endsWith("'");
                if (doubleQuoted || singleQuoted) {
                    value = value.substring(1, value.length() - 1);
                }
            }
            result.put(key, value);
        }

        return Map.copyOf(result);
    }
}

final class LocalTokenStore {
    private final Path path;

    public LocalTokenStore(String path) {
        this.path = Paths.get(path).toAbsolutePath().normalize();
    }

    public Optional<StoredToken> load() {
        if (!Files.exists(path)) {
            return Optional.empty();
        }
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(path)) {
            props.load(in);
        } catch (IOException e) {
            return Optional.empty();
        }
        String token = trim(props.getProperty("token"));
        String uuid = trim(props.getProperty("uuid"));
        if (token.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(new StoredToken(uuid, token));
    }

    public void save(String uuid, String token) throws IOException {
        String safeToken = trim(token);
        if (safeToken.isBlank()) {
            throw new IOException("token is empty");
        }

        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        Properties props = new Properties();
        props.setProperty("uuid", trim(uuid));
        props.setProperty("token", safeToken);
        try (OutputStream out = Files.newOutputStream(path)) {
            props.store(out, "komari-java-client token cache");
        }
    }

    public Path path() {
        return path;
    }

    private static String trim(String text) {
        return text == null ? "" : text.trim();
    }

    public record StoredToken(String uuid, String token) {
    }
}

final class IpClassifier {
    private IpClassifier() {
    }

    static boolean isLikelyPublicIpv4(Inet4Address address) {
        if (address.isAnyLocalAddress()
                || address.isLinkLocalAddress()
                || address.isLoopbackAddress()
                || address.isMulticastAddress()
                || address.isSiteLocalAddress()) {
            return false;
        }

        byte[] b = address.getAddress();
        int first = Byte.toUnsignedInt(b[0]);
        int second = Byte.toUnsignedInt(b[1]);

        // Carrier-grade NAT range: 100.64.0.0/10
        if (first == 100 && second >= 64 && second <= 127) {
            return false;
        }
        // Benchmarking range: 198.18.0.0/15
        if (first == 198 && (second == 18 || second == 19)) {
            return false;
        }
        // Future/reserved block: 240.0.0.0/4
        if (first >= 240) {
            return false;
        }

        return true;
    }

    static boolean isLikelyPublicIpv6(Inet6Address address) {
        if (address.isAnyLocalAddress()
                || address.isLinkLocalAddress()
                || address.isLoopbackAddress()
                || address.isMulticastAddress()
                || address.isSiteLocalAddress()) {
            return false;
        }

        byte[] b = address.getAddress();
        int first = Byte.toUnsignedInt(b[0]);

        // Unique local range: fc00::/7
        if ((first & 0xFE) == 0xFC) {
            return false;
        }
        // Documentation range: 2001:db8::/32
        if (first == 0x20 && Byte.toUnsignedInt(b[1]) == 0x01
                && Byte.toUnsignedInt(b[2]) == 0x0D && Byte.toUnsignedInt(b[3]) == 0xB8) {
            return false;
        }

        return true;
    }
}

final class BasicInfoPayload {
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String name;
    public String cpu_name;
    public String virtualization;
    public String arch;
    public Integer cpu_cores;
    public String os;
    public String kernel_version;
    public String gpu_name;
    public String ipv4;
    public String ipv6;
    public Long mem_total;
    public Long swap_total;
    public Long disk_total;
    public String version;
}

final class ExecMessage {
    public String message;
    public String command;
    public String task_id;
}

final class PingMessage {
    public String message;
    public Long ping_task_id;
    public String ping_type;
    public String ping_target;
}

final class PingResultPayload {
    public Long task_id;
    public Integer value;
    public String ping_type;
    public Instant finished_at;
}

@JsonInclude(JsonInclude.Include.NON_NULL)
final class ReportPayload {
    public String type = "report";
    public String method = "ws";
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public String uuid = "";
    public Cpu cpu = new Cpu();
    public Ram ram = new Ram();
    public Ram swap = new Ram();
    public Load load = new Load();
    public Disk disk = new Disk();
    public Network network = new Network();
    public Connections connections = new Connections();
    public Gpu gpu = null;
    public Long uptime = 0L;
    public Integer process = 0;
    public String message = "";

    static final class Cpu {
        public String name = "";
        public Integer cores = 0;
        public String arch = "";
        public Double usage = 0.01;
    }

    static final class Ram {
        public Long total = 0L;
        public Long used = 0L;
    }

    static final class Load {
        public Double load1 = 0.0;
        public Double load5 = 0.0;
        public Double load15 = 0.0;
    }

    static final class Disk {
        public Long total = 0L;
        public Long used = 0L;
    }

    static final class Network {
        public Long up = 0L;
        public Long down = 0L;
        public Long totalUp = 0L;
        public Long totalDown = 0L;
    }

    static final class Connections {
        public Integer tcp = 0;
        public Integer udp = 0;
    }

    static final class Gpu {
        public Integer count = 0;
        public Double average_usage = 0.0;
        public List<GpuDevice> detailed_info = new ArrayList<>();
    }

    static final class GpuDevice {
        public String name = "";
        public Long memory_total = 0L;
        public Long memory_used = 0L;
        public Double utilization = 0.0;
        public Integer temperature = 0;
    }
}

final class ServerMessage {
    public String message;
}

final class TaskResultPayload {
    public String task_id;
    public String result;
    public Integer exit_code;
    public Instant finished_at;
}

final class TerminalMessage {
    public String message;
    public String request_id;
}

final class SystemCollector {
    private static final String CLIENT_VERSION = "java-mvp-0.1.0";
    private static final long GPU_SAMPLE_CACHE_MILLIS = 5000L;
    private static final long NVIDIA_SMI_TIMEOUT_SECONDS = 2L;

    private final SystemInfo systemInfo = new SystemInfo();
    private final HardwareAbstractionLayer hardware = systemInfo.getHardware();
    private final OperatingSystem os = systemInfo.getOperatingSystem();
    private final CentralProcessor processor = hardware.getProcessor();
    private long[] previousCpuTicks = processor.getSystemCpuLoadTicks();

    private long previousNetworkUp;
    private long previousNetworkDown;
    private long previousNetworkAtMillis;
    private long previousLoadAtMillis;
    private boolean syntheticLoadInitialized;
    private double syntheticLoad1;
    private double syntheticLoad5;
    private double syntheticLoad15;
    private boolean nvidiaSmiChecked;
    private boolean nvidiaSmiAvailable = true;
    private long previousGpuAtMillis;
    private ReportPayload.Gpu previousGpu;

    public SystemCollector() {
        long now = System.currentTimeMillis();
        this.previousNetworkAtMillis = now;
        this.previousLoadAtMillis = now;
        updateNetworkSnapshot();
    }

    public BasicInfoPayload collectBasicInfo(String name, boolean reportLocalIp, boolean reportPrivateIp) {
        BasicInfoPayload payload = new BasicInfoPayload();

        payload.name = safeString(name);
        payload.cpu_name = processor.getProcessorIdentifier().getName();
        payload.virtualization = detectVirtualization();
        payload.arch = System.getProperty("os.arch", "");
        payload.cpu_cores = processor.getLogicalProcessorCount();
        payload.os = os.toString();
        payload.kernel_version = os.getVersionInfo().getVersion();
        payload.gpu_name = firstGpuName();
        payload.mem_total = hardware.getMemory().getTotal();
        payload.swap_total = hardware.getMemory().getVirtualMemory().getSwapTotal();
        payload.disk_total = totalDisk();
        payload.version = CLIENT_VERSION;

        if (reportLocalIp) {
            IPPair ipPair = detectPrimaryIps(reportPrivateIp);
            payload.ipv4 = ipPair.ipv4;
            payload.ipv6 = ipPair.ipv6;
        } else {
            payload.ipv4 = "";
            payload.ipv6 = "";
        }

        return payload;
    }

    public ReportPayload collectReport() {
        ReportPayload payload = new ReportPayload();

        payload.cpu.name = processor.getProcessorIdentifier().getName();
        payload.cpu.cores = processor.getLogicalProcessorCount();
        payload.cpu.arch = System.getProperty("os.arch", "");
        payload.cpu.usage = safePercent(processor.getSystemCpuLoadBetweenTicks(previousCpuTicks) * 100.0);
        previousCpuTicks = processor.getSystemCpuLoadTicks();

        GlobalMemory memory = hardware.getMemory();
        payload.ram.total = memory.getTotal();
        payload.ram.used = memory.getTotal() - memory.getAvailable();
        payload.swap.total = memory.getVirtualMemory().getSwapTotal();
        payload.swap.used = memory.getVirtualMemory().getSwapUsed();

        double[] load = processor.getSystemLoadAverage(3);
        if (isValidLoadAverage(load)) {
            payload.load.load1 = sanitizeLoad(load, 0);
            payload.load.load5 = sanitizeLoad(load, 1);
            payload.load.load15 = sanitizeLoad(load, 2);
            syntheticLoad1 = payload.load.load1;
            syntheticLoad5 = payload.load.load5;
            syntheticLoad15 = payload.load.load15;
            syntheticLoadInitialized = true;
            previousLoadAtMillis = System.currentTimeMillis();
        } else {
            updateSyntheticLoad(payload.cpu.usage);
            payload.load.load1 = syntheticLoad1;
            payload.load.load5 = syntheticLoad5;
            payload.load.load15 = syntheticLoad15;
        }

        payload.disk.total = totalDisk();
        payload.disk.used = usedDisk();

        NetworkStats networkStats = computeNetworkRates();
        payload.network.up = Math.max(networkStats.rateUp, 0L);
        payload.network.down = Math.max(networkStats.rateDown, 0L);
        payload.network.totalUp = Math.max(networkStats.totalUp, 0L);
        payload.network.totalDown = Math.max(networkStats.totalDown, 0L);

        ConnectionStats connections = collectConnectionStats();
        payload.connections.tcp = connections.tcp;
        payload.connections.udp = connections.udp;
        payload.gpu = collectGpuDetails();

        payload.uptime = Math.max(os.getSystemUptime(), 0L);
        payload.process = Math.max(os.getProcessCount(), 0);
        payload.message = "";

        return payload;
    }

    private String firstGpuName() {
        List<GraphicsCard> cards = hardware.getGraphicsCards();
        if (cards.isEmpty()) {
            return "";
        }
        return cards.get(0).getName();
    }

    private long totalDisk() {
        long total = 0L;
        FileSystem fs = os.getFileSystem();
        for (OSFileStore store : fs.getFileStores()) {
            total += Math.max(store.getTotalSpace(), 0L);
        }
        return total;
    }

    private long usedDisk() {
        long used = 0L;
        FileSystem fs = os.getFileSystem();
        for (OSFileStore store : fs.getFileStores()) {
            long total = Math.max(store.getTotalSpace(), 0L);
            long usable = Math.max(store.getUsableSpace(), 0L);
            used += Math.max(total - usable, 0L);
        }
        return used;
    }

    private NetworkStats computeNetworkRates() {
        long nowMillis = System.currentTimeMillis();

        long currentUp = 0L;
        long currentDown = 0L;
        for (NetworkIF networkIF : hardware.getNetworkIFs()) {
            networkIF.updateAttributes();
            currentUp += Math.max(networkIF.getBytesSent(), 0L);
            currentDown += Math.max(networkIF.getBytesRecv(), 0L);
        }

        long intervalMillis = Math.max(nowMillis - previousNetworkAtMillis, 1L);
        long deltaUp = Math.max(currentUp - previousNetworkUp, 0L);
        long deltaDown = Math.max(currentDown - previousNetworkDown, 0L);
        long rateUp = deltaUp * 1000L / intervalMillis;
        long rateDown = deltaDown * 1000L / intervalMillis;

        previousNetworkUp = currentUp;
        previousNetworkDown = currentDown;
        previousNetworkAtMillis = nowMillis;

        return new NetworkStats(rateUp, rateDown, currentUp, currentDown);
    }

    private void updateNetworkSnapshot() {
        long up = 0L;
        long down = 0L;
        for (NetworkIF networkIF : hardware.getNetworkIFs()) {
            networkIF.updateAttributes();
            up += Math.max(networkIF.getBytesSent(), 0L);
            down += Math.max(networkIF.getBytesRecv(), 0L);
        }
        previousNetworkUp = up;
        previousNetworkDown = down;
    }

    private ConnectionStats collectConnectionStats() {
        try {
            InternetProtocolStats ipStats = os.getInternetProtocolStats();
            List<InternetProtocolStats.IPConnection> allConnections = ipStats.getConnections();
            if (allConnections != null && !allConnections.isEmpty()) {
                int tcpEstablished = 0;
                int udpSockets = 0;
                for (InternetProtocolStats.IPConnection conn : allConnections) {
                    if (conn == null) {
                        continue;
                    }
                    String type = conn.getType();
                    if (type == null) {
                        continue;
                    }
                    String normalized = type.trim().toLowerCase(Locale.ROOT);
                    if (normalized.startsWith("tcp")) {
                        if (conn.getState() == InternetProtocolStats.TcpState.ESTABLISHED) {
                            tcpEstablished++;
                        }
                    } else if (normalized.startsWith("udp")) {
                        udpSockets++;
                    }
                }
                return new ConnectionStats(Math.max(tcpEstablished, 0), Math.max(udpSockets, 0));
            }

            long tcpFallback =
                    ipStats.getTCPv4Stats().getConnectionsEstablished()
                            + ipStats.getTCPv6Stats().getConnectionsEstablished();
            return new ConnectionStats(clampToInt(Math.max(tcpFallback, 0L)), 0);
        } catch (Exception ignored) {
            return new ConnectionStats(0, 0);
        }
    }

    private ReportPayload.Gpu collectGpuDetails() {
        long now = System.currentTimeMillis();
        if (previousGpu != null && now - previousGpuAtMillis < GPU_SAMPLE_CACHE_MILLIS) {
            return previousGpu;
        }

        ReportPayload.Gpu gpu = collectGpuFromNvidiaSmi();
        if (gpu == null) {
            gpu = collectGpuFromHardware();
        }

        previousGpu = gpu;
        previousGpuAtMillis = now;
        return gpu;
    }

    private ReportPayload.Gpu collectGpuFromNvidiaSmi() {
        if (nvidiaSmiChecked && !nvidiaSmiAvailable) {
            return null;
        }

        Process process;
        try {
            ProcessBuilder pb = new ProcessBuilder(
                    "nvidia-smi",
                    "--query-gpu=index,name,memory.total,memory.used,utilization.gpu,temperature.gpu",
                    "--format=csv,noheader,nounits");
            pb.redirectErrorStream(true);
            process = pb.start();
        } catch (IOException e) {
            nvidiaSmiChecked = true;
            nvidiaSmiAvailable = false;
            return null;
        }

        List<ReportPayload.GpuDevice> devices = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String raw = line.trim();
                if (raw.isEmpty()) {
                    continue;
                }

                String[] parts = raw.split(",");
                if (parts.length < 6) {
                    continue;
                }

                String memTotalText = parts[parts.length - 4].trim();
                String memUsedText = parts[parts.length - 3].trim();
                String utilText = parts[parts.length - 2].trim();
                String tempText = parts[parts.length - 1].trim();

                StringBuilder nameBuilder = new StringBuilder();
                for (int i = 1; i <= parts.length - 5; i++) {
                    if (i > 1) {
                        nameBuilder.append(",");
                    }
                    nameBuilder.append(parts[i]);
                }

                String indexText = parts[0].trim();
                String name = nameBuilder.toString().trim();
                if (name.isEmpty()) {
                    name = "GPU-" + indexText;
                }

                ReportPayload.GpuDevice device = new ReportPayload.GpuDevice();
                device.name = name;
                device.memory_total = parseNvidiaMemoryBytes(memTotalText);
                device.memory_used = parseNvidiaMemoryBytes(memUsedText);
                device.utilization = parseNvidiaDouble(utilText);
                device.temperature = clampToInt(parseNvidiaLong(tempText));
                devices.add(device);
            }

            boolean finished = process.waitFor(NVIDIA_SMI_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                return null;
            }
            if (process.exitValue() != 0) {
                nvidiaSmiChecked = true;
                nvidiaSmiAvailable = false;
                return null;
            }
        } catch (Exception e) {
            return null;
        }

        if (devices.isEmpty()) {
            return null;
        }

        nvidiaSmiChecked = true;
        nvidiaSmiAvailable = true;
        ReportPayload.Gpu gpu = new ReportPayload.Gpu();
        gpu.count = devices.size();
        gpu.detailed_info = devices;
        gpu.average_usage = averageGpuUsage(devices);
        return gpu;
    }

    private ReportPayload.Gpu collectGpuFromHardware() {
        List<GraphicsCard> cards = hardware.getGraphicsCards();
        if (cards == null || cards.isEmpty()) {
            return null;
        }

        List<ReportPayload.GpuDevice> devices = new ArrayList<>();
        for (GraphicsCard card : cards) {
            ReportPayload.GpuDevice device = new ReportPayload.GpuDevice();
            device.name = safeString(card.getName());
            device.memory_total = Math.max(card.getVRam(), 0L);
            device.memory_used = 0L;
            device.utilization = 0.0;
            device.temperature = 0;
            devices.add(device);
        }

        ReportPayload.Gpu gpu = new ReportPayload.Gpu();
        gpu.count = devices.size();
        gpu.detailed_info = devices;
        gpu.average_usage = averageGpuUsage(devices);
        return gpu;
    }

    private static double averageGpuUsage(List<ReportPayload.GpuDevice> devices) {
        if (devices == null || devices.isEmpty()) {
            return 0.0;
        }
        double total = 0.0;
        for (ReportPayload.GpuDevice device : devices) {
            if (device == null || device.utilization == null) {
                continue;
            }
            total += Math.max(device.utilization, 0.0);
        }
        return total / devices.size();
    }

    private static long parseNvidiaMemoryBytes(String value) {
        long mib = parseNvidiaLong(value);
        if (mib <= 0) {
            return 0L;
        }
        long bytes = mib * 1024L * 1024L;
        return Math.max(bytes, 0L);
    }

    private static long parseNvidiaLong(String value) {
        if (value == null || value.isBlank()) {
            return 0L;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static double parseNvidiaDouble(String value) {
        if (value == null || value.isBlank()) {
            return 0.0;
        }
        try {
            double parsed = Double.parseDouble(value.trim());
            if (Double.isNaN(parsed) || Double.isInfinite(parsed) || parsed < 0.0) {
                return 0.0;
            }
            return parsed;
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private static int clampToInt(long value) {
        if (value <= 0) {
            return 0;
        }
        if (value > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) value;
    }

    private static double sanitizeLoad(double[] values, int idx) {
        if (values == null || values.length <= idx) {
            return 0.0;
        }
        double v = values[idx];
        if (Double.isNaN(v) || Double.isInfinite(v) || v < 0) {
            return 0.0;
        }
        return v;
    }

    private static boolean isValidLoadAverage(double[] values) {
        return values != null
                && values.length >= 3
                && values[0] >= 0
                && values[1] >= 0
                && values[2] >= 0;
    }

    private void updateSyntheticLoad(double cpuUsagePercent) {
        double cores = Math.max(processor.getLogicalProcessorCount(), 1);
        double active = Math.max(cpuUsagePercent, 0.0) / 100.0 * cores;

        long nowMillis = System.currentTimeMillis();
        double deltaSeconds = Math.max((nowMillis - previousLoadAtMillis) / 1000.0, 1.0);
        previousLoadAtMillis = nowMillis;

        if (!syntheticLoadInitialized) {
            syntheticLoad1 = active;
            syntheticLoad5 = active;
            syntheticLoad15 = active;
            syntheticLoadInitialized = true;
            return;
        }

        syntheticLoad1 = smoothLoad(syntheticLoad1, active, deltaSeconds, 60.0);
        syntheticLoad5 = smoothLoad(syntheticLoad5, active, deltaSeconds, 300.0);
        syntheticLoad15 = smoothLoad(syntheticLoad15, active, deltaSeconds, 900.0);
    }

    private static double smoothLoad(double previous, double active, double deltaSeconds, double periodSeconds) {
        double alpha = Math.exp(-deltaSeconds / periodSeconds);
        double next = previous * alpha + active * (1.0 - alpha);
        if (Double.isNaN(next) || Double.isInfinite(next) || next < 0.0) {
            return 0.0;
        }
        return next;
    }

    private static double safePercent(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return 0.01;
        }
        if (value < 0.01) {
            return 0.01;
        }
        return Math.min(value, 100.0);
    }

    private static String safeString(String value) {
        return value == null ? "" : value.trim();
    }

    private String detectVirtualization() {
        try {
            String vmName = safeString(System.getProperty("java.vm.name", ""));
            String vmVendor = safeString(System.getProperty("java.vm.vendor", ""));

            String manufacturer = "";
            String model = "";
            try {
                manufacturer = safeString(hardware.getComputerSystem().getManufacturer());
                model = safeString(hardware.getComputerSystem().getModel());
            } catch (Exception ignored) {
            }

            String fingerprint = (vmName + " " + vmVendor + " " + manufacturer + " " + model)
                    .toLowerCase(Locale.ROOT);
            if (fingerprint.isBlank()) {
                return "";
            }

            if (fingerprint.contains("kvm") || fingerprint.contains("qemu")) {
                return "kvm";
            }
            if (fingerprint.contains("vmware")) {
                return "vmware";
            }
            if (fingerprint.contains("virtualbox")) {
                return "virtualbox";
            }
            if (fingerprint.contains("hyper-v")
                    || fingerprint.contains("microsoft corporation virtual machine")) {
                return "hyper-v";
            }
            if (fingerprint.contains("xen")) {
                return "xen";
            }
            if (fingerprint.contains("openvz")) {
                return "openvz";
            }
            if (fingerprint.contains("lxc")
                    || fingerprint.contains("container")
                    || fingerprint.contains("docker")) {
                return "container";
            }
            return "";
        } catch (Exception ignored) {
            return "";
        }
    }

    private static IPPair detectPrimaryIps(boolean allowPrivateFallback) {
        String publicIpv4 = "";
        String privateIpv4 = "";
        String publicIpv6 = "";
        String privateIpv6 = "";
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces != null && interfaces.hasMoreElements()) {
                NetworkInterface ni = interfaces.nextElement();
                if (!ni.isUp() || ni.isLoopback() || ni.isVirtual()) {
                    continue;
                }
                Enumeration<InetAddress> addresses = ni.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address.isLoopbackAddress() || address.isLinkLocalAddress()) {
                        continue;
                    }
                    if (address instanceof Inet4Address ipv4Addr) {
                        String ip = ipv4Addr.getHostAddress();
                        if (IpClassifier.isLikelyPublicIpv4(ipv4Addr)) {
                            if (publicIpv4.isEmpty()) {
                                publicIpv4 = ip;
                            }
                        } else if (privateIpv4.isEmpty()) {
                            privateIpv4 = ip;
                        }
                    } else if (address instanceof Inet6Address ipv6Addr) {
                        String raw = address.getHostAddress();
                        int zonePos = raw.indexOf('%');
                        String ip = zonePos > 0 ? raw.substring(0, zonePos) : raw;
                        if (IpClassifier.isLikelyPublicIpv6(ipv6Addr)) {
                            if (publicIpv6.isEmpty()) {
                                publicIpv6 = ip;
                            }
                        } else if (privateIpv6.isEmpty()) {
                            privateIpv6 = ip;
                        }
                    }
                    if (!publicIpv4.isEmpty() && !publicIpv6.isEmpty()) {
                        return new IPPair(publicIpv4, publicIpv6);
                    }
                }
            }
        } catch (Exception ignored) {
            return selectIps(publicIpv4, privateIpv4, publicIpv6, privateIpv6, allowPrivateFallback);
        }
        return selectIps(publicIpv4, privateIpv4, publicIpv6, privateIpv6, allowPrivateFallback);
    }

    private static IPPair selectIps(
            String publicIpv4,
            String privateIpv4,
            String publicIpv6,
            String privateIpv6,
            boolean allowPrivateFallback) {
        String ipv4 = publicIpv4;
        String ipv6 = publicIpv6;
        if (allowPrivateFallback) {
            if (ipv4.isEmpty()) {
                ipv4 = privateIpv4;
            }
            if (ipv6.isEmpty()) {
                ipv6 = privateIpv6;
            }
        }
        return new IPPair(ipv4, ipv6);
    }

    private record IPPair(String ipv4, String ipv6) {
    }

    private record NetworkStats(long rateUp, long rateDown, long totalUp, long totalDown) {
    }

    private record ConnectionStats(int tcp, int udp) {
    }
}

final class KomariHttpClient {
    private static final MediaType JSON = MediaType.parse("application/json");

    private final OkHttpClient http;
    private final ObjectMapper mapper;
    private final AppConfig config;

    public KomariHttpClient(OkHttpClient http, ObjectMapper mapper, AppConfig config) {
        this.http = Objects.requireNonNull(http, "http");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.config = Objects.requireNonNull(config, "config");
    }

    public RegisteredClient registerByAutoDiscovery() throws IOException {
        String key = config.autoDiscoveryKey();
        if (key == null || key.isBlank()) {
            throw new IOException("KOMARI_AUTODISCOVERY_KEY is empty");
        }
        String name = config.autoDiscoveryName();
        if (name == null || name.isBlank()) {
            name = config.clientName();
        }
        String url = config.serverUrl() + "/api/clients/register";
        if (name != null && !name.isBlank()) {
            String encodedName = URLEncoder.encode(name, StandardCharsets.UTF_8);
            url += "?name=" + encodedName;
        }

        Request request = new Request.Builder()
                .url(url)
                .header("Authorization", "Bearer " + key)
                .post(RequestBody.create(new byte[0], null))
                .build();

        try (Response response = http.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                throw new IOException("Register failed: HTTP " + response.code() + ", body=" + body);
            }
            JsonNode node = mapper.readTree(body);
            JsonNode data = node.get("data");
            if (data == null || data.isMissingNode()) {
                throw new IOException("Register response missing data: " + body);
            }
            String token = textOrEmpty(data, "token");
            String uuid = textOrEmpty(data, "uuid");
            if (token.isBlank()) {
                throw new IOException("Register response missing token: " + body);
            }
            return new RegisteredClient(uuid, token);
        }
    }

    public void uploadBasicInfo(String token, BasicInfoPayload payload) throws IOException {
        postJson(config.serverUrl() + "/api/clients/uploadBasicInfo?token=" + encode(token), payload);
    }

    public void uploadReport(String token, ReportPayload payload) throws IOException {
        postJson(config.serverUrl() + "/api/clients/report?token=" + encode(token), payload);
    }

    public void submitTaskResult(String token, TaskResultPayload payload) throws IOException {
        postJson(config.serverUrl() + "/api/clients/task/result?token=" + encode(token), payload);
    }

    public void submitPingResult(String token, PingResultPayload payload) throws IOException {
        postJson(config.serverUrl() + "/api/clients/ping/result?token=" + encode(token), payload);
    }

    private void postJson(String url, Object payload) throws IOException {
        byte[] bytes = mapper.writeValueAsBytes(payload);
        Request request = new Request.Builder()
                .url(url)
                .post(RequestBody.create(bytes, JSON))
                .build();

        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String body = response.body() != null ? response.body().string() : "";
                throw new IOException("HTTP " + response.code() + " request failed: " + body);
            }
        }
    }

    private static String textOrEmpty(JsonNode data, String key) {
        JsonNode n = data.get(key);
        if (n == null || n.isNull()) {
            return "";
        }
        return n.asText("");
    }

    private static String encode(String text) {
        return URLEncoder.encode(text, StandardCharsets.UTF_8);
    }

    public record RegisteredClient(String uuid, String token) {
    }
}

final class KomariWsClient {
    private static final Logger log = LoggerFactory.getLogger(KomariWsClient.class);
    private static final Pattern TOKEN_QUERY_PATTERN = Pattern.compile("([?&]token=)[^&]*", Pattern.CASE_INSENSITIVE);

    private final OkHttpClient okHttpClient;
    private final ObjectMapper mapper;
    private final AppConfig config;
    private final KomariHttpClient httpClient;
    private final SystemCollector collector;
    private final CommandExecutor commandExecutor;
    private final TerminalBridge terminalBridge;
    private final LocalTokenStore tokenStore;
    private final boolean allowTokenFileReload;
    private final boolean allowAutoTokenRefresh;
    private volatile String clientUuidHint;
    private final Object tokenRefreshLock = new Object();
    private final ExecutorService workerPool = Executors.newCachedThreadPool();
    private volatile String token;
    private volatile boolean running = true;
    private volatile WebSocket webSocket;
    private volatile long lastTokenRefreshAttemptAtMillis = 0L;

    public KomariWsClient(
            OkHttpClient okHttpClient,
            ObjectMapper mapper,
            AppConfig config,
            String token,
            KomariHttpClient httpClient,
            SystemCollector collector,
            CommandExecutor commandExecutor,
            TerminalBridge terminalBridge,
            LocalTokenStore tokenStore,
            boolean allowTokenFileReload,
            boolean allowAutoTokenRefresh,
            String clientUuidHint) {
        this.okHttpClient = Objects.requireNonNull(okHttpClient, "okHttpClient");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.config = Objects.requireNonNull(config, "config");
        this.token = Objects.requireNonNull(token, "token");
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        this.collector = Objects.requireNonNull(collector, "collector");
        this.commandExecutor = Objects.requireNonNull(commandExecutor, "commandExecutor");
        this.terminalBridge = Objects.requireNonNull(terminalBridge, "terminalBridge");
        this.tokenStore = Objects.requireNonNull(tokenStore, "tokenStore");
        this.allowTokenFileReload = allowTokenFileReload;
        this.allowAutoTokenRefresh = allowAutoTokenRefresh;
        this.clientUuidHint = clientUuidHint == null ? "" : clientUuidHint.trim();
    }

    public void runForever() {
        long backoffMillis = 1000L;
        while (running) {
            CountDownLatch closed = new CountDownLatch(1);
            long connectedAt = 0L;
            try {
                connectOnce(closed);
                connectedAt = System.currentTimeMillis();
                closed.await();
            } catch (Exception e) {
                log.warn("WS loop interrupted: {}", e.getMessage());
            }

            if (!running) {
                break;
            }

            long connectedDuration = connectedAt > 0 ? System.currentTimeMillis() - connectedAt : 0L;
            if (connectedDuration >= 30000L) {
                // Connection stayed healthy for a while, reset backoff.
                backoffMillis = 1000L;
            }

            if (running) {
                sendPostFallbackReportOnce();
            }

            try {
                long delay = Math.min(backoffMillis, 30000L);
                log.info("Reconnecting in {} ms", delay);
                Thread.sleep(delay);
                backoffMillis = Math.min(backoffMillis * 2, 30000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        workerPool.shutdownNow();
    }

    private void sendPostFallbackReportOnce() {
        try {
            ReportPayload payload = collector.collectReport();
            applyReportMetadata(payload);
            payload.method = "post";
            submitWithRetry("report-post-reconnect-fallback", () -> httpClient.uploadReport(token, payload));
        } catch (Exception e) {
            log.debug("POST fallback report skipped: {}", e.getMessage());
        }
    }

    private void applyReportMetadata(ReportPayload payload) {
        if (payload == null) {
            return;
        }
        if (clientUuidHint != null && !clientUuidHint.isBlank()) {
            payload.uuid = clientUuidHint;
        }
    }

    public void shutdown() {
        running = false;
        terminalBridge.shutdownAll();
        WebSocket ws = this.webSocket;
        if (ws != null) {
            ws.close(1000, "shutdown");
        }
        workerPool.shutdownNow();
    }

    private void connectOnce(CountDownLatch closedLatch) {
        String wsUrl = toWsUrl(config.serverUrl()) + "/api/clients/report?token=" + urlEncode(token);
        Request request = new Request.Builder().url(wsUrl).build();
        this.webSocket = okHttpClient.newWebSocket(request, new ClientListener(closedLatch));
        log.info("Connecting {}", sanitizeUrlForLog(wsUrl));
    }

    private final class ClientListener extends WebSocketListener {
        private final CountDownLatch closedLatch;
        private final ScheduledExecutorService reportLoop = Executors.newSingleThreadScheduledExecutor();

        private ClientListener(CountDownLatch closedLatch) {
            this.closedLatch = closedLatch;
        }

        @Override
        public void onOpen(WebSocket webSocket, Response response) {
            log.info("WS connected");
            sendReport(webSocket);

            int configuredInterval = Math.max(1, config.reportIntervalSeconds());
            int wsInterval = Math.min(configuredInterval, 10);
            if (configuredInterval != wsInterval) {
                log.warn("report interval={}s is above server WS deadline, using {}s for WS reports.",
                        configuredInterval,
                        wsInterval);
            }
            reportLoop.scheduleAtFixedRate(() -> sendReport(webSocket), wsInterval, wsInterval, TimeUnit.SECONDS);
        }

        @Override
        public void onMessage(WebSocket webSocket, String text) {
            try {
                String messageKind = resolveMessageKind(text);
                if (messageKind == null || messageKind.isBlank()) {
                    protocolDebug("Ignored WS message with unknown type, size={}", text == null ? 0 : text.length());
                    return;
                }
                protocolDebug("Received WS message type={}, size={}", messageKind, text.length());
                switch (messageKind.toLowerCase(Locale.ROOT).trim()) {
                    case "exec" -> handleExec(text);
                    case "ping" -> handlePing(text);
                    case "terminal" -> handleTerminal(text);
                    default -> log.debug("Ignored WS message: {}", text);
                }
            } catch (Exception e) {
                log.warn("Failed to parse server message: {}", e.getMessage());
            }
        }

        @Override
        public void onClosing(WebSocket webSocket, int code, String reason) {
            log.info("WS closing: code={}, reason={}", code, reason);
            webSocket.close(code, reason);
        }

        @Override
        public void onClosed(WebSocket webSocket, int code, String reason) {
            cleanup();
            log.info("WS closed: code={}, reason={}", code, reason);
            closedLatch.countDown();
        }

        @Override
        public void onFailure(WebSocket webSocket, Throwable t, Response response) {
            if (response != null && response.code() == 401) {
                maybeRefreshTokenFromUnauthorized("ws-connect", new IOException("HTTP 401 WS handshake failed"));
            }
            cleanup();
            log.warn("WS failure: {}", t.getMessage());
            closedLatch.countDown();
        }

        private void cleanup() {
            reportLoop.shutdownNow();
        }

        private void handleExec(String raw) {
            ExecMessage msg;
            try {
                msg = mapper.readValue(raw, ExecMessage.class);
            } catch (IOException e) {
                log.warn("Parse exec message failed: {}", e.getMessage());
                return;
            }
            fillExecAliases(raw, msg);
            if (msg.task_id == null || msg.task_id.isBlank()) {
                return;
            }
            if (msg.command == null) {
                return;
            }
            workerPool.submit(() -> {
                TaskResultPayload result = commandExecutor.execute(msg.task_id, msg.command);
                submitWithRetry(
                        "task-result task_id=" + msg.task_id,
                        () -> httpClient.submitTaskResult(token, result));
            });
        }

        private void handlePing(String raw) {
            PingMessage msg;
            try {
                msg = mapper.readValue(raw, PingMessage.class);
            } catch (IOException e) {
                log.warn("Parse ping message failed: {}", e.getMessage());
                return;
            }
            fillPingAliases(raw, msg);
            if (msg.ping_task_id == null || msg.ping_type == null || msg.ping_target == null) {
                return;
            }

            workerPool.submit(() -> {
                int value = measureLatency(msg.ping_type, msg.ping_target);
                PingResultPayload payload = new PingResultPayload();
                payload.task_id = msg.ping_task_id;
                payload.value = value;
                payload.ping_type = msg.ping_type;
                payload.finished_at = Instant.now();

                boolean wsSent = sendPingResultViaWs(payload);
                if (!wsSent) {
                    submitWithRetry(
                            "ping-result task_id=" + msg.ping_task_id,
                            () -> httpClient.submitPingResult(token, payload));
                }
            });
        }

        private void handleTerminal(String raw) {
            TerminalMessage msg;
            try {
                msg = mapper.readValue(raw, TerminalMessage.class);
            } catch (IOException e) {
                log.warn("Parse terminal message failed: {}", e.getMessage());
                return;
            }
            fillTerminalAliases(raw, msg);
            if (msg.request_id == null || msg.request_id.isBlank()) {
                return;
            }
            workerPool.submit(() -> terminalBridge.connectSession(msg.request_id));
        }

        private void fillExecAliases(String raw, ExecMessage msg) {
            if (msg == null || raw == null || raw.isBlank()) {
                return;
            }
            if (msg.task_id != null && !msg.task_id.isBlank() && msg.command != null) {
                return;
            }
            try {
                JsonNode node = mapper.readTree(raw);
                JsonNode dataNode = firstObjectNode(node, "data", "payload");
                if (msg.task_id == null || msg.task_id.isBlank()) {
                    msg.task_id = firstText(node, "task_id", "taskId", "id");
                    if (msg.task_id == null || msg.task_id.isBlank()) {
                        msg.task_id = firstText(dataNode, "task_id", "taskId", "id");
                    }
                }
                if (msg.command == null || msg.command.isBlank()) {
                    msg.command = firstText(node, "command", "cmd", "script");
                    if (msg.command == null || msg.command.isBlank()) {
                        msg.command = firstText(dataNode, "command", "cmd", "script");
                    }
                }
            } catch (Exception ignored) {
            }
        }

        private void fillPingAliases(String raw, PingMessage msg) {
            if (msg == null || raw == null || raw.isBlank()) {
                return;
            }
            if (msg.ping_task_id != null && msg.ping_type != null && msg.ping_target != null) {
                return;
            }
            try {
                JsonNode node = mapper.readTree(raw);
                JsonNode dataNode = firstObjectNode(node, "data", "payload");
                if (msg.ping_task_id == null) {
                    Long taskId = firstLong(node, "ping_task_id", "task_id", "taskId", "id");
                    if (taskId == null) {
                        taskId = firstLong(dataNode, "ping_task_id", "task_id", "taskId", "id");
                    }
                    if (taskId != null) {
                        msg.ping_task_id = taskId;
                    }
                }
                if (msg.ping_type == null || msg.ping_type.isBlank()) {
                    msg.ping_type = firstText(node, "ping_type");
                    if (msg.ping_type == null || msg.ping_type.isBlank()) {
                        msg.ping_type = firstText(dataNode, "ping_type", "type", "kind");
                    }
                }
                if (msg.ping_target == null || msg.ping_target.isBlank()) {
                    msg.ping_target = firstText(node, "ping_target", "target", "host");
                    if (msg.ping_target == null || msg.ping_target.isBlank()) {
                        msg.ping_target = firstText(dataNode, "ping_target", "target", "host");
                    }
                }
            } catch (Exception ignored) {
            }
        }

        private void fillTerminalAliases(String raw, TerminalMessage msg) {
            if (msg == null || raw == null || raw.isBlank()) {
                return;
            }
            if (msg.request_id != null && !msg.request_id.isBlank()) {
                return;
            }
            try {
                JsonNode node = mapper.readTree(raw);
                JsonNode dataNode = firstObjectNode(node, "data", "payload");
                msg.request_id = firstText(node, "request_id", "requestId", "id");
                if (msg.request_id == null || msg.request_id.isBlank()) {
                    msg.request_id = firstText(dataNode, "request_id", "requestId", "id");
                }
            } catch (Exception ignored) {
            }
        }

        private String firstText(JsonNode node, String... keys) {
            if (node == null || keys == null) {
                return null;
            }
            for (String key : keys) {
                JsonNode value = node.get(key);
                if (value != null && !value.isNull() && value.isValueNode()) {
                    String text = value.asText(null);
                    if (text != null && !text.isBlank()) {
                        return text;
                    }
                }
            }
            return null;
        }

        private JsonNode firstObjectNode(JsonNode node, String... keys) {
            if (node == null || keys == null) {
                return null;
            }
            for (String key : keys) {
                JsonNode value = node.get(key);
                if (value != null && value.isObject()) {
                    return value;
                }
            }
            return null;
        }

        private String resolveMessageKind(String raw) {
            if (raw == null || raw.isBlank()) {
                return null;
            }
            try {
                JsonNode node = mapper.readTree(raw);
                JsonNode dataNode = firstObjectNode(node, "data", "payload");

                String direct = firstText(node, "message", "type", "event", "op", "kind");
                if (isKnownMessageKind(direct)) {
                    return direct;
                }

                String nested = firstText(dataNode, "message", "type", "event", "op", "kind");
                if (isKnownMessageKind(nested)) {
                    return nested;
                }

                if (isExecShape(node) || isExecShape(dataNode)) {
                    return "exec";
                }
                if (isPingShape(node) || isPingShape(dataNode)) {
                    return "ping";
                }
                if (isTerminalShape(node) || isTerminalShape(dataNode)) {
                    return "terminal";
                }

                if (direct != null && !direct.isBlank()) {
                    return direct;
                }
                return nested;
            } catch (Exception ignored) {
                return null;
            }
        }

        private boolean isKnownMessageKind(String text) {
            if (text == null || text.isBlank()) {
                return false;
            }
            String normalized = text.trim().toLowerCase(Locale.ROOT);
            return "exec".equals(normalized) || "ping".equals(normalized) || "terminal".equals(normalized);
        }

        private boolean isExecShape(JsonNode node) {
            if (node == null || !node.isObject()) {
                return false;
            }
            String taskId = firstText(node, "task_id", "taskId", "id");
            String command = firstText(node, "command", "cmd", "script");
            return taskId != null && !taskId.isBlank() && command != null && !command.isBlank();
        }

        private boolean isPingShape(JsonNode node) {
            if (node == null || !node.isObject()) {
                return false;
            }
            String target = firstText(node, "ping_target", "target", "host");
            if (target == null || target.isBlank()) {
                return false;
            }
            return firstLong(node, "ping_task_id", "task_id", "taskId", "id") != null;
        }

        private boolean isTerminalShape(JsonNode node) {
            if (node == null || !node.isObject()) {
                return false;
            }
            String requestId = firstText(node, "request_id", "requestId", "id");
            return requestId != null && !requestId.isBlank();
        }

        private Long firstLong(JsonNode node, String... keys) {
            if (node == null || keys == null) {
                return null;
            }
            for (String key : keys) {
                JsonNode value = node.get(key);
                if (value == null || value.isNull()) {
                    continue;
                }
                if (value.isIntegralNumber()) {
                    return value.asLong();
                }
                if (value.isTextual()) {
                    try {
                        return Long.parseLong(value.asText().trim());
                    } catch (Exception ignored) {
                    }
                }
            }
            return null;
        }

        private void sendReport(WebSocket ws) {
            ReportPayload payload = null;
            try {
                payload = collector.collectReport();
                applyReportMetadata(payload);
                payload.method = "ws";
                String json = mapper.writeValueAsString(payload);
                boolean ok = ws.send(json);
                if (!ok) {
                    protocolDebug("WS report send failed, switching to POST fallback");
                    payload.method = "post";
                    ReportPayload postPayload = payload;
                    submitWithRetry("report-post-fallback", () -> httpClient.uploadReport(token, postPayload));
                    ws.close(1001, "report-send-failed");
                } else {
                    protocolDebug(
                            "WS report sent cpu={} load1={} processes={}",
                            payload.cpu.usage,
                            payload.load.load1,
                            payload.process);
                }
            } catch (Exception e) {
                log.warn("Collect/send report failed: {}", e.getMessage());
                try {
                    if (payload == null) {
                        payload = collector.collectReport();
                        applyReportMetadata(payload);
                    }
                    payload.method = "post";
                    ReportPayload postPayload = payload;
                    submitWithRetry("report-post-fallback-on-exception", () -> httpClient.uploadReport(token, postPayload));
                } catch (Exception ignored) {
                }
            }
        }

        private boolean sendPingResultViaWs(PingResultPayload payload) {
            try {
                WebSocket ws = KomariWsClient.this.webSocket;
                if (ws == null) {
                    return false;
                }
                Map<String, Object> body = new HashMap<>();
                body.put("type", "ping_result");
                body.put("task_id", payload.task_id);
                body.put("value", payload.value);
                body.put("ping_type", payload.ping_type);
                body.put("finished_at", payload.finished_at);
                String json = mapper.writeValueAsString(body);
                boolean sent = ws.send(json);
                if (sent) {
                    protocolDebug("Ping result sent via WS task_id={} value={}", payload.task_id, payload.value);
                }
                return sent;
            } catch (Exception e) {
                return false;
            }
        }
    }

    private int measureLatency(String type, String target) {
        String normalized = type.toLowerCase(Locale.ROOT).trim();
        return switch (normalized) {
            case "icmp" -> icmpLatency(target, 5000);
            case "tcp" -> tcpLatency(target, 5000);
            case "http" -> httpLatency(target, 5000);
            default -> -1;
        };
    }

    private int icmpLatency(String host, int timeoutMillis) {
        int fromSystemPing = systemPingLatency(host, timeoutMillis);
        if (fromSystemPing >= 0) {
            return fromSystemPing;
        }
        try {
            long start = System.nanoTime();
            boolean reachable = InetAddress.getByName(host).isReachable(timeoutMillis);
            if (!reachable) {
                return -1;
            }
            return nanosToMillis(System.nanoTime() - start);
        } catch (Exception e) {
            return -1;
        }
    }

    private int systemPingLatency(String host, int timeoutMillis) {
        if (host == null || host.isBlank()) {
            return -1;
        }

        ProcessBuilder builder;
        if (isWindows()) {
            List<String> args = new ArrayList<>();
            args.add("ping");
            if (isLikelyIpv6Literal(host)) {
                args.add("-6");
            }
            args.add("-n");
            args.add("1");
            args.add("-w");
            args.add(String.valueOf(Math.max(timeoutMillis, 1000)));
            args.add(host.trim());
            builder = new ProcessBuilder(args);
        } else {
            int timeoutSeconds = Math.max(1, (int) Math.ceil(timeoutMillis / 1000.0));
            builder = new ProcessBuilder(
                    "ping",
                    "-c",
                    "1",
                    "-W",
                    String.valueOf(timeoutSeconds),
                    host.trim());
        }
        builder.redirectErrorStream(true);

        Process process;
        long startedAt = System.nanoTime();
        try {
            process = builder.start();
        } catch (IOException e) {
            return -1;
        }

        try {
            long waitMillis = Math.max(timeoutMillis + 1500L, 2000L);
            boolean finished = process.waitFor(waitMillis, TimeUnit.MILLISECONDS);
            if (!finished) {
                process.destroyForcibly();
                return -1;
            }

            byte[] bytes = process.getInputStream().readAllBytes();
            String output = new String(bytes, StandardCharsets.UTF_8);
            int parsedLatency = parseLatencyFromPingOutput(output);
            if (parsedLatency >= 0) {
                return parsedLatency;
            }

            if (process.exitValue() == 0) {
                return nanosToMillis(System.nanoTime() - startedAt);
            }
            return -1;
        } catch (Exception e) {
            return -1;
        }
    }

    private static boolean isLikelyIpv6Literal(String host) {
        if (host == null) {
            return false;
        }
        String trimmed = host.trim();
        return trimmed.contains(":") && !trimmed.contains("://");
    }

    private static int parseLatencyFromPingOutput(String output) {
        if (output == null || output.isBlank()) {
            return -1;
        }

        Pattern lessPattern = Pattern.compile("(?i)<\\s*1\\s*ms");
        Matcher lessMatcher = lessPattern.matcher(output);
        if (lessMatcher.find()) {
            return 1;
        }

        Pattern valuePattern = Pattern.compile("(?i)([0-9]+(?:\\.[0-9]+)?)\\s*ms");
        Matcher valueMatcher = valuePattern.matcher(output);
        if (!valueMatcher.find()) {
            return -1;
        }
        try {
            double value = Double.parseDouble(valueMatcher.group(1));
            if (Double.isNaN(value) || Double.isInfinite(value) || value < 0) {
                return -1;
            }
            return (int) Math.max(Math.round(value), 0L);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private int tcpLatency(String target, int timeoutMillis) {
        try {
            HostPort hostPort = parseHostPort(target);
            long start = System.nanoTime();
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(hostPort.host, hostPort.port), timeoutMillis);
            }
            return nanosToMillis(System.nanoTime() - start);
        } catch (Exception e) {
            return -1;
        }
    }

    private int httpLatency(String target, int timeoutMillis) {
        try {
            String url = normalizeHttpTarget(target);
            long start = System.nanoTime();
            OkHttpClient client = okHttpClient.newBuilder()
                    .callTimeout(timeoutMillis, TimeUnit.MILLISECONDS)
                    .build();

            Request headRequest = new Request.Builder()
                    .url(url)
                    .head()
                    .build();
            try (Response response = client.newCall(headRequest).execute()) {
                int code = response.code();
                if (!response.isSuccessful() && code >= 500) {
                    return -1;
                }
                if (code == 405 || code == 501) {
                    Request getRequest = new Request.Builder()
                            .url(url)
                            .get()
                            .build();
                    try (Response getResponse = client.newCall(getRequest).execute()) {
                        if (!getResponse.isSuccessful() && getResponse.code() >= 500) {
                            return -1;
                        }
                    }
                }
            }
            return nanosToMillis(System.nanoTime() - start);
        } catch (Exception e) {
            return -1;
        }
    }

    private static HostPort parseHostPort(String target) throws URISyntaxException {
        if (target.contains("://")) {
            URI uri = new URI(target);
            String host = uri.getHost();
            int port = uri.getPort();
            if (host == null || port < 1) {
                throw new URISyntaxException(target, "invalid tcp uri");
            }
            return new HostPort(host, port);
        }
        if (target.startsWith("[")) {
            int closing = target.indexOf(']');
            if (closing > 1 && closing + 2 < target.length() && target.charAt(closing + 1) == ':') {
                String host = target.substring(1, closing);
                int port = Integer.parseInt(target.substring(closing + 2));
                return new HostPort(host, port);
            }
        }
        int sep = target.lastIndexOf(':');
        if (sep <= 0 || sep >= target.length() - 1) {
            throw new URISyntaxException(target, "target must be host:port");
        }
        String host = target.substring(0, sep);
        int port = Integer.parseInt(target.substring(sep + 1));
        return new HostPort(host, port);
    }

    private static String normalizeHttpTarget(String target) {
        String trimmed = target == null ? "" : target.trim();
        if (trimmed.isEmpty()) {
            return trimmed;
        }
        if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
            return trimmed;
        }
        return "http://" + trimmed;
    }

    private static int nanosToMillis(long nanos) {
        long ms = TimeUnit.NANOSECONDS.toMillis(nanos);
        if (ms > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) ms;
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    }

    private static String toWsUrl(String serverUrl) {
        if (serverUrl.startsWith("https://")) {
            return "wss://" + serverUrl.substring("https://".length());
        }
        if (serverUrl.startsWith("http://")) {
            return "ws://" + serverUrl.substring("http://".length());
        }
        if (serverUrl.startsWith("wss://") || serverUrl.startsWith("ws://")) {
            return serverUrl;
        }
        return "ws://" + serverUrl;
    }

    private static String urlEncode(String value) {
        return java.net.URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8);
    }

    private static String sanitizeUrlForLog(String url) {
        if (url == null || url.isBlank()) {
            return "";
        }
        return TOKEN_QUERY_PATTERN.matcher(url).replaceAll("$1***");
    }

    private void protocolDebug(String template, Object... args) {
        if (config.protocolDebug()) {
            log.info("[protocol] " + template, args);
        }
    }

    private boolean submitWithRetry(String label, IoAction action) {
        int maxAttempts = Math.max(1, config.submitRetryMaxAttempts());
        int backoffMillis = Math.max(100, config.submitRetryBackoffMillis());
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                action.run();
                return true;
            } catch (IOException e) {
                if (maybeRefreshTokenFromUnauthorized(label, e)) {
                    continue;
                }
                if (attempt >= maxAttempts) {
                    log.warn("Submit failed after {} attempts ({}): {}", maxAttempts, label, e.getMessage());
                    return false;
                }
                try {
                    long sleep = (long) backoffMillis * attempt;
                    Thread.sleep(sleep);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return false;
    }

    private boolean maybeRefreshTokenFromUnauthorized(String context, IOException failure) {
        if ((!allowTokenFileReload && !allowAutoTokenRefresh) || failure == null) {
            return false;
        }

        String message = failure.getMessage();
        if (message == null || !message.contains("HTTP 401")) {
            return false;
        }

        synchronized (tokenRefreshLock) {
            long cooldownSeconds = Math.max(0, config.tokenRefreshCooldownSeconds());
            long cooldownMillis = cooldownSeconds * 1000L;
            long now = System.currentTimeMillis();
            if (cooldownMillis > 0 && lastTokenRefreshAttemptAtMillis > 0L) {
                long elapsed = now - lastTokenRefreshAttemptAtMillis;
                if (elapsed < cooldownMillis) {
                    long waitMillis = cooldownMillis - elapsed;
                    log.warn("Skip auto-refresh after unauthorized ({}); cooldown active for {} ms.", context, waitMillis);
                    return false;
                }
            }
            lastTokenRefreshAttemptAtMillis = now;
            try {
                if (tryReloadTokenFromFile(context)) {
                    return true;
                }
                if (!allowAutoTokenRefresh) {
                    return false;
                }

                KomariHttpClient.RegisteredClient registered = httpClient.registerByAutoDiscovery();
                String newToken = registered.token();
                if (newToken == null || newToken.isBlank()) {
                    return false;
                }

                this.token = newToken;
                terminalBridge.updateToken(newToken);
                String uuid = registered.uuid() == null ? "" : registered.uuid().trim();
                if (!uuid.isBlank()) {
                    this.clientUuidHint = uuid;
                }
                try {
                    BasicInfoPayload basicInfo = collector.collectBasicInfo(
                            config.clientName(),
                            config.reportLocalIp(),
                            config.reportPrivateIp());
                    httpClient.uploadBasicInfo(newToken, basicInfo);
                } catch (Exception uploadError) {
                    log.warn("Basic info re-upload after token refresh failed: {}", uploadError.getMessage());
                }
                try {
                    tokenStore.save(uuid, newToken);
                } catch (Exception e) {
                    log.warn("Token refresh cache write failed: {}", e.getMessage());
                }
                WebSocket currentWs = this.webSocket;
                if (currentWs != null) {
                    currentWs.close(1001, "token-refreshed");
                }
                log.warn("Token refreshed by auto-discovery due to unauthorized response ({}).", context);
                return true;
            } catch (Exception refreshError) {
                log.warn("Auto-refresh token failed after unauthorized response ({}): {}",
                        context,
                        refreshError.getMessage());
                return false;
            }
        }
    }

    private boolean tryReloadTokenFromFile(String context) {
        if (!allowTokenFileReload) {
            return false;
        }
        Optional<LocalTokenStore.StoredToken> loaded = tokenStore.load();
        if (loaded.isEmpty()) {
            return false;
        }

        String fileToken = loaded.get().token() == null ? "" : loaded.get().token().trim();
        if (fileToken.isBlank() || fileToken.equals(token)) {
            return false;
        }

        BasicInfoPayload basicInfo = collector.collectBasicInfo(
                config.clientName(),
                config.reportLocalIp(),
                config.reportPrivateIp());
        try {
            httpClient.uploadBasicInfo(fileToken, basicInfo);
        } catch (Exception e) {
            log.warn("Token file reload candidate failed basic-info verify ({}): {}", context, e.getMessage());
            return false;
        }

        this.token = fileToken;
        terminalBridge.updateToken(fileToken);
        String uuid = loaded.get().uuid() == null ? "" : loaded.get().uuid().trim();
        if (!uuid.isBlank()) {
            this.clientUuidHint = uuid;
        }
        WebSocket currentWs = this.webSocket;
        if (currentWs != null) {
            currentWs.close(1001, "token-reloaded");
        }
        log.warn("Token reloaded from local token file due to unauthorized response ({}).", context);
        return true;
    }

    @FunctionalInterface
    private interface IoAction {
        void run() throws IOException;
    }

    private record HostPort(String host, int port) {
    }
}

final class CommandExecutor {
    private final int timeoutSeconds;
    private final int maxResultBytes;

    public CommandExecutor(int timeoutSeconds, int maxResultBytes) {
        this.timeoutSeconds = timeoutSeconds;
        this.maxResultBytes = maxResultBytes;
    }

    public TaskResultPayload execute(String taskId, String command) {
        TaskResultPayload payload = new TaskResultPayload();
        payload.task_id = taskId;
        payload.finished_at = Instant.now();

        ProcessBuilder builder;
        if (isWindows()) {
            builder = new ProcessBuilder("cmd.exe", "/c", command);
        } else {
            builder = new ProcessBuilder("/bin/sh", "-lc", command);
        }

        builder.redirectErrorStream(true);
        ExecutorService outputReader = Executors.newSingleThreadExecutor();
        Future<String> outputFuture = null;
        try {
            Process process = builder.start();
            outputFuture = outputReader.submit(() -> readLimited(process.getInputStream(), maxResultBytes));
            boolean finished = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            if (!finished) {
                destroyProcessTree(process);
                payload.exit_code = -1;
                String partial = awaitOutput(outputFuture, 2);
                if (partial == null || partial.isBlank()) {
                    payload.result = "Command timeout after " + timeoutSeconds + "s";
                } else {
                    payload.result = ensureNonEmptyResult(partial)
                            + System.lineSeparator()
                            + "...[timeout after " + timeoutSeconds + "s]";
                }
                payload.finished_at = Instant.now();
                return payload;
            }

            String output = awaitOutput(outputFuture, 5);
            if (output == null) {
                output = "";
            }
            payload.exit_code = process.exitValue();
            payload.result = ensureNonEmptyResult(output);
            payload.finished_at = Instant.now();
            return payload;
        } catch (Exception e) {
            payload.exit_code = -1;
            payload.result = "Command execution failed: " + e.getMessage();
            payload.finished_at = Instant.now();
            return payload;
        } finally {
            if (outputFuture != null && !outputFuture.isDone()) {
                outputFuture.cancel(true);
            }
            outputReader.shutdownNow();
        }
    }

    private static String awaitOutput(Future<String> outputFuture, int waitSeconds) {
        if (outputFuture == null) {
            return null;
        }
        int timeout = Math.max(1, waitSeconds);
        try {
            return outputFuture.get(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (TimeoutException e) {
            return null;
        } catch (ExecutionException e) {
            return null;
        }
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase().contains("win");
    }

    private static String readLimited(InputStream stream, int maxBytes) throws IOException {
        if (stream == null) {
            return "";
        }

        ByteArrayOutputStream buffer = new ByteArrayOutputStream(Math.max(maxBytes, 1024));
        boolean truncated = false;
        byte[] chunk = new byte[8192];
        int total = 0;

        int read;
        while ((read = stream.read(chunk)) != -1) {
            if (!truncated && total + read <= maxBytes) {
                buffer.write(chunk, 0, read);
                total += read;
                continue;
            }

            if (!truncated) {
                int remain = Math.max(0, maxBytes - total);
                if (remain > 0) {
                    buffer.write(chunk, 0, remain);
                    total += remain;
                }
                truncated = true;
            }
        }

        String output = decodeCommandOutput(buffer.toByteArray());
        if (truncated) {
            output += System.lineSeparator() + "...[truncated]";
        }
        return output;
    }

    private static String decodeCommandOutput(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        if (isValidUtf8(bytes)) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (isWindows()) {
            try {
                return new String(bytes, Charset.forName("GBK"));
            } catch (Exception ignored) {
            }
        }
        return new String(bytes, Charset.defaultCharset());
    }

    private static boolean isValidUtf8(byte[] bytes) {
        try {
            StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes));
            return true;
        } catch (CharacterCodingException e) {
            return false;
        }
    }

    private static String ensureNonEmptyResult(String output) {
        if (output == null || output.isEmpty()) {
            return "\n";
        }
        return output;
    }

    private static void destroyProcessTree(Process process) {
        if (process == null) {
            return;
        }
        try {
            ProcessHandle handle = process.toHandle();
            handle.descendants().forEach(descendant -> {
                try {
                    descendant.destroy();
                    if (descendant.isAlive()) {
                        descendant.destroyForcibly();
                    }
                } catch (Exception ignored) {
                }
            });
            process.destroy();
            if (process.isAlive()) {
                process.destroyForcibly();
            }
        } catch (Exception ignored) {
            try {
                process.destroyForcibly();
            } catch (Exception ignoredToo) {
            }
        }
    }
}

final class TerminalBridge {
    private static final Logger log = LoggerFactory.getLogger(TerminalBridge.class);
    private static final Pattern TOKEN_QUERY_PATTERN = Pattern.compile("([?&]token=)[^&]*", Pattern.CASE_INSENSITIVE);
    private static final int MAX_CONNECT_ATTEMPTS = 3;
    private static final long RETRY_CONNECT_DELAY_MILLIS = 750L;

    private final OkHttpClient okHttpClient;
    private final ObjectMapper mapper;
    private final String serverUrl;
    private final boolean protocolDebug;
    private volatile String token;
    private final Map<String, TerminalSession> sessions = new ConcurrentHashMap<>();

    public TerminalBridge(
            OkHttpClient okHttpClient,
            ObjectMapper mapper,
            String serverUrl,
            String token,
            boolean protocolDebug) {
        this.okHttpClient = Objects.requireNonNull(okHttpClient, "okHttpClient");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.serverUrl = Objects.requireNonNull(serverUrl, "serverUrl");
        this.token = Objects.requireNonNull(token, "token").trim();
        this.protocolDebug = protocolDebug;
    }

    public void updateToken(String token) {
        if (token == null || token.isBlank()) {
            return;
        }
        this.token = token.trim();
    }

    public void connectSession(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            return;
        }
        sessions.compute(requestId, (k, old) -> {
            if (old != null && !old.isClosed()) {
                log.info("Terminal session already active: {}", requestId);
                return old;
            }
            TerminalSession next = new TerminalSession(requestId);
            next.open();
            return next;
        });
    }

    public void shutdownAll() {
        for (TerminalSession session : sessions.values()) {
            session.close("shutdown");
        }
        sessions.clear();
    }

    private final class TerminalSession {
        private final String requestId;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final ExecutorService ioPool = Executors.newFixedThreadPool(2);
        private final AtomicBoolean shellStarted = new AtomicBoolean(false);
        private volatile int connectAttempts = 0;
        private volatile WebSocket ws;
        private volatile Process process;
        private volatile OutputStream shellInput;

        private TerminalSession(String requestId) {
            this.requestId = requestId;
        }

        private void open() {
            if (closed.get()) {
                return;
            }
            connectAttempts++;
            String wsUrl = toWsUrl(serverUrl)
                    + "/api/clients/terminal?id="
                    + encode(requestId)
                    + "&token="
                    + encode(token);
            log.info("Connecting terminal session {} (attempt {}/{})", requestId, connectAttempts, MAX_CONNECT_ATTEMPTS);
            protocolDebug("Terminal connect url={}", sanitizeUrlForLog(wsUrl));
            Request request = new Request.Builder().url(wsUrl).build();
            ws = okHttpClient.newWebSocket(request, new SessionListener());
        }

        private boolean isClosed() {
            return closed.get();
        }

        private void startShell() throws IOException {
            if (!shellStarted.compareAndSet(false, true)) {
                return;
            }
            ProcessBuilder pb = createShellProcessBuilder();
            pb.redirectErrorStream(true);
            process = pb.start();
            shellInput = process.getOutputStream();

            ioPool.submit(this::pipeShellOutput);
            ioPool.submit(this::waitShellExit);
        }

        private ProcessBuilder createShellProcessBuilder() {
            if (isWindows()) {
                return new ProcessBuilder("cmd.exe");
            }
            return new ProcessBuilder("/bin/sh");
        }

        private void pipeShellOutput() {
            Process p = process;
            if (p == null) {
                return;
            }
            try (InputStream in = p.getInputStream()) {
                byte[] buffer = new byte[8192];
                int n;
                while (!closed.get() && (n = in.read(buffer)) != -1) {
                    WebSocket current = ws;
                    if (current == null) {
                        break;
                    }
                    byte[] chunk = new byte[n];
                    System.arraycopy(buffer, 0, chunk, 0, n);
                    protocolDebug("Terminal shell output(request_id={}, bytes={})", requestId, n);
                    if (!current.send(ByteString.of(chunk))) {
                        break;
                    }
                }
            } catch (Exception e) {
                if (!closed.get()) {
                    log.warn("Terminal output error(request_id={}): {}", requestId, e.getMessage());
                }
            } finally {
                close("shell-output-ended");
            }
        }

        private void waitShellExit() {
            Process p = process;
            if (p == null) {
                return;
            }
            try {
                int exit = p.waitFor();
                log.info("Terminal shell exited(request_id={}, exit={})", requestId, exit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                close("shell-exited");
            }
        }

        private void handleTextInput(String text) {
            if (text == null || text.isEmpty()) {
                return;
            }
            String trimmed = text.stripLeading();
            protocolDebug("Terminal text frame(request_id={}, chars={}, json={})", requestId, text.length(), trimmed.startsWith("{"));
            if (trimmed.startsWith("{")) {
                ControlPayload payload = parseControlPayload(trimmed);
                if (payload.parseFailed()) {
                    // Not a valid control message; treat as plain terminal input.
                    writeToShell(text.getBytes(StandardCharsets.UTF_8));
                    return;
                }
                if (payload.closeRequested()) {
                    close("browser-close-requested");
                    return;
                }
                byte[] inputBytes = payload.inputBytes();
                if (inputBytes != null && inputBytes.length > 0) {
                    writeToShell(inputBytes);
                }
                return;
            }
            writeToShell(text.getBytes(StandardCharsets.UTF_8));
        }

        private ControlPayload parseControlPayload(String text) {
            try {
                JsonNode node = mapper.readTree(text);
                if (node == null || !node.isObject()) {
                    return ControlPayload.parseError();
                }

                String action = firstText(node, "action", "type", "event", "op", "kind");
                if (isCloseAction(action)) {
                    return ControlPayload.close();
                }

                if (firstBoolean(node, "close", "closed", "disconnect", "disconnected")) {
                    return ControlPayload.close();
                }
                JsonNode nestedControl = firstNode(node, "data", "payload");
                if (nestedControl != null
                        && nestedControl.isObject()
                        && firstBoolean(nestedControl, "close", "closed", "disconnect", "disconnected")) {
                    return ControlPayload.close();
                }

                byte[] inputBytes = decodeInputBytes(node);
                if (inputBytes == null || inputBytes.length == 0) {
                    String fallbackText = firstText(
                            node,
                            "content",
                            "message",
                            "value",
                            "chunk",
                            "stdin",
                            "command",
                            "cmd");
                    if ((fallbackText == null || fallbackText.isBlank()) && nestedControl != null && nestedControl.isObject()) {
                        fallbackText = firstText(
                                nestedControl,
                                "content",
                                "message",
                                "value",
                                "chunk",
                                "stdin",
                                "command",
                                "cmd");
                    }
                    if (fallbackText != null && !fallbackText.isBlank()) {
                        protocolDebug("Terminal fallback text input(request_id={}, chars={})", requestId, fallbackText.length());
                        return ControlPayload.input(fallbackText.getBytes(StandardCharsets.UTF_8));
                    }

                    String keyToken = firstRawText(node, "key", "keys");
                    if ((keyToken == null || keyToken.isBlank()) && nestedControl != null && nestedControl.isObject()) {
                        keyToken = firstRawText(nestedControl, "key", "keys");
                    }
                    boolean ctrlKey = firstBoolean(node, "ctrlKey", "ctrl")
                            || firstBoolean(nestedControl, "ctrlKey", "ctrl");
                    boolean altKey = firstBoolean(node, "altKey", "alt")
                            || firstBoolean(nestedControl, "altKey", "alt");
                    boolean metaKey = firstBoolean(node, "metaKey", "meta")
                            || firstBoolean(nestedControl, "metaKey", "meta");
                    byte[] keyBytes = keyTokenToBytes(keyToken, ctrlKey, altKey, metaKey);
                    if (keyBytes != null && keyBytes.length > 0) {
                        protocolDebug(
                                "Terminal key input(request_id={}, key={}, ctrl={}, alt={}, meta={})",
                                requestId,
                                keyToken,
                                ctrlKey,
                                altKey,
                                metaKey);
                        return ControlPayload.input(keyBytes);
                    }

                    if (isNoInputControlFrame(node, nestedControl, action)) {
                        protocolDebug("Terminal ignored control frame(request_id={}, action={})", requestId, action);
                        return ControlPayload.handled();
                    }
                    // Unknown JSON frame: treat as parse error so caller can pass raw text through.
                    return ControlPayload.parseError();
                }

                return ControlPayload.input(inputBytes);
            } catch (Exception e) {
                return ControlPayload.parseError();
            }
        }

        private byte[] decodeInputBytes(JsonNode rootNode) {
            JsonNode inputNode = firstNode(
                    rootNode,
                    "data",
                    "input",
                    "text",
                    "payload",
                    "content",
                    "message",
                    "value",
                    "chunk",
                    "stdin",
                    "bytes");
            if (inputNode == null || inputNode.isNull()) {
                return null;
            }

            if (inputNode.isObject()) {
                JsonNode nestedInput = firstNode(
                        inputNode,
                        "data",
                        "input",
                        "text",
                        "payload",
                        "value",
                        "chunk",
                        "content",
                        "message",
                        "stdin",
                        "bytes");
                if (nestedInput != null && !nestedInput.isNull()) {
                    boolean decodeBase64 = isBase64(rootNode) || isBase64(inputNode);
                    return decodeRawInputBytes(nestedInput, decodeBase64);
                }
                if (inputNode.has("close") && inputNode.get("close").asBoolean(false)) {
                    return null;
                }
            }

            boolean decodeBase64 = isBase64(rootNode);
            return decodeRawInputBytes(inputNode, decodeBase64);
        }

        private byte[] decodeRawInputBytes(JsonNode node, boolean decodeBase64) {
            if (node == null || node.isNull()) {
                return null;
            }
            if (node.isBinary()) {
                try {
                    return node.binaryValue();
                } catch (IOException ignored) {
                    return null;
                }
            }
            if (node.isArray()) {
                return decodeByteArray(node);
            }
            if (!node.isValueNode()) {
                return null;
            }

            String text = node.asText("");
            if (text.isEmpty()) {
                return null;
            }
            if (decodeBase64) {
                try {
                    return Base64.getDecoder().decode(text);
                } catch (IllegalArgumentException ignored) {
                    // Keep compatibility with clients that accidentally set base64 but send plain text.
                }
            }
            return text.getBytes(StandardCharsets.UTF_8);
        }

        private byte[] decodeByteArray(JsonNode arrayNode) {
            if (arrayNode == null || !arrayNode.isArray()) {
                return null;
            }
            byte[] bytes = new byte[arrayNode.size()];
            for (int i = 0; i < arrayNode.size(); i++) {
                JsonNode n = arrayNode.get(i);
                if (n == null || !n.isIntegralNumber()) {
                    return null;
                }
                int value = n.asInt();
                if (value < 0 || value > 255) {
                    return null;
                }
                bytes[i] = (byte) value;
            }
            return bytes;
        }

        private boolean isNoInputControlFrame(JsonNode rootNode, JsonNode nestedNode, String action) {
            if (isNoInputAction(action)) {
                return true;
            }
            if (rootNode != null && rootNode.isObject()) {
                if (hasAnyField(rootNode,
                        "cols",
                        "rows",
                        "width",
                        "height",
                        "resize",
                        "heartbeat",
                        "keepalive",
                        "ping",
                        "pong",
                        "status",
                        "ack")) {
                    return true;
                }
                String rootAction = firstText(rootNode, "type", "event", "op", "kind", "action");
                if (isNoInputAction(rootAction)) {
                    return true;
                }
            }
            if (nestedNode != null && nestedNode.isObject()) {
                if (hasAnyField(nestedNode,
                        "cols",
                        "rows",
                        "width",
                        "height",
                        "resize",
                        "heartbeat",
                        "keepalive",
                        "ping",
                        "pong",
                        "status",
                        "ack")) {
                    return true;
                }
                String nestedAction = firstText(nestedNode, "type", "event", "op", "kind", "action");
                if (isNoInputAction(nestedAction)) {
                    return true;
                }
            }
            return false;
        }

        private boolean hasAnyField(JsonNode node, String... names) {
            if (node == null || names == null) {
                return false;
            }
            for (String name : names) {
                if (name != null && node.has(name)) {
                    return true;
                }
            }
            return false;
        }

        private boolean isNoInputAction(String action) {
            if (action == null || action.isBlank()) {
                return false;
            }
            String normalized = action.trim().toLowerCase(Locale.ROOT);
            return "resize".equals(normalized)
                    || "resized".equals(normalized)
                    || "ping".equals(normalized)
                    || "pong".equals(normalized)
                    || "heartbeat".equals(normalized)
                    || "keepalive".equals(normalized)
                    || "ready".equals(normalized)
                    || "init".equals(normalized)
                    || "open".equals(normalized)
                    || "connected".equals(normalized)
                    || "ack".equals(normalized)
                    || "noop".equals(normalized);
        }

        private byte[] keyTokenToBytes(String keyToken, boolean ctrlKey, boolean altKey, boolean metaKey) {
            if (keyToken == null || keyToken.isEmpty()) {
                return null;
            }
            if (keyToken.length() == 1 && Character.isWhitespace(keyToken.charAt(0))) {
                return keyToken.getBytes(StandardCharsets.UTF_8);
            }
            String key = keyToken.trim();
            if (key.isEmpty()) {
                return null;
            }
            String normalized = key.toLowerCase(Locale.ROOT);

            if ((ctrlKey || metaKey) && key.length() == 1) {
                char ch = Character.toUpperCase(key.charAt(0));
                if (ch >= '@' && ch <= '_') {
                    return new byte[]{(byte) (ch & 0x1F)};
                }
            }

            return switch (normalized) {
                case "enter", "return" -> System.lineSeparator().getBytes(StandardCharsets.UTF_8);
                case "tab" -> "\t".getBytes(StandardCharsets.UTF_8);
                case "backspace" -> "\b".getBytes(StandardCharsets.UTF_8);
                case "space" -> " ".getBytes(StandardCharsets.UTF_8);
                case "escape", "esc" -> "\u001B".getBytes(StandardCharsets.UTF_8);
                case "arrowup", "up" -> "\u001B[A".getBytes(StandardCharsets.UTF_8);
                case "arrowdown", "down" -> "\u001B[B".getBytes(StandardCharsets.UTF_8);
                case "arrowright", "right" -> "\u001B[C".getBytes(StandardCharsets.UTF_8);
                case "arrowleft", "left" -> "\u001B[D".getBytes(StandardCharsets.UTF_8);
                case "delete", "del" -> "\u001B[3~".getBytes(StandardCharsets.UTF_8);
                case "ctrl+c", "^c", "control+c" -> new byte[]{0x03};
                case "ctrl+d", "^d", "control+d" -> new byte[]{0x04};
                case "ctrl+z", "^z", "control+z" -> new byte[]{0x1A};
                default -> key.getBytes(StandardCharsets.UTF_8);
            };
        }

        private void writeToShell(byte[] bytes) {
            OutputStream out = shellInput;
            if (out == null || bytes == null || bytes.length == 0 || closed.get()) {
                return;
            }
            try {
                synchronized (this) {
                    protocolDebug("Terminal write to shell(request_id={}, bytes={})", requestId, bytes.length);
                    out.write(bytes);
                    out.flush();
                }
            } catch (IOException e) {
                if (!closed.get()) {
                    log.warn("Terminal input write failed(request_id={}): {}", requestId, e.getMessage());
                }
                close("shell-input-failed");
            }
        }

        private void close(String reason) {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            log.info("Closing terminal session {} ({})", requestId, reason);
            sessions.remove(requestId);

            try {
                if (shellInput != null) {
                    shellInput.close();
                }
            } catch (IOException ignored) {
            }

            try {
                if (process != null && process.isAlive()) {
                    destroyProcessTree(process);
                }
            } catch (Exception ignored) {
            }

            try {
                if (ws != null) {
                    ws.close(1000, reason);
                }
            } catch (Exception ignored) {
            }

            ioPool.shutdownNow();
        }

        private boolean scheduleReconnect(String reason, Response response) {
            if (closed.get() || shellStarted.get() || connectAttempts >= MAX_CONNECT_ATTEMPTS) {
                return false;
            }
            int httpCode = response == null ? -1 : response.code();
            if (httpCode == 401 || httpCode == 403 || httpCode == 404) {
                return false;
            }

            ws = null;
            log.warn(
                    "Retry terminal session {} after {} ms ({}, attempt {}/{})",
                    requestId,
                    RETRY_CONNECT_DELAY_MILLIS,
                    reason,
                    connectAttempts,
                    MAX_CONNECT_ATTEMPTS);
            ioPool.submit(() -> {
                try {
                    Thread.sleep(RETRY_CONNECT_DELAY_MILLIS);
                    if (!closed.get() && !shellStarted.get()) {
                        open();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            return true;
        }

        private String firstText(JsonNode node, String... fieldNames) {
            if (node == null || fieldNames == null) {
                return null;
            }
            for (String fieldName : fieldNames) {
                JsonNode field = node.get(fieldName);
                if (field != null && field.isValueNode() && !field.isNull()) {
                    String value = field.asText(null);
                    if (value != null && !value.isBlank()) {
                        return value;
                    }
                }
            }
            return null;
        }

        private String firstRawText(JsonNode node, String... fieldNames) {
            if (node == null || fieldNames == null) {
                return null;
            }
            for (String fieldName : fieldNames) {
                JsonNode field = node.get(fieldName);
                if (field != null && field.isValueNode() && !field.isNull()) {
                    return field.asText(null);
                }
            }
            return null;
        }

        private JsonNode firstNode(JsonNode node, String... fieldNames) {
            if (node == null || fieldNames == null) {
                return null;
            }
            for (String fieldName : fieldNames) {
                JsonNode field = node.get(fieldName);
                if (field != null && !field.isNull()) {
                    return field;
                }
            }
            return null;
        }

        private boolean firstBoolean(JsonNode node, String... fieldNames) {
            if (node == null || fieldNames == null) {
                return false;
            }
            for (String fieldName : fieldNames) {
                JsonNode field = node.get(fieldName);
                if (field == null || field.isNull()) {
                    continue;
                }
                if (field.isBoolean() && field.asBoolean()) {
                    return true;
                }
                if (field.isTextual()) {
                    String normalized = field.asText("").trim().toLowerCase(Locale.ROOT);
                    if ("1".equals(normalized)
                            || "true".equals(normalized)
                            || "yes".equals(normalized)
                            || "on".equals(normalized)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private boolean isCloseAction(String action) {
            if (action == null) {
                return false;
            }
            String normalized = action.trim().toLowerCase(Locale.ROOT);
            return "close".equals(normalized)
                    || "disconnect".equals(normalized)
                    || "terminate".equals(normalized)
                    || "exit".equals(normalized);
        }

        private boolean isBase64(JsonNode node) {
            if (node == null || node.isNull()) {
                return false;
            }
            JsonNode base64Node = node.get("base64");
            if (base64Node != null && !base64Node.isNull()) {
                if (base64Node.isBoolean() && base64Node.asBoolean()) {
                    return true;
                }
                if (base64Node.isTextual()) {
                    String normalized = base64Node.asText("").trim().toLowerCase(Locale.ROOT);
                    if ("1".equals(normalized)
                            || "true".equals(normalized)
                            || "yes".equals(normalized)
                            || "on".equals(normalized)) {
                        return true;
                    }
                }
            }
            String encoding = firstText(node, "encoding", "codec", "format");
            if (encoding == null) {
                return false;
            }
            String normalized = encoding.trim().toLowerCase(Locale.ROOT);
            return "base64".equals(normalized)
                    || "b64".equals(normalized)
                    || "base-64".equals(normalized);
        }

        private record ControlPayload(byte[] inputBytes, boolean closeRequested, boolean parseFailed) {
            private static ControlPayload input(byte[] inputBytes) {
                return new ControlPayload(inputBytes, false, false);
            }

            private static ControlPayload handled() {
                return new ControlPayload(null, false, false);
            }

            private static ControlPayload close() {
                return new ControlPayload(null, true, false);
            }

            private static ControlPayload parseError() {
                return new ControlPayload(null, false, true);
            }
        }

        private void destroyProcessTree(Process target) {
            if (target == null) {
                return;
            }
            try {
                ProcessHandle handle = target.toHandle();
                handle.descendants().forEach(descendant -> {
                    try {
                        descendant.destroy();
                        if (descendant.isAlive()) {
                            descendant.destroyForcibly();
                        }
                    } catch (Exception ignored) {
                    }
                });
                target.destroy();
                if (target.isAlive()) {
                    target.destroyForcibly();
                }
            } catch (Exception ignored) {
                try {
                    target.destroyForcibly();
                } catch (Exception ignoredToo) {
                }
            }
        }

        private final class SessionListener extends WebSocketListener {
            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                int code = response == null ? -1 : response.code();
                log.info("Terminal ws connected(request_id={}, code={})", requestId, code);
                try {
                    startShell();
                } catch (Exception e) {
                    log.warn("Terminal shell start failed(request_id={}): {}", requestId, e.getMessage());
                    close("shell-start-failed");
                }
            }

            @Override
            public void onMessage(WebSocket webSocket, String text) {
                handleTextInput(text);
            }

            @Override
            public void onMessage(WebSocket webSocket, ByteString bytes) {
                protocolDebug("Terminal binary frame(request_id={}, bytes={})", requestId, bytes.size());
                writeToShell(bytes.toByteArray());
            }

            @Override
            public void onClosing(WebSocket webSocket, int code, String reason) {
                log.info("Terminal ws closing(request_id={}, code={}, reason={})", requestId, code, reason);
                webSocket.close(code, reason);
            }

            @Override
            public void onClosed(WebSocket webSocket, int code, String reason) {
                log.info("Terminal ws closed(request_id={}, code={}, reason={})", requestId, code, reason);
                if (scheduleReconnect("ws-closed code=" + code, null)) {
                    return;
                }
                close("server-closed");
            }

            @Override
            public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                if (!closed.get()) {
                    String responseSummary = "";
                    if (response != null) {
                        String bodySnippet = "";
                        try {
                            if (response.body() != null) {
                                bodySnippet = response.body().string();
                                if (bodySnippet.length() > 300) {
                                    bodySnippet = bodySnippet.substring(0, 300) + "...";
                                }
                            }
                        } catch (Exception ignored) {
                        }
                        responseSummary = ", httpCode=" + response.code() + ", body=" + bodySnippet;
                    }
                    String throwableType = t == null ? "null" : t.getClass().getSimpleName();
                    String throwableMessage = (t == null || t.getMessage() == null) ? "null" : t.getMessage();
                    log.warn(
                            "Terminal ws failed(request_id={}): type={}, message={}{}",
                            requestId,
                            throwableType,
                            throwableMessage,
                            responseSummary);
                }
                if (scheduleReconnect("ws-failed", response)) {
                    return;
                }
                close("ws-failed");
            }
        }
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase().contains("win");
    }

    private static String toWsUrl(String url) {
        if (url.startsWith("https://")) {
            return "wss://" + url.substring("https://".length());
        }
        if (url.startsWith("http://")) {
            return "ws://" + url.substring("http://".length());
        }
        if (url.startsWith("wss://") || url.startsWith("ws://")) {
            return url;
        }
        return "ws://" + url;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String sanitizeUrlForLog(String url) {
        if (url == null || url.isBlank()) {
            return "";
        }
        return TOKEN_QUERY_PATTERN.matcher(url).replaceAll("$1***");
    }

    private void protocolDebug(String template, Object... args) {
        if (protocolDebug) {
            log.info("[protocol] " + template, args);
        }
    }
}

