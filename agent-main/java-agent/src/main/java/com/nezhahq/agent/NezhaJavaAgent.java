package com.nezhahq.agent;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.nezhahq.agent.proto.GeoIP;
import com.nezhahq.agent.proto.Host;
import com.nezhahq.agent.proto.IOStreamData;
import com.nezhahq.agent.proto.IP;
import com.nezhahq.agent.proto.NezhaServiceGrpc;
import com.nezhahq.agent.proto.Receipt;
import com.nezhahq.agent.proto.State;
import com.nezhahq.agent.proto.State_SensorTemperature;
import com.nezhahq.agent.proto.Task;
import com.nezhahq.agent.proto.TaskResult;
import com.nezhahq.agent.proto.Uint64Receipt;
import com.pty4j.PtyProcess;
import com.pty4j.PtyProcessBuilder;
import com.pty4j.WinSize;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.stub.StreamObserver;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.GraphicsCard;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;
import oshi.hardware.Sensors;
import oshi.software.os.FileSystem;
import oshi.software.os.InternetProtocolStats;
import oshi.software.os.OSFileStore;
import oshi.software.os.OperatingSystem;

import javax.naming.Context;
import javax.naming.InvalidNameException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.CodeSource;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Single-file embeddable Java implementation of the Nezha agent client.
 * Generated protobuf/gRPC classes still come from src/main/proto/nezha.proto.
 */
public final class NezhaJavaAgent {
    public static final String VERSION = "java-agent-0.1.0-SNAPSHOT";
    private static final Logger LOGGER = Logger.getLogger(NezhaJavaAgent.class.getName());

    private NezhaJavaAgent() {
    }

    public static void main(String[] args) throws Exception {
        Path configPath = parseConfigPath(args);
        NezhaAgentClient client = createClient(configPath);

        Runtime.getRuntime().addShutdownHook(new Thread(client::stop, "nezha-agent-shutdown"));

        try {
            client.runForever();
        } catch (Exception error) {
            LOGGER.log(Level.SEVERE, "Agent stopped with an unrecoverable error", error);
            throw error;
        }
    }

    public static Path parseConfigPath(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (("-c".equals(arg) || "--config".equals(arg)) && i + 1 < args.length) {
                return Path.of(args[i + 1]).toAbsolutePath().normalize();
            }
            if (arg.startsWith("--config=")) {
                return Path.of(arg.substring("--config=".length())).toAbsolutePath().normalize();
            }
        }
        return defaultConfigPath();
    }

    public static AgentConfig loadConfig(Path configPath) throws IOException {
        return AgentConfig.load(normalizeConfigPath(configPath));
    }

    public static NezhaAgentClient createClient(Path configPath) throws IOException {
        Path normalizedPath = normalizeConfigPath(configPath);
        return new NezhaAgentClient(normalizedPath, AgentConfig.load(normalizedPath));
    }

    public static RunningAgent start(Path configPath) throws IOException {
        return createClient(configPath).start();
    }

    public static CompletableFuture<Void> startAsync(Path configPath) throws IOException {
        return start(configPath).completion();
    }

    public static void runForever(Path configPath) throws IOException, InterruptedException {
        createClient(configPath).runForever();
    }

    private static Path normalizeConfigPath(Path configPath) {
        return Objects.requireNonNull(configPath, "configPath").toAbsolutePath().normalize();
    }

    public static Path defaultConfigPath() {
        try {
            CodeSource codeSource = NezhaJavaAgent.class.getProtectionDomain().getCodeSource();
            if (codeSource != null && codeSource.getLocation() != null) {
                Path location = Path.of(codeSource.getLocation().toURI()).toAbsolutePath().normalize();
                Path directory = Files.isRegularFile(location) ? location.getParent() : location;
                if (directory != null) {
                    return directory.resolve("config.yml").toAbsolutePath().normalize();
                }
            }
        } catch (Exception ignored) {
            // Fall back to the current working directory below.
        }
        return Path.of("config.yml").toAbsolutePath().normalize();
    }

    private static InputStream responseStream(HttpURLConnection connection) throws IOException {
        try {
            return connection.getInputStream();
        } catch (IOException error) {
            InputStream errorStream = connection.getErrorStream();
            if (errorStream != null) {
                return errorStream;
            }
            throw error;
        }
    }

public static final class RunningAgent implements AutoCloseable {
    private final NezhaAgentClient client;
    private final CompletableFuture<Void> completion;

    private RunningAgent(NezhaAgentClient client, CompletableFuture<Void> completion) {
        this.client = client;
        this.completion = completion;
    }

    public NezhaAgentClient client() {
        return client;
    }

    public CompletableFuture<Void> completion() {
        return completion;
    }

    public void stop() {
        client.stop();
    }

    @Override
    public void close() {
        stop();
    }
}


public static final class AgentConfig {
    private boolean debug;
    private String server = "";
    private String clientSecret = "";
    private String uuid = "";
    private List<String> hardDrivePartitionAllowlist = new ArrayList<>();
    private Map<String, Boolean> nicAllowlist = new LinkedHashMap<>();
    private List<String> dns = new ArrayList<>();
    private boolean gpu;
    private boolean temperature;
    private boolean skipConnectionCount;
    private boolean skipProcsCount;
    private boolean disableAutoUpdate;
    private boolean disableForceUpdate;
    private boolean disableCommandExecute;
    private int reportDelay;
    private boolean tls;
    private boolean insecureTls;
    private boolean useIpv6CountryCode;
    private boolean useGiteeToUpgrade;
    private boolean useAtomGitToUpgrade;
    private boolean disableNat;
    private boolean disableSendQuery;
    private int ipReportPeriod;
    private int selfUpdatePeriod;
    private List<String> customIpApi = new ArrayList<>();

    private Path filePath;

    public static AgentConfig load(Path path) throws IOException {
        AgentConfig config = new AgentConfig();
        config.filePath = path;

        if (Files.exists(path)) {
            config.applyMap(readYaml(path));
        }
        config.applyEnvironment(System.getenv());

        boolean generatedUuid = false;
        if (config.uuid == null || config.uuid.isBlank()) {
            config.uuid = UUID.randomUUID().toString();
            generatedUuid = true;
        }

        config.validate(false);
        if (!Files.exists(path) || generatedUuid) {
            config.save();
        }
        return config;
    }

    public AgentConfig copy() {
        AgentConfig copy = new AgentConfig();
        copy.filePath = filePath;
        copy.applyMap(toMap());
        return copy;
    }

    public void applyRemoteOverlay(Map<String, ?> overlay) {
        applyMap(overlay);
        validate(true);
    }

    public void save() throws IOException {
        Objects.requireNonNull(filePath, "config file path is not set");
        Path parent = filePath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(true);
        Yaml yaml = new Yaml(options);
        try (Writer writer = Files.newBufferedWriter(filePath, StandardCharsets.UTF_8)) {
            yaml.dump(toMap(), writer);
        }
    }

    public Map<String, Object> toMap() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("debug", debug);
        out.put("server", server);
        out.put("client_secret", clientSecret);
        out.put("uuid", uuid);
        out.put("hard_drive_partition_allowlist", hardDrivePartitionAllowlist);
        out.put("nic_allowlist", nicAllowlist);
        out.put("dns", dns);
        out.put("gpu", gpu);
        out.put("temperature", temperature);
        out.put("skip_connection_count", skipConnectionCount);
        out.put("skip_procs_count", skipProcsCount);
        out.put("disable_auto_update", disableAutoUpdate);
        out.put("disable_force_update", disableForceUpdate);
        out.put("disable_command_execute", disableCommandExecute);
        out.put("report_delay", reportDelay);
        out.put("tls", tls);
        out.put("insecure_tls", insecureTls);
        out.put("use_ipv6_country_code", useIpv6CountryCode);
        out.put("use_gitee_to_upgrade", useGiteeToUpgrade);
        out.put("use_atomgit_to_upgrade", useAtomGitToUpgrade);
        out.put("disable_nat", disableNat);
        out.put("disable_send_query", disableSendQuery);
        out.put("ip_report_period", ipReportPeriod);
        out.put("self_update_period", selfUpdatePeriod);
        out.put("custom_ip_api", customIpApi);
        return out;
    }

    private void applyEnvironment(Map<String, String> env) {
        Map<String, Object> overlay = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : env.entrySet()) {
            if (!entry.getKey().startsWith("NZ_")) {
                continue;
            }
            String key = entry.getKey().substring(3).toLowerCase(Locale.ROOT);
            overlay.put(key, entry.getValue());
        }
        applyMap(overlay);
    }

    private void applyMap(Map<String, ?> values) {
        if (values == null) {
            return;
        }
        for (Map.Entry<String, ?> entry : values.entrySet()) {
            String key = normalizeKey(entry.getKey());
            Object value = entry.getValue();
            switch (key) {
                case "debug" -> debug = boolValue(value, debug);
                case "server" -> server = stringValue(value, server);
                case "client_secret" -> clientSecret = stringValue(value, clientSecret);
                case "uuid" -> uuid = stringValue(value, uuid);
                case "hard_drive_partition_allowlist" -> hardDrivePartitionAllowlist = stringList(value);
                case "nic_allowlist" -> nicAllowlist = boolMap(value);
                case "dns" -> dns = stringList(value);
                case "gpu" -> gpu = boolValue(value, gpu);
                case "temperature" -> temperature = boolValue(value, temperature);
                case "skip_connection_count" -> skipConnectionCount = boolValue(value, skipConnectionCount);
                case "skip_procs_count" -> skipProcsCount = boolValue(value, skipProcsCount);
                case "disable_auto_update" -> disableAutoUpdate = boolValue(value, disableAutoUpdate);
                case "disable_force_update" -> disableForceUpdate = boolValue(value, disableForceUpdate);
                case "disable_command_execute" -> disableCommandExecute = boolValue(value, disableCommandExecute);
                case "report_delay" -> reportDelay = intValue(value, reportDelay);
                case "tls" -> tls = boolValue(value, tls);
                case "insecure_tls" -> insecureTls = boolValue(value, insecureTls);
                case "use_ipv6_country_code" -> useIpv6CountryCode = boolValue(value, useIpv6CountryCode);
                case "use_gitee_to_upgrade" -> useGiteeToUpgrade = boolValue(value, useGiteeToUpgrade);
                case "use_atomgit_to_upgrade" -> useAtomGitToUpgrade = boolValue(value, useAtomGitToUpgrade);
                case "disable_nat" -> disableNat = boolValue(value, disableNat);
                case "disable_send_query" -> disableSendQuery = boolValue(value, disableSendQuery);
                case "ip_report_period" -> ipReportPeriod = intValue(value, ipReportPeriod);
                case "self_update_period" -> selfUpdatePeriod = intValue(value, selfUpdatePeriod);
                case "custom_ip_api" -> customIpApi = stringList(value);
                default -> {
                    // Keep unknown keys forward-compatible with the Go agent config.
                }
            }
        }
        normalizeDefaults();
    }

    public void validate(boolean remoteEdit) {
        normalizeDefaults();
        if (reportDelay < 1 || reportDelay > 4) {
            throw new IllegalArgumentException("report_delay ranges from 1-4");
        }
        if (!remoteEdit) {
            if (server == null || server.isBlank()) {
                throw new IllegalArgumentException("server address should not be empty");
            }
            if (clientSecret == null || clientSecret.isBlank()) {
                throw new IllegalArgumentException("client_secret must be specified");
            }
            UUID.fromString(uuid);
        }
    }

    private void normalizeDefaults() {
        if (reportDelay == 0) {
            reportDelay = 3;
        }
        if (ipReportPeriod == 0) {
            ipReportPeriod = 1800;
        } else if (ipReportPeriod < 30) {
            ipReportPeriod = 30;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> readYaml(Path path) throws IOException {
        Yaml yaml = new Yaml();
        try (InputStream input = Files.newInputStream(path)) {
            Object loaded = yaml.load(input);
            if (loaded == null) {
                return Map.of();
            }
            if (!(loaded instanceof Map<?, ?> map)) {
                throw new IllegalArgumentException("config file must be a YAML object");
            }
            return (Map<String, Object>) map;
        }
    }

    private static String normalizeKey(String key) {
        return key.replace("-", "_").toLowerCase(Locale.ROOT);
    }

    private static boolean boolValue(Object value, boolean fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Boolean b) {
            return b;
        }
        return Boolean.parseBoolean(value.toString().trim());
    }

    private static int intValue(Object value, int fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        String text = value.toString().trim();
        if (text.isEmpty()) {
            return fallback;
        }
        return Integer.parseInt(text);
    }

    private static String stringValue(Object value, String fallback) {
        if (value == null) {
            return fallback;
        }
        return value.toString();
    }

    private static List<String> stringList(Object value) {
        List<String> out = new ArrayList<>();
        if (value == null) {
            return out;
        }
        if (value instanceof Iterable<?> iterable) {
            for (Object item : iterable) {
                if (item != null && !item.toString().isBlank()) {
                    out.add(item.toString());
                }
            }
            return out;
        }
        String text = value.toString().trim();
        if (text.isEmpty()) {
            return out;
        }
        for (String item : text.split(",")) {
            if (!item.isBlank()) {
                out.add(item.trim());
            }
        }
        return out;
    }

    private static Map<String, Boolean> boolMap(Object value) {
        Map<String, Boolean> out = new LinkedHashMap<>();
        if (value == null) {
            return out;
        }
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getKey() != null) {
                    out.put(entry.getKey().toString(), boolValue(entry.getValue(), true));
                }
            }
            return out;
        }
        for (String item : stringList(value)) {
            out.put(item, true);
        }
        return out;
    }

    public boolean isDebug() {
        return debug;
    }

    public String getServer() {
        return server;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public String getUuid() {
        return uuid;
    }

    public List<String> getHardDrivePartitionAllowlist() {
        return hardDrivePartitionAllowlist;
    }

    public Map<String, Boolean> getNicAllowlist() {
        return nicAllowlist;
    }

    public List<String> getDns() {
        return dns;
    }

    public boolean isGpu() {
        return gpu;
    }

    public boolean isTemperature() {
        return temperature;
    }

    public boolean isSkipConnectionCount() {
        return skipConnectionCount;
    }

    public boolean isSkipProcsCount() {
        return skipProcsCount;
    }

    public boolean isDisableCommandExecute() {
        return disableCommandExecute;
    }

    public boolean isDisableForceUpdate() {
        return disableForceUpdate;
    }

    public int getReportDelay() {
        return reportDelay;
    }

    public boolean isTls() {
        return tls;
    }

    public boolean isInsecureTls() {
        return insecureTls;
    }

    public boolean isUseIpv6CountryCode() {
        return useIpv6CountryCode;
    }

    public boolean isDisableNat() {
        return disableNat;
    }

    public boolean isDisableSendQuery() {
        return disableSendQuery;
    }

    public int getIpReportPeriod() {
        return ipReportPeriod;
    }

    public List<String> getCustomIpApi() {
        return customIpApi;
    }
}

private static final class AuthHeadersInterceptor implements ClientInterceptor {
    private static final Metadata.Key<String> CLIENT_SECRET =
            Metadata.Key.of("client_secret", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> CLIENT_UUID =
            Metadata.Key.of("client_uuid", Metadata.ASCII_STRING_MARSHALLER);

    private final Supplier<AgentConfig> configSupplier;

    AuthHeadersInterceptor(Supplier<AgentConfig> configSupplier) {
        this.configSupplier = configSupplier;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions,
            Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                AgentConfig config = configSupplier.get();
                headers.put(CLIENT_SECRET, config.getClientSecret());
                headers.put(CLIENT_UUID, config.getUuid());
                super.start(responseListener, headers);
            }
        };
    }
}

public static final class NezhaAgentClient {
    private static final Logger LOGGER = Logger.getLogger(NezhaAgentClient.class.getName());
    private static final Duration NETWORK_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(10);

    private final Path configPath;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean reloadPending = new AtomicBoolean();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
    private final ExecutorService taskExecutor = Executors.newCachedThreadPool();

    private volatile AgentConfig config;
    private volatile ManagedChannel channel;
    private volatile long previousDashboardBootTime;
    private final AtomicReference<CountDownLatch> disconnectSignal = new AtomicReference<>();

    public NezhaAgentClient(Path configPath, AgentConfig config) {
        this.configPath = configPath;
        this.config = config;
    }

    public void runForever() throws InterruptedException {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Agent client is already running");
        }
        try {
            while (running.get()) {
                try {
                    runOnce();
                } catch (Exception error) {
                    if (running.get() && config.isDebug()) {
                        LOGGER.log(Level.WARNING, "Agent worker failed, reconnecting", error);
                    }
                } finally {
                    shutdownChannel();
                }
                if (running.get()) {
                    waitBeforeReconnect();
                }
            }
        } finally {
            started.set(false);
        }
    }

    public RunningAgent start() {
        CompletableFuture<Void> completion = startAsync();
        return new RunningAgent(this, completion);
    }

    public CompletableFuture<Void> startAsync() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Thread worker = new Thread(() -> {
            try {
                runForever();
                future.complete(null);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                future.completeExceptionally(error);
            } catch (Throwable error) {
                future.completeExceptionally(error);
            }
        }, "nezha-agent-client");
        worker.start();
        return future;
    }

    public void stop() {
        running.set(false);
        shutdownChannel();
        scheduler.shutdownNow();
        taskExecutor.shutdownNow();
    }

    private void runOnce() throws Exception {
        AgentConfig initialConfig = config;
        ManagedChannel managedChannel = newChannel(initialConfig);
        channel = managedChannel;
        Channel authedChannel = ClientInterceptors.intercept(
                managedChannel,
                new AuthHeadersInterceptor(() -> config)
        );

        NezhaServiceGrpc.NezhaServiceBlockingStub blockingStub = NezhaServiceGrpc.newBlockingStub(authedChannel);
        NezhaServiceGrpc.NezhaServiceStub asyncStub = NezhaServiceGrpc.newStub(authedChannel);
        MonitorService monitor = new MonitorService(() -> config);
        GeoIpService geoIpService = new GeoIpService();
        StreamTaskRunner streamTaskRunner = new StreamTaskRunner(() -> config, asyncStub, scheduler, taskExecutor);
        TaskHandler taskHandler = new TaskHandler(() -> config, configPath, this::replaceConfig, streamTaskRunner);

        updateGeoForceFlag(reportHost(blockingStub, monitor), geoIpService);
        if (initialConfig.isDebug()) {
            LOGGER.info(() -> "Connection to " + initialConfig.getServer() + " established");
        }

        CountDownLatch disconnected = new CountDownLatch(1);
        disconnectSignal.set(disconnected);
        AtomicReference<StreamObserver<TaskResult>> taskSinkRef = new AtomicReference<>();

        StreamObserver<Task> taskReceiver = new StreamObserver<>() {
            @Override
            public void onNext(Task task) {
                StreamObserver<TaskResult> sink = taskSinkRef.get();
                taskExecutor.execute(() -> {
                    Optional<TaskResult> result = taskHandler.handle(task);
                    result.ifPresent(taskResult -> sendTaskResult(sink, taskResult, disconnected));
                });
            }

            @Override
            public void onError(Throwable throwable) {
                logStreamFailure("task stream", throwable);
                disconnected.countDown();
            }

            @Override
            public void onCompleted() {
                disconnected.countDown();
            }
        };
        StreamObserver<TaskResult> taskSink = asyncStub.requestTask(taskReceiver);
        taskSinkRef.set(taskSink);

        StreamObserver<State> stateSink = asyncStub.reportSystemState(new StreamObserver<>() {
            @Override
            public void onNext(Receipt receipt) {
                // The dashboard receipt is an acknowledgement; no local state is needed for now.
            }

            @Override
            public void onError(Throwable throwable) {
                logStreamFailure("state stream", throwable);
                disconnected.countDown();
            }

            @Override
            public void onCompleted() {
                disconnected.countDown();
            }
        });

        ScheduledFuture<?> stateFuture = scheduler.scheduleAtFixedRate(
                () -> sendState(stateSink, monitor, disconnected),
                0,
                config.getReportDelay(),
                TimeUnit.SECONDS
        );
        ScheduledFuture<?> hostFuture = scheduler.scheduleAtFixedRate(
                () -> safeReportHost(blockingStub, monitor, geoIpService, disconnected),
                10,
                600,
                TimeUnit.SECONDS
        );
        ScheduledFuture<?> geoFuture = scheduler.scheduleAtFixedRate(
                () -> safeReportGeoIp(blockingStub, geoIpService, disconnected),
                0,
                Math.max(30, config.getIpReportPeriod()),
                TimeUnit.SECONDS
        );

        disconnected.await();
        disconnectSignal.compareAndSet(disconnected, null);

        stateFuture.cancel(true);
        hostFuture.cancel(true);
        geoFuture.cancel(true);
        completeQuietly(stateSink);
        completeQuietly(taskSink);
    }

    private ManagedChannel newChannel(AgentConfig config) throws SSLException {
        NettyChannelBuilder builder = NettyChannelBuilder
                .forTarget(config.getServer())
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .maxInboundMessageSize(16 * 1024 * 1024);

        if (!config.isTls()) {
            builder.usePlaintext();
        } else if (config.isInsecureTls()) {
            builder.sslContext(GrpcSslContexts.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build());
        } else {
            builder.useTransportSecurity();
        }
        return builder.build();
    }

    private long reportHost(NezhaServiceGrpc.NezhaServiceBlockingStub blockingStub, MonitorService monitor) {
        Host host = monitor.collectHost();
        Uint64Receipt receipt = blockingStub
                .withDeadlineAfter(NETWORK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .reportSystemInfo2(host);
        LOGGER.fine(() -> "Reported host info, dashboard boot time: " + receipt.getData());
        return receipt.getData();
    }

    private void safeReportHost(
            NezhaServiceGrpc.NezhaServiceBlockingStub blockingStub,
            MonitorService monitor,
            GeoIpService geoIpService,
            CountDownLatch disconnected) {
        try {
            long dashboardBootTime = reportHost(blockingStub, monitor);
            updateGeoForceFlag(dashboardBootTime, geoIpService);
        } catch (Exception error) {
            LOGGER.log(Level.WARNING, "ReportSystemInfo2 failed", error);
            disconnected.countDown();
        }
    }

    private void safeReportGeoIp(
            NezhaServiceGrpc.NezhaServiceBlockingStub blockingStub,
            GeoIpService geoIpService,
            CountDownLatch disconnected) {
        try {
            AgentConfig current = config;
            Optional<GeoIP> geoIp = geoIpService.fetch(current.isUseIpv6CountryCode(), current.getCustomIpApi());
            if (geoIp.isEmpty()) {
                return;
            }
            if (!geoIpService.shouldReport()) {
                return;
            }
            GeoIP dashboardGeoIp = blockingStub
                    .withDeadlineAfter(NETWORK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .reportGeoIP(geoIp.get());
            if (dashboardGeoIp.getDashboardBootTime() > 0) {
                previousDashboardBootTime = dashboardGeoIp.getDashboardBootTime();
            }
            geoIpService.markReported(dashboardGeoIp);
        } catch (Exception error) {
            LOGGER.log(Level.FINE, "ReportGeoIP failed", error);
        }
    }

    private void updateGeoForceFlag(long dashboardBootTime, GeoIpService geoIpService) {
        boolean dashboardRestarted = previousDashboardBootTime == 0 || dashboardBootTime != previousDashboardBootTime;
        previousDashboardBootTime = dashboardBootTime;
        if (geoIpService != null && dashboardRestarted) {
            geoIpService.forceNextReport();
        }
    }

    private void sendState(StreamObserver<State> stateSink, MonitorService monitor, CountDownLatch disconnected) {
        try {
            stateSink.onNext(monitor.collectState());
        } catch (Exception error) {
            LOGGER.log(Level.WARNING, "ReportSystemState failed", error);
            disconnected.countDown();
        }
    }

    private void sendTaskResult(
            StreamObserver<TaskResult> sink,
            TaskResult taskResult,
            CountDownLatch disconnected) {
        if (sink == null) {
            return;
        }
        try {
            sink.onNext(taskResult);
        } catch (Exception error) {
            LOGGER.log(Level.WARNING, "Sending task result failed", error);
            disconnected.countDown();
        }
    }

    private void replaceConfig(AgentConfig nextConfig) {
        nextConfig.validate(false);
        this.config = nextConfig;
        LOGGER.info("Configuration updated; reconnecting in 10 seconds");
        if (reloadPending.compareAndSet(false, true)) {
            scheduler.schedule(() -> {
                try {
                    shutdownChannel();
                } finally {
                    reloadPending.set(false);
                }
            }, 10, TimeUnit.SECONDS);
        }
    }

    private void shutdownChannel() {
        CountDownLatch disconnected = disconnectSignal.getAndSet(null);
        if (disconnected != null) {
            disconnected.countDown();
        }
        ManagedChannel current = channel;
        channel = null;
        if (current != null) {
            current.shutdownNow();
        }
    }

    private void waitBeforeReconnect() throws InterruptedException {
        CountDownLatch sleepSignal = new CountDownLatch(1);
        if (!disconnectSignal.compareAndSet(null, sleepSignal)) {
            TimeUnit.MILLISECONDS.sleep(RECONNECT_DELAY.toMillis());
            return;
        }
        try {
            sleepSignal.await(RECONNECT_DELAY.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            disconnectSignal.compareAndSet(sleepSignal, null);
        }
    }

    private static void completeQuietly(StreamObserver<?> streamObserver) {
        try {
            streamObserver.onCompleted();
        } catch (Exception ignored) {
            // Stream is already closed.
        }
    }

    private static void logStreamFailure(String name, Throwable throwable) {
        Status status = Status.fromThrowable(throwable);
        LOGGER.log(Level.WARNING, name + " closed: " + status, throwable);
    }
}

public static final class GeoIpService {
    private static final List<String> DEFAULT_ENDPOINTS = List.of(
            "https://blog.cloudflare.com/cdn-cgi/trace",
            "https://developers.cloudflare.com/cdn-cgi/trace",
            "https://hostinger.com/cdn-cgi/trace",
            "https://ahrefs.com/cdn-cgi/trace"
    );

    private int retryTimes;
    private long failedStartedAtMillis;
    private long latestRetryAtMillis;
    private String geoQueryIp = "";
    private boolean geoQueryIpChanged = true;
    private boolean forceNextReport = true;
    private String cachedCountryCode = "";

    public Optional<GeoIP> fetch(boolean preferIpv6CountryCode, List<String> customEndpoints) {
        List<String> endpoints = customEndpoints == null || customEndpoints.isEmpty()
                ? DEFAULT_ENDPOINTS
                : customEndpoints;

        String ipv4 = fetchIp(endpoints, false);
        String ipv6 = fetchIp(endpoints, true);

        if (ipv4.isBlank() && ipv6.isBlank()) {
            return fallbackGeoIp();
        }

        String selectedIp = selectedIp(preferIpv6CountryCode, ipv4, ipv6);
        geoQueryIpChanged = !Objects.equals(geoQueryIp, selectedIp) || geoQueryIpChanged;
        geoQueryIp = selectedIp;
        retryTimes = 0;
        return Optional.of(GeoIP.newBuilder()
                .setUse6(preferIpv6CountryCode)
                .setIp(IP.newBuilder().setIpv4(ipv4).setIpv6(ipv6).build())
                .build());
    }

    public boolean shouldReport() {
        return forceNextReport || geoQueryIpChanged;
    }

    public void markReported(GeoIP dashboardGeoIp) {
        if (dashboardGeoIp != null) {
            cachedCountryCode = dashboardGeoIp.getCountryCode();
        }
        forceNextReport = false;
        geoQueryIpChanged = false;
    }

    public void forceNextReport() {
        forceNextReport = true;
    }

    public String getCachedCountryCode() {
        return cachedCountryCode;
    }

    private static String selectedIp(boolean preferIpv6CountryCode, String ipv4, String ipv6) {
        if (!ipv6.isBlank() && (preferIpv6CountryCode || ipv4.isBlank())) {
            return ipv6;
        }
        return ipv4;
    }

    private Optional<GeoIP> fallbackGeoIp() {
        long now = System.currentTimeMillis();
        if (retryTimes > 2 && now < latestRetryAtMillis + ((latestRetryAtMillis - failedStartedAtMillis) * 2)) {
            geoQueryIpChanged = !geoQueryIp.isBlank() || geoQueryIpChanged;
            geoQueryIp = "";
            return Optional.of(GeoIP.newBuilder()
                    .setUse6(false)
                    .setIp(IP.newBuilder().build())
                    .build());
        }

        retryTimes++;
        latestRetryAtMillis = now;
        if (retryTimes == 1) {
            failedStartedAtMillis = now;
        }
        return Optional.empty();
    }

    private String fetchIp(List<String> endpoints, boolean ipv6) {
        for (String endpoint : endpoints) {
            try {
                String candidate = extractIp(getEndpointBody(endpoint));
                if (candidate.isBlank()) {
                    continue;
                }
                if (isIpVersion(candidate, ipv6)) {
                    return candidate;
                }
            } catch (IOException | IllegalArgumentException ignored) {
            }
        }
        return "";
    }

    private static String getEndpointBody(String endpoint) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) URI.create(endpoint).toURL().openConnection();
        connection.setConnectTimeout(5_000);
        connection.setReadTimeout(20_000);
        connection.setInstanceFollowRedirects(false);
        connection.setRequestMethod("GET");
        connection.setRequestProperty("User-Agent", "nezha-agent/1.0");
        try (InputStream input = responseStream(connection)) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } finally {
            connection.disconnect();
        }
    }

    private static String extractIp(String body) {
        if (body == null || body.isBlank()) {
            return "";
        }
        for (String line : body.split("\\R")) {
            String trimmed = line.trim();
            if (trimmed.startsWith("ip=")) {
                return trimmed.substring(3).trim();
            }
        }
        return body.replace("\n", "").replace("\r", "").trim();
    }

    private static boolean isIpVersion(String value, boolean ipv6) {
        try {
            InetAddress address = InetAddress.getByName(value);
            boolean looksIpv6 = address.getHostAddress().contains(":");
            return ipv6 == looksIpv6;
        } catch (Exception ignored) {
            return false;
        }
    }
}

public static final class MonitorService {
    private static final Set<String> DEFAULT_EXCLUDED_NIC_PARTS = Set.of(
            "lo", "tun", "docker", "veth", "br-", "vmbr", "vnet", "kube", "Meta", "tailscale", "fw", "tap"
    );

    private final Supplier<AgentConfig> configSupplier;
    private final SystemInfo systemInfo = new SystemInfo();
    private final HardwareAbstractionLayer hardware = systemInfo.getHardware();
    private final OperatingSystem os = systemInfo.getOperatingSystem();
    private final AtomicReference<long[]> cpuTicks = new AtomicReference<>();
    private final GpuMonitor gpuMonitor = new GpuMonitor();

    private long netInTransfer;
    private long netOutTransfer;
    private long netInSpeed;
    private long netOutSpeed;
    private long lastNetworkSampleSecond;

    public MonitorService(Supplier<AgentConfig> configSupplier) {
        this.configSupplier = configSupplier;
        this.cpuTicks.set(hardware.getProcessor().getSystemCpuLoadTicks());
    }

    public Host collectHost() {
        AgentConfig config = configSupplier.get();
        CentralProcessor processor = hardware.getProcessor();
        GlobalMemory memory = hardware.getMemory();

        Host.Builder host = Host.newBuilder()
                .setPlatform(os.getFamily())
                .setPlatformVersion(os.getVersionInfo().getVersion())
                .setArch(System.getProperty("os.arch", ""))
                .setVirtualization(virtualization())
                .setBootTime(os.getSystemBootTime())
                .setMemTotal(memory.getTotal())
                .setSwapTotal(memory.getVirtualMemory().getSwapTotal())
                .setDiskTotal(diskTotal(config))
                .setVersion(VERSION);

        String cpuName = processor.getProcessorIdentifier().getName();
        if (cpuName != null && !cpuName.isBlank()) {
            host.addCpu(cpuName);
        }

        if (config.isGpu()) {
            List<String> gpuNames = gpuMonitor.models();
            if (gpuNames.isEmpty()) {
                gpuNames = hardware.getGraphicsCards().stream()
                        .map(GraphicsCard::getName)
                        .filter(name -> name != null && !name.isBlank())
                        .toList();
            }
            for (String gpuName : gpuNames) {
                host.addGpu(gpuName);
            }
        }

        return host.build();
    }

    public State collectState() {
        AgentConfig config = configSupplier.get();
        CentralProcessor processor = hardware.getProcessor();
        GlobalMemory memory = hardware.getMemory();
        NetworkStats networkStats = trackNetwork(config);

        long[] oldTicks = cpuTicks.getAndSet(processor.getSystemCpuLoadTicks());
        double cpuPercent = processor.getSystemCpuLoadBetweenTicks(oldTicks) * 100.0;
        double[] loadAverage = processor.getSystemLoadAverage(3);

        State.Builder state = State.newBuilder()
                .setCpu(nonNegative(cpuPercent))
                .setMemUsed(memory.getTotal() - memory.getAvailable())
                .setSwapUsed(memory.getVirtualMemory().getSwapUsed())
                .setDiskUsed(diskUsed(config))
                .setNetInTransfer(networkStats.inTransfer())
                .setNetOutTransfer(networkStats.outTransfer())
                .setNetInSpeed(networkStats.inSpeed())
                .setNetOutSpeed(networkStats.outSpeed())
                .setUptime(os.getSystemUptime())
                .setLoad1(loadValue(loadAverage, 0))
                .setLoad5(loadValue(loadAverage, 1))
                .setLoad15(loadValue(loadAverage, 2));

        if (!config.isSkipConnectionCount()) {
            ConnectionStats connectionStats = connectionStats();
            state.setTcpConnCount(connectionStats.tcp());
            state.setUdpConnCount(connectionStats.udp());
        }

        if (!config.isSkipProcsCount()) {
            state.setProcessCount(os.getProcessCount());
        }

        if (config.isTemperature()) {
            Sensors sensors = hardware.getSensors();
            double cpuTemperature = sensors.getCpuTemperature();
            if (cpuTemperature > 0) {
                state.addTemperatures(State_SensorTemperature.newBuilder()
                        .setName("CPU")
                        .setTemperature(cpuTemperature)
                        .build());
            }
        }

        if (config.isGpu()) {
            state.addAllGpu(gpuMonitor.usage());
        }

        return state.build();
    }

    private String virtualization() {
        try {
            Object result = os.getClass().getMethod("getVirtualization").invoke(os);
            return result == null ? "" : result.toString();
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            return "";
        }
    }

    private long diskTotal(AgentConfig config) {
        long total = 0;
        for (OSFileStore store : filteredFileStores(config)) {
            total += Math.max(0, store.getTotalSpace());
        }
        return total;
    }

    private long diskUsed(AgentConfig config) {
        long used = 0;
        for (OSFileStore store : filteredFileStores(config)) {
            long total = Math.max(0, store.getTotalSpace());
            long usable = Math.max(0, store.getUsableSpace());
            used += Math.max(0, total - usable);
        }
        return used;
    }

    private List<OSFileStore> filteredFileStores(AgentConfig config) {
        FileSystem fileSystem = os.getFileSystem();
        List<String> allowlist = config.getHardDrivePartitionAllowlist();
        if (allowlist == null || allowlist.isEmpty()) {
            return fileSystem.getFileStores();
        }
        return fileSystem.getFileStores().stream()
                .filter(store -> allowlist.contains(store.getMount()) || allowlist.contains(store.getName()))
                .toList();
    }

    private synchronized NetworkStats trackNetwork(AgentConfig config) {
        long in = 0;
        long out = 0;
        Map<String, Boolean> allowlist = config.getNicAllowlist();
        for (NetworkIF networkIF : hardware.getNetworkIFs()) {
            networkIF.updateAttributes();
            if (!shouldTrackNic(networkIF.getName(), allowlist)) {
                continue;
            }
            in += Math.max(0, networkIF.getBytesRecv());
            out += Math.max(0, networkIF.getBytesSent());
        }

        long now = System.currentTimeMillis() / 1000;
        long diff = now - lastNetworkSampleSecond;
        if (diff > 0) {
            netInSpeed = Math.max(0, in - netInTransfer) / diff;
            netOutSpeed = Math.max(0, out - netOutTransfer) / diff;
        }
        netInTransfer = in;
        netOutTransfer = out;
        lastNetworkSampleSecond = now;
        return new NetworkStats(netInTransfer, netOutTransfer, netInSpeed, netOutSpeed);
    }

    private ConnectionStats connectionStats() {
        InternetProtocolStats protocolStats = os.getInternetProtocolStats();
        long tcp = 0;
        long udp = 0;
        try {
            for (InternetProtocolStats.IPConnection connection : protocolStats.getConnections()) {
                String type = connection.getType();
                if (type == null) {
                    continue;
                }
                String normalized = type.toLowerCase(Locale.ROOT);
                if (normalized.startsWith("tcp")) {
                    tcp++;
                } else if (normalized.startsWith("udp")) {
                    udp++;
                }
            }
        } catch (RuntimeException error) {
            tcp = safeEstablished(protocolStats.getTCPv4Stats()) + safeEstablished(protocolStats.getTCPv6Stats());
        }
        return new ConnectionStats(tcp, udp);
    }

    private static long safeEstablished(InternetProtocolStats.TcpStats stats) {
        if (stats == null) {
            return 0;
        }
        return Math.max(0, stats.getConnectionsEstablished());
    }

    private static boolean shouldTrackNic(String name, Map<String, Boolean> allowlist) {
        if (name == null || name.isBlank()) {
            return false;
        }
        if (allowlist != null && !allowlist.isEmpty()) {
            return Boolean.TRUE.equals(allowlist.get(name));
        }
        String lowerName = name.toLowerCase(Locale.ROOT);
        for (String part : DEFAULT_EXCLUDED_NIC_PARTS) {
            if (lowerName.contains(part.toLowerCase(Locale.ROOT))) {
                return false;
            }
        }
        return true;
    }

    private static double loadValue(double[] values, int index) {
        if (values == null || values.length <= index || values[index] < 0) {
            return 0;
        }
        return values[index];
    }

    private static double nonNegative(double value) {
        if (Double.isNaN(value) || value < 0) {
            return 0;
        }
        return value;
    }

    private record NetworkStats(long inTransfer, long outTransfer, long inSpeed, long outSpeed) {
    }

    private record ConnectionStats(long tcp, long udp) {
    }
}

static final class GpuMonitor {
    private static final Logger LOGGER = Logger.getLogger(GpuMonitor.class.getName());
    private static final Pattern XML_PRODUCT_NAME =
            Pattern.compile("<product_name>\\s*([^<]+?)\\s*</product_name>", Pattern.CASE_INSENSITIVE);
    private static final Pattern XML_GPU_UTIL =
            Pattern.compile("<gpu_util>\\s*([0-9.]+)\\s*%?\\s*</gpu_util>", Pattern.CASE_INSENSITIVE);
    private static final Pattern AMD_CARD_BLOCK =
            Pattern.compile("\"card\\d+\"\\s*:\\s*\\{([^}]+)}", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern AMD_CARD_SERIES =
            Pattern.compile("\"Card series\"\\s*:\\s*\"([^\"]+)\"", Pattern.CASE_INSENSITIVE);
    private static final Pattern AMD_GPU_USE =
            Pattern.compile("\"GPU use \\(\\%\\)\"\\s*:\\s*\"?([0-9.]+)", Pattern.CASE_INSENSITIVE);

    private final AtomicReference<String> nvidiaSmiPath = new AtomicReference<>();
    private final AtomicReference<String> rocmSmiPath = new AtomicReference<>();
    private final AtomicReference<String> intelGpuTopPath = new AtomicReference<>();

    List<String> models() {
        List<String> nvidia = nvidiaModels();
        if (!nvidia.isEmpty()) {
            return nvidia;
        }
        List<String> amd = amdModels();
        if (!amd.isEmpty()) {
            return amd;
        }
        return List.of();
    }

    List<Double> usage() {
        List<Double> nvidia = nvidiaUsage();
        if (!nvidia.isEmpty()) {
            return nvidia;
        }
        List<Double> amd = amdUsage();
        if (!amd.isEmpty()) {
            return amd;
        }
        List<Double> intel = intelUsage();
        if (!intel.isEmpty()) {
            return intel;
        }
        if (isWindows()) {
            return windowsGpuUsage();
        }
        return List.of();
    }

    private List<String> nvidiaModels() {
        try {
            return regexValues(runCommand(List.of(resolveNvidiaSmi(), "-q", "-x"), Duration.ofSeconds(10)), XML_PRODUCT_NAME);
        } catch (Exception error) {
            LOGGER.log(Level.FINE, "nvidia-smi model query failed", error);
            return List.of();
        }
    }

    private List<Double> nvidiaUsage() {
        try {
            return regexDoubles(runCommand(List.of(resolveNvidiaSmi(), "-q", "-x"), Duration.ofSeconds(10)), XML_GPU_UTIL);
        } catch (Exception error) {
            LOGGER.log(Level.FINE, "nvidia-smi usage query failed", error);
            return List.of();
        }
    }

    private List<String> amdModels() {
        try {
            String output = runCommand(List.of(resolveRocmSmi(), "-u", "--showproductname", "--json"), Duration.ofSeconds(10));
            return amdBlockValues(output, AMD_CARD_SERIES);
        } catch (Exception error) {
            LOGGER.log(Level.FINE, "rocm-smi model query failed", error);
            return List.of();
        }
    }

    private List<Double> amdUsage() {
        try {
            String output = runCommand(List.of(resolveRocmSmi(), "-u", "--showproductname", "--json"), Duration.ofSeconds(10));
            return amdBlockDoubles(output, AMD_GPU_USE);
        } catch (Exception error) {
            LOGGER.log(Level.FINE, "rocm-smi usage query failed", error);
            return List.of();
        }
    }

    private List<Double> intelUsage() {
        try {
            String output = runCommand(List.of(resolveIntelGpuTop(), "-s", "1000", "-l"), Duration.ofSeconds(3));
            return parseIntelGpuTop(output).map(List::of).orElseGet(List::of);
        } catch (Exception error) {
            LOGGER.log(Level.FINE, "intel_gpu_top usage query failed", error);
            return List.of();
        }
    }

    private List<Double> windowsGpuUsage() {
        try {
            String command = "(Get-Counter '\\GPU Engine(*engtype_3D)\\Utilization Percentage' -ErrorAction Stop)"
                    + ".CounterSamples | Measure-Object -Property CookedValue -Sum | ForEach-Object { [Math]::Min(100, $_.Sum) }";
            String output = runCommand(List.of("powershell.exe", "-NoProfile", "-Command", command), Duration.ofSeconds(10));
            return firstDouble(output).map(List::of).orElseGet(List::of);
        } catch (Exception error) {
            LOGGER.log(Level.FINE, "Windows GPU counter query failed", error);
            return List.of();
        }
    }

    private String resolveNvidiaSmi() {
        return nvidiaSmiPath.updateAndGet(existing -> existing != null ? existing : findExecutable(
                "nvidia-smi",
                isWindows()
                        ? List.of("C:\\Program Files\\NVIDIA Corporation\\NVSMI\\nvidia-smi.exe")
                        : List.of("/usr/bin/nvidia-smi", "/usr/local/bin/nvidia-smi")
        ));
    }

    private String resolveRocmSmi() {
        return rocmSmiPath.updateAndGet(existing -> existing != null ? existing : findExecutable(
                "rocm-smi",
                List.of("/opt/rocm/bin/rocm-smi", "/usr/bin/rocm-smi", "/usr/local/bin/rocm-smi")
        ));
    }

    private String resolveIntelGpuTop() {
        return intelGpuTopPath.updateAndGet(existing -> existing != null ? existing : findExecutable(
                "intel_gpu_top",
                List.of("/usr/bin/intel_gpu_top", "/usr/local/bin/intel_gpu_top")
        ));
    }

    private static String findExecutable(String command, List<String> wellKnownPaths) {
        for (String candidate : wellKnownPaths) {
            if (Files.isExecutable(Path.of(candidate))) {
                return candidate;
            }
        }

        String path = System.getenv("PATH");
        if (path != null) {
            List<String> names = executableNames(command);
            for (String part : path.split(java.io.File.pathSeparator)) {
                if (part == null || part.isBlank()) {
                    continue;
                }
                for (String name : names) {
                    Path candidate = Path.of(part, name);
                    if (Files.isExecutable(candidate)) {
                        return candidate.toString();
                    }
                }
            }
        }

        return command;
    }

    private static List<String> executableNames(String command) {
        if (!isWindows()) {
            return List.of(command);
        }
        String lower = command.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".exe") || lower.endsWith(".bat") || lower.endsWith(".cmd")) {
            return List.of(command);
        }
        return List.of(command + ".exe", command + ".bat", command + ".cmd", command);
    }

    private static String runCommand(List<String> command, Duration timeout)
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        Process process = builder.start();
        CompletableFuture<byte[]> outputFuture = CompletableFuture.supplyAsync(() -> {
            try {
                return process.getInputStream().readAllBytes();
            } catch (IOException error) {
                throw new IllegalStateException(error);
            }
        });
        boolean completed = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (!completed) {
            destroyProcessTree(process);
            process.waitFor(2, TimeUnit.SECONDS);
        }
        String output = new String(outputFuture.get(2, TimeUnit.SECONDS), StandardCharsets.UTF_8);
        if (!completed) {
            throw new IOException("command timed out: " + command.getFirst());
        }
        if (process.exitValue() != 0) {
            throw new IOException("command failed: " + command.getFirst());
        }
        return output;
    }

    private static List<String> regexValues(String text, Pattern pattern) {
        List<String> values = new ArrayList<>();
        Matcher matcher = pattern.matcher(text == null ? "" : text);
        while (matcher.find()) {
            String value = matcher.group(1).trim();
            if (!value.isBlank()) {
                values.add(value);
            }
        }
        return values;
    }

    private static List<Double> regexDoubles(String text, Pattern pattern) {
        List<Double> values = new ArrayList<>();
        Matcher matcher = pattern.matcher(text == null ? "" : text);
        while (matcher.find()) {
            parseDouble(matcher.group(1)).ifPresent(values::add);
        }
        return values;
    }

    private static List<String> amdBlockValues(String text, Pattern valuePattern) {
        List<String> values = new ArrayList<>();
        Matcher block = AMD_CARD_BLOCK.matcher(text == null ? "" : text);
        while (block.find()) {
            Matcher value = valuePattern.matcher(block.group(1));
            if (value.find() && !value.group(1).isBlank()) {
                values.add(value.group(1).trim());
            }
        }
        return values;
    }

    private static List<Double> amdBlockDoubles(String text, Pattern valuePattern) {
        List<Double> values = new ArrayList<>();
        Matcher block = AMD_CARD_BLOCK.matcher(text == null ? "" : text);
        while (block.find()) {
            Matcher value = valuePattern.matcher(block.group(1));
            if (value.find()) {
                parseDouble(value.group(1)).ifPresent(values::add);
            }
        }
        return values;
    }

    private static Optional<Double> parseIntelGpuTop(String output) {
        String header = "";
        List<String> engineNames = List.of();
        int preEngineColumns = 0;
        boolean skippedFirstDataRow = false;

        for (String rawLine : (output == null ? "" : output).split("\\R")) {
            String line = rawLine.trim();
            if (line.isEmpty()) {
                continue;
            }
            if (line.startsWith("Freq")) {
                header = line;
                continue;
            }
            if (line.startsWith("req")) {
                IntelHeader parsed = parseIntelHeaders(header, line);
                engineNames = parsed.engineNames();
                preEngineColumns = parsed.preEngineColumns();
                continue;
            }
            if (!skippedFirstDataRow) {
                skippedFirstDataRow = true;
                continue;
            }
            Optional<Double> usage = parseIntelDataRow(line, engineNames, preEngineColumns);
            if (usage.isPresent()) {
                return usage;
            }
        }
        return Optional.empty();
    }

    private static IntelHeader parseIntelHeaders(String header1, String header2) {
        List<String> h1 = Arrays.asList(header1.trim().split("\\s+"));
        List<String> h2 = Arrays.asList(header2.trim().split("\\s+"));
        List<String> engines = new ArrayList<>();
        for (String column : h1) {
            String key = column.replaceAll("[0-9/]+$", "");
            if (Set.of("RCS", "BCS", "VCS", "VECS", "CCS").contains(key)) {
                engines.add(key);
            }
        }
        int preEngineColumns = engines.isEmpty() ? 0 : Math.max(h2.size() - 3 * engines.size(), 0);
        return new IntelHeader(engines, preEngineColumns);
    }

    private static Optional<Double> parseIntelDataRow(String line, List<String> engineNames, int preEngineColumns) {
        if (engineNames.isEmpty()) {
            return Optional.empty();
        }
        List<String> fields = Arrays.asList(line.trim().split("\\s+"));
        int required = preEngineColumns + 3 * engineNames.size();
        if (fields.size() < required) {
            return Optional.empty();
        }
        double max = 0;
        boolean found = false;
        for (int i = 0; i < engineNames.size(); i++) {
            int index = preEngineColumns + 3 * i;
            Optional<Double> value = parseDouble(fields.get(index));
            if (value.isPresent()) {
                max = Math.max(max, value.get());
                found = true;
            }
        }
        return found ? Optional.of(max) : Optional.empty();
    }

    private static Optional<Double> firstDouble(String text) {
        for (String token : (text == null ? "" : text).trim().split("\\s+")) {
            Optional<Double> value = parseDouble(token);
            if (value.isPresent()) {
                return value;
            }
        }
        return Optional.empty();
    }

    private static Optional<Double> parseDouble(String value) {
        try {
            return Optional.of(Double.parseDouble(value.trim()));
        } catch (RuntimeException error) {
            return Optional.empty();
        }
    }

    private static void destroyProcessTree(Process process) {
        process.descendants().forEach(ProcessHandle::destroyForcibly);
        process.destroyForcibly();
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    }

    private record IntelHeader(List<String> engineNames, int preEngineColumns) {
    }
}

public interface StreamTaskLauncher {
    void startNat(String json) throws IOException;

    void startFileManager(String json) throws IOException;

    void startTerminal(String json) throws IOException;
}

public static final class StreamTaskRunner implements StreamTaskLauncher {
    private static final Logger LOGGER = Logger.getLogger(StreamTaskRunner.class.getName());
    private static final int BUFFER_SIZE = 10 * 1024;

    private final Supplier<AgentConfig> configSupplier;
    private final NezhaServiceGrpc.NezhaServiceStub asyncStub;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService executor;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public StreamTaskRunner(
            Supplier<AgentConfig> configSupplier,
            NezhaServiceGrpc.NezhaServiceStub asyncStub,
            ScheduledExecutorService scheduler,
            ExecutorService executor) {
        this.configSupplier = configSupplier;
        this.asyncStub = asyncStub;
        this.scheduler = scheduler;
        this.executor = executor;
    }

    public void startNat(String json) throws IOException {
        AgentConfig config = configSupplier.get();
        if (config.isDisableNat()) {
            LOGGER.info("This server has disabled NAT traversal");
            return;
        }

        NatTask task = objectMapper.readValue(json, NatTask.class);
        Socket socket = new Socket();
        socket.connect(HostPort.parse(task.host()).toSocketAddress(), 10_000);
        OutputStream socketOut = socket.getOutputStream();

        IoStreamSession session = openSession(
                "NAT " + task.streamId(),
                task.streamId(),
                (stream, data) -> writeToSocket(socketOut, data, stream),
                () -> closeQuietly(socket)
        );

        executor.execute(() -> copySocketInputToStream(socket, session));
    }

    public void startFileManager(String json) throws IOException {
        AgentConfig config = configSupplier.get();
        if (config.isDisableCommandExecute()) {
            LOGGER.info("This agent has disabled command execution");
            return;
        }

        StreamIdTask task = objectMapper.readValue(json, StreamIdTask.class);
        AtomicReference<FileManagerSession> fmRef = new AtomicReference<>();
        IoStreamSession session = openSession(
                "FM " + task.streamId(),
                task.streamId(),
                (stream, data) -> {
                    FileManagerSession fm = fmRef.get();
                    if (fm != null) {
                        fm.onData(data);
                    }
                },
                () -> {
                    FileManagerSession fm = fmRef.get();
                    if (fm != null) {
                        fm.close();
                    }
                },
                false
        );
        fmRef.set(new FileManagerSession(session, executor));
        session.sendStreamId(task.streamId());
        session.startKeepAlive();
    }

    public void startTerminal(String json) throws IOException {
        AgentConfig config = configSupplier.get();
        if (config.isDisableCommandExecute()) {
            LOGGER.info("This agent has disabled command execution");
            return;
        }

        StreamIdTask task = objectMapper.readValue(json, StreamIdTask.class);
        PtyProcess process = startShell();
        OutputStream processInput = process.getOutputStream();

        IoStreamSession session = openSession(
                "Terminal " + task.streamId(),
                task.streamId(),
                (stream, data) -> handleTerminalInput(process, processInput, data, stream),
                process::destroyForcibly
        );

        executor.execute(() -> copyInputToStream(process.getInputStream(), session, "terminal"));
    }

    private IoStreamSession openSession(
            String label,
            String streamId,
            BiConsumer<IoStreamSession, IOStreamData> dataConsumer,
            Runnable closeHook) {
        return openSession(label, streamId, dataConsumer, closeHook, true);
    }

    private IoStreamSession openSession(
            String label,
            String streamId,
            BiConsumer<IoStreamSession, IOStreamData> dataConsumer,
            Runnable closeHook,
            boolean startImmediately) {
        AtomicReference<IoStreamSession> sessionRef = new AtomicReference<>();
        StreamObserver<IOStreamData> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(IOStreamData data) {
                IoStreamSession session = sessionRef.get();
                if (session != null) {
                    dataConsumer.accept(session, data);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                Status status = Status.fromThrowable(throwable);
                LOGGER.log(Level.FINE, label + " stream closed: " + status, throwable);
                IoStreamSession session = sessionRef.get();
                if (session != null) {
                    session.closeFromRemote();
                }
            }

            @Override
            public void onCompleted() {
                IoStreamSession session = sessionRef.get();
                if (session != null) {
                    session.closeFromRemote();
                }
            }
        };

        StreamObserver<IOStreamData> sink = asyncStub.iOStream(responseObserver);
        IoStreamSession session = new IoStreamSession(label, sink, scheduler, closeHook);
        sessionRef.set(session);
        if (startImmediately) {
            session.sendStreamId(streamId);
            session.startKeepAlive();
        }
        return session;
    }

    private void writeToSocket(OutputStream socketOut, IOStreamData data, IoStreamSession session) {
        try {
            byte[] payload = data.getData().toByteArray();
            socketOut.write(payload);
            socketOut.flush();
        } catch (IOException error) {
            LOGGER.log(Level.FINE, "NAT socket write failed", error);
            session.close();
        }
    }

    private void copySocketInputToStream(Socket socket, IoStreamSession session) {
        try (InputStream input = socket.getInputStream()) {
            copyInputToStream(input, session, "NAT");
        } catch (IOException error) {
            if (!session.isClosed()) {
                session.send(errorMessage(error));
            }
            session.close();
        }
    }

    private void copyInputToStream(InputStream input, IoStreamSession session, String label) {
        try (input) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int read;
            while (!session.isClosed() && (read = input.read(buffer)) >= 0) {
                if (!session.send(Arrays.copyOf(buffer, read))) {
                    return;
                }
            }
        } catch (IOException error) {
            if (!session.isClosed()) {
                session.send(errorMessage(error));
            }
            LOGGER.log(Level.FINE, label + " input copy failed", error);
        } finally {
            session.close();
        }
    }

    private void handleTerminalInput(PtyProcess process, OutputStream processInput, IOStreamData data, IoStreamSession session) {
        byte[] payload = data.getData().toByteArray();
        if (payload.length == 0) {
            return;
        }
        try {
            switch (payload[0]) {
                case 0 -> {
                    processInput.write(payload, 1, payload.length - 1);
                    processInput.flush();
                }
                case 1 -> {
                    WindowSize size = objectMapper.readValue(payloadText(payload, 1), WindowSize.class);
                    if (size.cols() > 0 && size.rows() > 0) {
                        process.setWinSize(new WinSize(size.cols(), size.rows()));
                    }
                }
                default -> {
                    // Unknown terminal control message.
                }
            }
        } catch (IOException | RuntimeException error) {
            LOGGER.log(Level.FINE, "terminal input failed", error);
            session.close();
        }
    }

    private PtyProcess startShell() throws IOException {
        String[] command;
        if (isWindows()) {
            command = commandExists("powershell.exe") ? new String[]{"powershell.exe"} : new String[]{"cmd.exe"};
        } else {
            String shell = System.getenv("SHELL");
            if (shell == null || shell.isBlank()) {
                shell = commandExists("bash") ? "bash" : "sh";
            }
            command = new String[]{shell};
        }

        Map<String, String> env = new HashMap<>(System.getenv());
        env.putIfAbsent("TERM", "xterm");
        return new PtyProcessBuilder()
                .setCommand(command)
                .setEnvironment(env)
                .setDirectory(System.getProperty("user.dir"))
                .start();
    }

    private static boolean commandExists(String command) {
        String path = System.getenv("PATH");
        if (path == null) {
            return false;
        }
        String executable = isWindows() && !command.toLowerCase(Locale.ROOT).endsWith(".exe") ? command + ".exe" : command;
        for (String part : path.split(java.io.File.pathSeparator)) {
            if (java.nio.file.Files.isExecutable(java.nio.file.Path.of(part, executable))) {
                return true;
            }
        }
        return false;
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
    }

    private static String payloadText(byte[] payload, int offset) {
        if (payload.length <= offset) {
            return "";
        }
        return new String(payload, offset, payload.length - offset, StandardCharsets.UTF_8);
    }

    private static byte[] errorMessage(Throwable error) {
        String message = error.getMessage() == null ? error.toString() : error.getMessage();
        return message.getBytes(StandardCharsets.UTF_8);
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // Closing best effort.
        }
    }

    private record StreamIdTask(@JsonProperty("StreamID") String streamId) {
    }

    private record NatTask(@JsonProperty("StreamID") String streamId, @JsonProperty("Host") String host) {
    }

    private record WindowSize(@JsonProperty("Cols") int cols, @JsonProperty("Rows") int rows) {
    }

    private record HostPort(String host, int port) {
        static HostPort parse(String value) {
            String text = value.trim();
            if (text.startsWith("[")) {
                int end = text.indexOf(']');
                if (end < 0 || end + 2 > text.length() || text.charAt(end + 1) != ':') {
                    throw new IllegalArgumentException("invalid host:port: " + value);
                }
                return new HostPort(text.substring(1, end), Integer.parseInt(text.substring(end + 2)));
            }

            int split = text.lastIndexOf(':');
            if (split <= 0 || split == text.length() - 1) {
                throw new IllegalArgumentException("invalid host:port: " + value);
            }
            return new HostPort(text.substring(0, split), Integer.parseInt(text.substring(split + 1)));
        }

        InetSocketAddress toSocketAddress() {
            return new InetSocketAddress(host, port);
        }
    }
}

static final class IoStreamSession {
    private static final Logger LOGGER = Logger.getLogger(IoStreamSession.class.getName());
    private static final byte[] STREAM_ID_PREFIX = new byte[]{(byte) 0xff, 0x05, (byte) 0xff, 0x05};

    private final String label;
    private final StreamObserver<IOStreamData> sink;
    private final ScheduledExecutorService scheduler;
    private final Runnable closeHook;
    private final AtomicBoolean closed = new AtomicBoolean();
    private ScheduledFuture<?> keepAlive;

    IoStreamSession(String label, StreamObserver<IOStreamData> sink, ScheduledExecutorService scheduler, Runnable closeHook) {
        this.label = label;
        this.sink = sink;
        this.scheduler = scheduler;
        this.closeHook = closeHook;
    }

    void sendStreamId(String streamId) {
        byte[] streamBytes = streamId.getBytes(StandardCharsets.UTF_8);
        byte[] payload = Arrays.copyOf(STREAM_ID_PREFIX, STREAM_ID_PREFIX.length + streamBytes.length);
        System.arraycopy(streamBytes, 0, payload, STREAM_ID_PREFIX.length, streamBytes.length);
        send(payload);
    }

    boolean send(byte[] data) {
        if (closed.get()) {
            return false;
        }
        synchronized (sink) {
            if (closed.get()) {
                return false;
            }
            try {
                sink.onNext(IOStreamData.newBuilder().setData(ByteString.copyFrom(data)).build());
                return true;
            } catch (RuntimeException error) {
                LOGGER.log(Level.FINE, label + " send failed", error);
                close(false);
                return false;
            }
        }
    }

    void startKeepAlive() {
        keepAlive = scheduler.scheduleAtFixedRate(() -> send(new byte[0]), 30, 30, TimeUnit.SECONDS);
    }

    void close() {
        close(true);
    }

    void closeFromRemote() {
        close(false);
    }

    boolean isClosed() {
        return closed.get();
    }

    private void close(boolean completeSink) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (keepAlive != null) {
            keepAlive.cancel(true);
        }
        try {
            closeHook.run();
        } catch (RuntimeException error) {
            LOGGER.log(Level.FINE, label + " close hook failed", error);
        }
        if (completeSink) {
            synchronized (sink) {
                try {
                    sink.onCompleted();
                } catch (RuntimeException ignored) {
                    // Stream is already closed.
                }
            }
        }
    }
}

static final class FileManagerProtocol {
    static final byte[] COMPLETE = new byte[]{0x4E, 0x5A, 0x55, 0x50}; // NZUP

    private static final byte[] FILE = new byte[]{0x4E, 0x5A, 0x54, 0x44}; // NZTD
    private static final byte[] FILE_NAME = new byte[]{0x4E, 0x5A, 0x46, 0x4E}; // NZFN
    private static final byte[] ERROR = new byte[]{0x4E, 0x45, 0x52, 0x52}; // NERR

    private FileManagerProtocol() {
    }

    static byte[] listingHeader(String path) {
        byte[] pathBytes = path.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        write(out, FILE_NAME);
        writeUint32(out, pathBytes.length);
        write(out, pathBytes);
        return out.toByteArray();
    }

    static byte[] appendFileName(byte[] bin, String name, boolean directory) {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream(bin.length + nameBytes.length + 2);
        write(out, bin);
        out.write(directory ? 1 : 0);
        out.write(nameBytes.length & 0xff);
        write(out, nameBytes);
        return out.toByteArray();
    }

    static byte[] fileHeader(long size) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(12);
        write(out, FILE);
        writeUint64(out, size);
        return out.toByteArray();
    }

    static byte[] error(Throwable error) {
        String message = error.getMessage() == null ? error.toString() : error.getMessage();
        ByteArrayOutputStream out = new ByteArrayOutputStream(4 + message.length());
        write(out, ERROR);
        write(out, message.getBytes(StandardCharsets.UTF_8));
        return out.toByteArray();
    }

    static long readUint64(byte[] bytes, int offset) {
        return ByteBuffer.wrap(bytes, offset, Long.BYTES).order(ByteOrder.BIG_ENDIAN).getLong();
    }

    private static void writeUint32(ByteArrayOutputStream out, int value) {
        write(out, ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(value).array());
    }

    private static void writeUint64(ByteArrayOutputStream out, long value) {
        write(out, ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN).putLong(value).array());
    }

    private static void write(ByteArrayOutputStream out, byte[] bytes) {
        try {
            out.write(bytes);
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}

static final class FileManagerSession {
    private static final Logger LOGGER = Logger.getLogger(FileManagerSession.class.getName());
    private static final int CHUNK_SIZE = 1024 * 1024;

    private final IoStreamSession session;
    private final ExecutorService executor;

    private OutputStream uploadStream;
    private long uploadSize;
    private long uploadReceived;
    private Path uploadPath;

    FileManagerSession(IoStreamSession session, ExecutorService executor) {
        this.session = session;
        this.executor = executor;
    }

    synchronized void onData(IOStreamData data) {
        byte[] payload = data.getData().toByteArray();
        if (payload.length == 0) {
            return;
        }
        if (uploadStream != null) {
            acceptUploadChunk(payload);
            return;
        }

        switch (payload[0]) {
            case 0 -> listDir(pathFrom(payload, 1));
            case 1 -> executor.execute(() -> download(pathFrom(payload, 1)));
            case 2 -> beginUpload(payload);
            default -> session.send(FileManagerProtocol.error(new IllegalArgumentException("unknown file manager opcode: " + payload[0])));
        }
    }

    synchronized void close() {
        resetUpload();
    }

    private void listDir(Path requested) {
        Path dir = requested;
        if (dir == null || !Files.isDirectory(dir)) {
            dir = Path.of(System.getProperty("user.home", "."));
        }

        try (DirectoryStream<Path> entries = Files.newDirectoryStream(dir)) {
            byte[] bin = FileManagerProtocol.listingHeader(dir.toString() + java.io.File.separator);
            for (Path entry : entries) {
                bin = FileManagerProtocol.appendFileName(bin, entry.getFileName().toString(), Files.isDirectory(entry));
            }
            session.send(bin);
        } catch (IOException error) {
            session.send(FileManagerProtocol.error(error));
        }
    }

    private void download(Path path) {
        if (path == null) {
            session.send(FileManagerProtocol.error(new IllegalArgumentException("path is empty")));
            return;
        }
        try {
            long size = Files.size(path);
            if (size <= 0) {
                session.send(FileManagerProtocol.error(new IllegalArgumentException("requested file is empty")));
                return;
            }
            if (!session.send(FileManagerProtocol.fileHeader(size))) {
                return;
            }

            try (var input = Files.newInputStream(path, StandardOpenOption.READ)) {
                byte[] buffer = new byte[CHUNK_SIZE];
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    if (!session.send(Arrays.copyOf(buffer, read))) {
                        return;
                    }
                }
            }
        } catch (IOException | RuntimeException error) {
            LOGGER.log(Level.FINE, "file download failed", error);
            session.send(FileManagerProtocol.error(error));
        }
    }

    private synchronized void beginUpload(byte[] payload) {
        if (payload.length < 9) {
            session.send(FileManagerProtocol.error(new IllegalArgumentException("data is invalid")));
            return;
        }
        uploadSize = FileManagerProtocol.readUint64(payload, 1);
        uploadReceived = 0;
        uploadPath = pathFrom(payload, 9);
        if (uploadPath == null) {
            session.send(FileManagerProtocol.error(new IllegalArgumentException("path is empty")));
            resetUpload();
            return;
        }
        try {
            uploadStream = Files.newOutputStream(uploadPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            if (uploadSize == 0) {
                uploadStream.close();
                resetUpload();
                session.send(FileManagerProtocol.COMPLETE);
            }
        } catch (IOException error) {
            session.send(FileManagerProtocol.error(error));
            resetUpload();
        }
    }

    private synchronized void acceptUploadChunk(byte[] payload) {
        try {
            uploadStream.write(payload);
            uploadReceived += payload.length;
            if (uploadReceived >= uploadSize) {
                uploadStream.close();
                resetUpload();
                session.send(FileManagerProtocol.COMPLETE);
            }
        } catch (IOException error) {
            session.send(FileManagerProtocol.error(error));
            resetUpload();
        }
    }

    private void resetUpload() {
        if (uploadStream != null) {
            try {
                uploadStream.close();
            } catch (IOException ignored) {
                // Closing best effort.
            }
        }
        uploadStream = null;
        uploadSize = 0;
        uploadReceived = 0;
        uploadPath = null;
    }

    private static Path pathFrom(byte[] payload, int offset) {
        if (payload.length <= offset) {
            return null;
        }
        String text = new String(payload, offset, payload.length - offset, StandardCharsets.UTF_8);
        if (text.isEmpty()) {
            return null;
        }
        return Path.of(text);
    }
}

public static final class TaskHandler {
    private static final Logger LOGGER = Logger.getLogger(TaskHandler.class.getName());
    private static final int TASK_TYPE_HTTP_GET = 1;
    private static final int TASK_TYPE_ICMP_PING = 2;
    private static final int TASK_TYPE_TCP_PING = 3;
    private static final int TASK_TYPE_COMMAND = 4;
    private static final int TASK_TYPE_UPGRADE = 6;
    private static final int TASK_TYPE_KEEPALIVE = 7;
    private static final int TASK_TYPE_TERMINAL_GRPC = 8;
    private static final int TASK_TYPE_NAT = 9;
    private static final int TASK_TYPE_FM = 11;
    private static final int TASK_TYPE_REPORT_CONFIG = 12;
    private static final int TASK_TYPE_APPLY_CONFIG = 13;

    private final Supplier<AgentConfig> configSupplier;
    private final Path configPath;
    private final Consumer<AgentConfig> configConsumer;
    private final StreamTaskLauncher streamTaskLauncher;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TaskHandler(
            Supplier<AgentConfig> configSupplier,
            Path configPath,
            Consumer<AgentConfig> configConsumer,
            StreamTaskLauncher streamTaskLauncher) {
        this.configSupplier = configSupplier;
        this.configPath = configPath;
        this.configConsumer = configConsumer;
        this.streamTaskLauncher = streamTaskLauncher;
    }

    public Optional<TaskResult> handle(Task task) {
        TaskResult.Builder result = TaskResult.newBuilder()
                .setId(task.getId())
                .setType(task.getType());

        try {
            switch ((int) task.getType()) {
                case TASK_TYPE_HTTP_GET -> handleHttpGet(task, result);
                case TASK_TYPE_ICMP_PING -> handleIcmpPing(task, result);
                case TASK_TYPE_TCP_PING -> handleTcpPing(task, result);
                case TASK_TYPE_COMMAND -> handleCommand(task, result);
                case TASK_TYPE_UPGRADE -> handleUpgrade(result);
                case TASK_TYPE_KEEPALIVE -> {
                    // Official agent returns the default TaskResult for keepalive.
                }
                case TASK_TYPE_TERMINAL_GRPC -> {
                    streamTaskLauncher.startTerminal(task.getData());
                    return Optional.empty();
                }
                case TASK_TYPE_NAT -> {
                    streamTaskLauncher.startNat(task.getData());
                    return Optional.empty();
                }
                case TASK_TYPE_FM -> {
                    streamTaskLauncher.startFileManager(task.getData());
                    return Optional.empty();
                }
                case TASK_TYPE_REPORT_CONFIG -> handleReportConfig(result);
                case TASK_TYPE_APPLY_CONFIG -> {
                    handleApplyConfig(task);
                    return Optional.empty();
                }
                default -> {
                    LOGGER.info(() -> "Unsupported task type: " + task.getType());
                    return Optional.empty();
                }
            }
        } catch (Exception error) {
            if (returnsNoResult(task.getType())) {
                LOGGER.log(Level.FINE, "Task failed without result: " + task.getType(), error);
                return Optional.empty();
            }
            LOGGER.log(Level.WARNING, "Task failed: " + task.getType(), error);
            result.setData(error.getMessage() == null ? error.toString() : error.getMessage());
        }

        return Optional.of(result.build());
    }

    private static boolean returnsNoResult(long taskType) {
        return taskType == TASK_TYPE_TERMINAL_GRPC
                || taskType == TASK_TYPE_NAT
                || taskType == TASK_TYPE_FM
                || taskType == TASK_TYPE_APPLY_CONFIG;
    }

    private void handleHttpGet(Task task, TaskResult.Builder result) throws IOException {
        if (configSupplier.get().isDisableSendQuery()) {
            result.setData("This server has disabled query sending");
            return;
        }

        long started = System.nanoTime();
        HttpURLConnection connection = openHttpGet(task.getData());
        int statusCode;
        try (InputStream input = responseStream(connection)) {
            input.transferTo(OutputStream.nullOutputStream());
            statusCode = connection.getResponseCode();
        } finally {
            connection.disconnect();
        }
        result.setDelay(elapsedMillis(started));

        if (statusCode < 200 || statusCode > 399) {
            result.setData("HTTP error: " + statusCode + " " + connection.getResponseMessage());
            return;
        }
        tlsPeerCertificate(connection).ifPresent(certificate ->
                result.setData(issuerCommonName(certificate) + "|" + certificate.getNotAfter()));
        result.setSuccessful(true);
    }

    private void handleIcmpPing(Task task, TaskResult.Builder result) throws IOException {
        if (configSupplier.get().isDisableSendQuery()) {
            result.setData("This server has disabled query sending");
            return;
        }

        InetAddress address = resolveAddress(task.getData(), configSupplier.get().getDns());
        long totalNanos = 0;
        int received = 0;
        for (int i = 0; i < 5; i++) {
            long started = System.nanoTime();
            if (address.isReachable(4000)) {
                totalNanos += System.nanoTime() - started;
                received++;
            }
        }
        if (received == 0) {
            result.setData("packets recv 0");
            return;
        }
        result.setDelay((float) (totalNanos / 1_000_000.0 / received));
        result.setSuccessful(true);
    }

    private void handleTcpPing(Task task, TaskResult.Builder result) throws IOException {
        if (configSupplier.get().isDisableSendQuery()) {
            result.setData("This server has disabled query sending");
            return;
        }

        AgentConfig config = configSupplier.get();
        HostPort hostPort = HostPort.parse(task.getData());
        InetAddress address = resolveAddress(hostPort.host(), config.getDns());
        long started = System.nanoTime();
        try (java.net.Socket socket = new java.net.Socket()) {
            socket.connect(new InetSocketAddress(address, hostPort.port()), 10_000);
        }
        result.setDelay(elapsedMillis(started));
        result.setSuccessful(true);
    }

    private void handleCommand(Task task, TaskResult.Builder result)
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        if (configSupplier.get().isDisableCommandExecute()) {
            result.setData("This agent has disabled command execution");
            return;
        }

        long started = System.nanoTime();
        ProcessBuilder builder = commandBuilder(task.getData());
        builder.redirectErrorStream(true);
        Process process = builder.start();
        CompletableFuture<byte[]> outputFuture = CompletableFuture.supplyAsync(() -> {
            try {
                return process.getInputStream().readAllBytes();
            } catch (IOException error) {
                throw new IllegalStateException(error);
            }
        });
        boolean completed = process.waitFor(2, TimeUnit.HOURS);
        if (!completed) {
            destroyProcessTree(process);
            process.waitFor(30, TimeUnit.SECONDS);
        }
        byte[] output = outputFuture.get(30, TimeUnit.SECONDS);
        result.setDelay((float) ((System.nanoTime() - started) / 1_000_000_000.0));

        String data = new String(output, StandardCharsets.UTF_8);
        if (!completed) {
            result.setData("task execution timed out\n" + data);
            return;
        }
        if (process.exitValue() != 0) {
            result.setData(data + "\nexit code: " + process.exitValue());
            return;
        }
        result.setData(data);
        result.setSuccessful(true);
    }

    private void handleUpgrade(TaskResult.Builder result) {
        if (configSupplier.get().isDisableForceUpdate()) {
            return;
        }
        result.setData("Self-update is intentionally not implemented in the embeddable Java agent");
    }

    private void handleReportConfig(TaskResult.Builder result) throws IOException {
        if (configSupplier.get().isDisableCommandExecute()) {
            result.setData("This agent has disabled command execution");
            return;
        }

        result.setData(objectMapper.writeValueAsString(configSupplier.get().toMap()));
        result.setSuccessful(true);
    }

    private void handleApplyConfig(Task task) throws IOException {
        if (configSupplier.get().isDisableCommandExecute()) {
            return;
        }

        Map<String, Object> overlay = objectMapper.readValue(task.getData(), new TypeReference<>() {
        });
        AgentConfig next = configSupplier.get().copy();
        next.applyRemoteOverlay(overlay);
        next.save();
        configConsumer.accept(next);
    }

    private static ProcessBuilder commandBuilder(String command) {
        if (System.getProperty("os.name", "").toLowerCase().contains("win")) {
            return new ProcessBuilder("cmd", "/c", command);
        }
        return new ProcessBuilder("sh", "-c", command);
    }

    private static void destroyProcessTree(Process process) {
        process.descendants().forEach(ProcessHandle::destroyForcibly);
        process.destroyForcibly();
    }

    private static HttpURLConnection openHttpGet(String url) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) URI.create(url).toURL().openConnection();
        connection.setConnectTimeout(10_000);
        connection.setReadTimeout(30_000);
        connection.setInstanceFollowRedirects(false);
        connection.setRequestMethod("GET");
        connection.setRequestProperty("User-Agent", "nezha-agent/1.0");
        return connection;
    }

    private static float elapsedMillis(long startedNanos) {
        return (float) ((System.nanoTime() - startedNanos) / 1_000_000.0);
    }

    private static Optional<X509Certificate> tlsPeerCertificate(HttpURLConnection connection) {
        if (connection instanceof HttpsURLConnection httpsConnection) {
            try {
                Certificate[] certificates = httpsConnection.getServerCertificates();
                if (certificates.length > 0 && certificates[0] instanceof X509Certificate certificate) {
                    return Optional.of(certificate);
                }
            } catch (SSLPeerUnverifiedException ignored) {
                // Plain HTTP or an unverified TLS session has no peer certificate to report.
            }
        }
        return Optional.empty();
    }

    private static String issuerCommonName(X509Certificate certificate) {
        String name = certificate.getIssuerX500Principal().getName();
        try {
            for (Rdn rdn : new LdapName(name).getRdns()) {
                if ("CN".equalsIgnoreCase(rdn.getType())) {
                    return String.valueOf(rdn.getValue());
                }
            }
        } catch (InvalidNameException ignored) {
            // Fall back to the full issuer DN below.
        }
        return name;
    }

    private static InetAddress resolveAddress(String hostOrIp, List<String> dnsServers) throws IOException {
        try {
            return InetAddress.getByName(hostOrIp);
        } catch (IOException firstError) {
            if (dnsServers == null || dnsServers.isEmpty()) {
                throw firstError;
            }
            IOException lastError = firstError;
            for (String dnsServer : dnsServers) {
                try {
                    Optional<InetAddress> address = queryDnsServer(hostOrIp, dnsServer, "A");
                    if (address.isPresent()) {
                        return address.get();
                    }
                    address = queryDnsServer(hostOrIp, dnsServer, "AAAA");
                    if (address.isPresent()) {
                        return address.get();
                    }
                } catch (NamingException | RuntimeException error) {
                    lastError = new IOException(error);
                }
            }
            throw lastError;
        }
    }

    private static Optional<InetAddress> queryDnsServer(String host, String dnsServer, String recordType)
            throws NamingException, IOException {
        Hashtable<String, String> environment = new Hashtable<>();
        environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
        environment.put(Context.PROVIDER_URL, "dns://" + stripDnsPort(dnsServer) + "/");

        DirContext context = new InitialDirContext(environment);
        try {
            Attributes attributes = context.getAttributes(host, new String[]{recordType});
            Attribute records = attributes.get(recordType);
            if (records == null) {
                return Optional.empty();
            }
            NamingEnumeration<?> values = records.getAll();
            while (values.hasMore()) {
                Object value = values.next();
                if (value != null) {
                    return Optional.of(InetAddress.getByName(value.toString()));
                }
            }
            return Optional.empty();
        } finally {
            context.close();
        }
    }

    private static String stripDnsPort(String dnsServer) {
        String text = dnsServer.trim();
        if (text.startsWith("[")) {
            int end = text.indexOf(']');
            if (end > 0) {
                return text.substring(1, end);
            }
        }
        int split = text.lastIndexOf(':');
        if (split > 0 && text.indexOf(':') == split) {
            return text.substring(0, split);
        }
        return text;
    }

    private record HostPort(String host, int port) {
        private static HostPort parse(String value) {
            String text = value.trim();
            if (text.startsWith("[")) {
                int end = text.indexOf(']');
                if (end < 0 || end + 2 > text.length() || text.charAt(end + 1) != ':') {
                    throw new IllegalArgumentException("invalid host:port: " + value);
                }
                return new HostPort(text.substring(1, end), Integer.parseInt(text.substring(end + 2)));
            }

            int split = text.lastIndexOf(':');
            if (split <= 0 || split == text.length() - 1) {
                throw new IllegalArgumentException("invalid host:port: " + value);
            }
            return new HostPort(text.substring(0, split), Integer.parseInt(text.substring(split + 1)));
        }
    }
}


}

