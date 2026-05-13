package com.nezhahq.agent;

import com.nezhahq.agent.NezhaJavaAgent.GeoIpService;
import com.nezhahq.agent.proto.GeoIP;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeoIpServiceTest {
    @Test
    void returnsOfficialStyleFallbackAfterRepeatedLookupFailures() throws Exception {
        GeoIpService service = new GeoIpService();
        List<String> invalidEndpoints = List.of("not-a-uri");

        service.fetch(false, invalidEndpoints);
        Thread.sleep(2);
        service.fetch(false, invalidEndpoints);
        Thread.sleep(2);
        service.fetch(false, invalidEndpoints);

        Optional<GeoIP> fallback = service.fetch(false, invalidEndpoints);

        assertTrue(fallback.isPresent());
        assertEquals("", fallback.get().getIp().getIpv4());
        assertEquals("", fallback.get().getIp().getIpv6());
    }

    @Test
    void tracksIpChangesAndForcedReportsLikeOfficialAgent() throws Exception {
        HttpServer server = startIpServer("203.0.113.10");
        GeoIpService service = new GeoIpService();

        try {
            Optional<GeoIP> geoIp = service.fetch(false, List.of("http://127.0.0.1:" + server.getAddress().getPort() + "/trace"));

            assertTrue(geoIp.isPresent());
            assertTrue(service.shouldReport());

            service.markReported(GeoIP.newBuilder().setCountryCode("ZZ").build());
            assertEquals("ZZ", service.getCachedCountryCode());
            service.fetch(false, List.of("http://127.0.0.1:" + server.getAddress().getPort() + "/trace"));
            assertTrue(!service.shouldReport());

            service.forceNextReport();
            assertTrue(service.shouldReport());
        } finally {
            server.stop(0);
        }
    }

    private static HttpServer startIpServer(String ip) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/trace", exchange -> {
                byte[] body = ("ip=" + ip + "\n").getBytes();
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.start();
            return server;
        } catch (Exception error) {
            throw new TestAbortedException("local HTTP loopback is unavailable in this test environment", error);
        }
    }
}
