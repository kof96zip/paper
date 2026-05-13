package com.nezhahq.agent;

import com.nezhahq.agent.proto.GeoIP;
import com.nezhahq.agent.proto.Host;
import com.nezhahq.agent.proto.IOStreamData;
import com.nezhahq.agent.proto.NezhaServiceGrpc;
import com.nezhahq.agent.proto.Receipt;
import com.nezhahq.agent.proto.State;
import com.nezhahq.agent.proto.Task;
import com.nezhahq.agent.proto.TaskResult;
import com.nezhahq.agent.proto.Uint64Receipt;
import com.sun.net.httpserver.HttpServer;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientLifecycleTest {
    private static final Metadata.Key<String> CLIENT_SECRET =
            Metadata.Key.of("client_secret", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> CLIENT_UUID =
            Metadata.Key.of("client_uuid", Metadata.ASCII_STRING_MARSHALLER);

    private final Path tempDir = Path.of("target", "unit-temp", "lifecycle").toAbsolutePath();

    @Test
    void clientConnectsReportsAndStopsAgainstLocalGrpcDashboard() throws Exception {
        Files.createDirectories(tempDir);
        TestDashboard dashboard = new TestDashboard();
        CapturingAuthInterceptor auth = new CapturingAuthInterceptor();
        Server grpcServer = startDashboard(dashboard, auth);
        HttpServer ipServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        ipServer.createContext("/trace", exchange -> {
            byte[] body = "ip=127.0.0.1\nloc=ZZ\n".getBytes();
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        ipServer.start();

        Path configPath = tempDir.resolve("config.yml");
        Files.writeString(configPath, """
                server: "127.0.0.1:%d"
                client_secret: "secret"
                uuid: "11111111-1111-1111-1111-111111111111"
                report_delay: 1
                ip_report_period: 30
                custom_ip_api:
                  - "http://127.0.0.1:%d/trace"
                """.formatted(grpcServer.getPort(), ipServer.getAddress().getPort()));

        NezhaJavaAgent.RunningAgent agent = NezhaJavaAgent.start(configPath);
        try {
            assertTrue(dashboard.hostReported.await(10, TimeUnit.SECONDS));
            assertTrue(dashboard.stateReported.await(10, TimeUnit.SECONDS));
            assertTrue(dashboard.geoIpReported.await(10, TimeUnit.SECONDS));
            assertTrue(dashboard.taskResultReported.await(10, TimeUnit.SECONDS));
            assertEquals("secret", auth.clientSecret.get());
            assertEquals("11111111-1111-1111-1111-111111111111", auth.clientUuid.get());
        } finally {
            agent.close();
            assertCompletes(agent.completion());
            grpcServer.shutdownNow();
            ipServer.stop(0);
        }
    }

    private static void assertCompletes(java.util.concurrent.CompletableFuture<Void> completion) throws Exception {
        completion.get(Duration.ofSeconds(5).toMillis(), TimeUnit.MILLISECONDS);
    }

    private static Server startDashboard(TestDashboard dashboard, CapturingAuthInterceptor auth) {
        try {
            return NettyServerBuilder
                    .forAddress(new InetSocketAddress("127.0.0.1", 0))
                    .addService(ServerInterceptors.intercept(dashboard, auth))
                    .build()
                    .start();
        } catch (Exception | LinkageError error) {
            throw new TestAbortedException("local Netty loopback is unavailable in this test environment", error);
        }
    }

    private static final class TestDashboard extends NezhaServiceGrpc.NezhaServiceImplBase {
        private final CountDownLatch hostReported = new CountDownLatch(1);
        private final CountDownLatch stateReported = new CountDownLatch(1);
        private final CountDownLatch geoIpReported = new CountDownLatch(1);
        private final CountDownLatch taskResultReported = new CountDownLatch(1);

        @Override
        public void reportSystemInfo2(Host request, StreamObserver<Uint64Receipt> responseObserver) {
            hostReported.countDown();
            responseObserver.onNext(Uint64Receipt.newBuilder().setData(1).build());
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<State> reportSystemState(StreamObserver<Receipt> responseObserver) {
            return new StreamObserver<>() {
                @Override
                public void onNext(State value) {
                    stateReported.countDown();
                    responseObserver.onNext(Receipt.newBuilder().setProced(true).build());
                }

                @Override
                public void onError(Throwable throwable) {
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public StreamObserver<TaskResult> requestTask(StreamObserver<Task> responseObserver) {
            responseObserver.onNext(Task.newBuilder().setId(99).setType(7).build());
            return new StreamObserver<>() {
                @Override
                public void onNext(TaskResult value) {
                    if (value.getId() == 99 && !value.getSuccessful()) {
                        taskResultReported.countDown();
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public StreamObserver<IOStreamData> iOStream(StreamObserver<IOStreamData> responseObserver) {
            return new StreamObserver<>() {
                @Override
                public void onNext(IOStreamData value) {
                }

                @Override
                public void onError(Throwable throwable) {
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public void reportGeoIP(GeoIP request, StreamObserver<GeoIP> responseObserver) {
            geoIpReported.countDown();
            responseObserver.onNext(GeoIP.newBuilder(request)
                    .setCountryCode("ZZ")
                    .setDashboardBootTime(1)
                    .build());
            responseObserver.onCompleted();
        }
    }

    private static final class CapturingAuthInterceptor implements ServerInterceptor {
        private final AtomicReference<String> clientSecret = new AtomicReference<>();
        private final AtomicReference<String> clientUuid = new AtomicReference<>();

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
            clientSecret.compareAndSet(null, headers.get(CLIENT_SECRET));
            clientUuid.compareAndSet(null, headers.get(CLIENT_UUID));
            return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            }, headers);
        }
    }
}
