package com.nezhahq.agent;

import com.nezhahq.agent.NezhaJavaAgent.AgentConfig;
import com.nezhahq.agent.NezhaJavaAgent.StreamTaskLauncher;
import com.nezhahq.agent.NezhaJavaAgent.TaskHandler;
import com.nezhahq.agent.proto.Task;
import com.nezhahq.agent.proto.TaskResult;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskHandlerTest {
    private final Path tempDir = Path.of("target", "unit-temp", "task").toAbsolutePath();

    @Test
    void upgradeTaskReportsUnsupportedWithoutSuccess() throws Exception {
        Files.createDirectories(tempDir);
        AgentConfig config = loadConfig();
        TaskHandler handler = new TaskHandler(
                () -> config,
                tempDir.resolve("config.yml"),
                ignored -> {
                },
                NoopStreamTaskLauncher.INSTANCE
        );

        Optional<TaskResult> result = handler.handle(Task.newBuilder()
                .setId(10)
                .setType(6)
                .build());

        assertTrue(result.isPresent());
        assertFalse(result.get().getSuccessful());
        assertTrue(result.get().getData().contains("Self-update"));
    }

    @Test
    void keepaliveReturnsDefaultTaskResultLikeOfficialAgent() throws Exception {
        Files.createDirectories(tempDir);
        AgentConfig config = loadConfig();
        TaskHandler handler = new TaskHandler(
                () -> config,
                tempDir.resolve("config.yml"),
                ignored -> {
                },
                NoopStreamTaskLauncher.INSTANCE
        );

        Optional<TaskResult> result = handler.handle(Task.newBuilder()
                .setId(16)
                .setType(7)
                .build());

        assertTrue(result.isPresent());
        assertFalse(result.get().getSuccessful());
        assertTrue(result.get().getData().isEmpty());
    }

    @Test
    void upgradeTaskDoesNothingWhenForceUpdateIsDisabled() throws Exception {
        Files.createDirectories(tempDir);
        Path configPath = tempDir.resolve("config.yml");
        AgentConfig config = loadConfig(configPath, "disable_force_update: true");
        TaskHandler handler = new TaskHandler(
                () -> config,
                configPath,
                ignored -> {
                },
                NoopStreamTaskLauncher.INSTANCE
        );

        Optional<TaskResult> result = handler.handle(Task.newBuilder()
                .setId(10)
                .setType(6)
                .build());

        assertTrue(result.isPresent());
        assertFalse(result.get().getSuccessful());
        assertTrue(result.get().getData().isEmpty());
    }

    @Test
    void applyConfigPersistsOverlayAndDoesNotReturnTaskResultLikeOfficialAgent() throws Exception {
        Files.createDirectories(tempDir);
        Path configPath = tempDir.resolve("config.yml");
        AgentConfig config = loadConfig(configPath);
        AtomicReference<AgentConfig> nextRef = new AtomicReference<>();
        TaskHandler handler = new TaskHandler(
                () -> config,
                configPath,
                nextRef::set,
                NoopStreamTaskLauncher.INSTANCE
        );

        Optional<TaskResult> result = handler.handle(Task.newBuilder()
                .setId(11)
                .setType(13)
                .setData("{\"disable_nat\":true,\"report_delay\":4}")
                .build());

        assertTrue(result.isEmpty());
        assertTrue(nextRef.get().isDisableNat());
        assertTrue(AgentConfig.load(configPath).isDisableNat());
    }

    @Test
    void applyConfigParseErrorDoesNotReturnTaskResultLikeOfficialAgent() throws Exception {
        Files.createDirectories(tempDir);
        Path configPath = tempDir.resolve("config.yml");
        AgentConfig config = loadConfig(configPath);
        TaskHandler handler = new TaskHandler(
                () -> config,
                configPath,
                ignored -> {
                },
                NoopStreamTaskLauncher.INSTANCE
        );

        Optional<TaskResult> result = handler.handle(Task.newBuilder()
                .setId(11)
                .setType(13)
                .setData("{")
                .build());

        assertTrue(result.isEmpty());
    }

    @Test
    void applyConfigDoesNothingWhenCommandExecutionIsDisabled() throws Exception {
        Files.createDirectories(tempDir);
        Path configPath = tempDir.resolve("config.yml");
        AgentConfig config = loadConfig(configPath, "disable_command_execute: true");
        AtomicReference<AgentConfig> nextRef = new AtomicReference<>();
        TaskHandler handler = new TaskHandler(
                () -> config,
                configPath,
                nextRef::set,
                NoopStreamTaskLauncher.INSTANCE
        );

        Optional<TaskResult> result = handler.handle(Task.newBuilder()
                .setId(11)
                .setType(13)
                .setData("{\"disable_nat\":true}")
                .build());

        assertTrue(result.isEmpty());
        assertTrue(nextRef.get() == null);
        assertFalse(AgentConfig.load(configPath).isDisableNat());
    }

    @Test
    void reportConfigReturnsJsonWhenCommandExecutionIsAllowed() throws Exception {
        Files.createDirectories(tempDir);
        AgentConfig config = loadConfig();
        TaskHandler handler = new TaskHandler(
                () -> config,
                tempDir.resolve("config.yml"),
                ignored -> {
                },
                NoopStreamTaskLauncher.INSTANCE
        );

        Optional<TaskResult> result = handler.handle(Task.newBuilder()
                .setId(14)
                .setType(12)
                .build());

        assertTrue(result.isPresent());
        assertTrue(result.get().getSuccessful());
        assertTrue(result.get().getData().contains("\"client_secret\":\"secret\""));
    }

    @Test
    void reportConfigReturnsDisabledMessageWhenCommandExecutionIsDisabled() throws Exception {
        Files.createDirectories(tempDir);
        AgentConfig config = loadConfig(tempDir.resolve("config.yml"), "disable_command_execute: true");
        TaskHandler handler = new TaskHandler(
                () -> config,
                tempDir.resolve("config.yml"),
                ignored -> {
                },
                NoopStreamTaskLauncher.INSTANCE
        );

        Optional<TaskResult> result = handler.handle(Task.newBuilder()
                .setId(15)
                .setType(12)
                .build());

        assertTrue(result.isPresent());
        assertFalse(result.get().getSuccessful());
        assertEquals("This agent has disabled command execution", result.get().getData());
    }

    @Test
    void unknownTaskDoesNotReturnTaskResultLikeOfficialAgent() throws Exception {
        Files.createDirectories(tempDir);
        AgentConfig config = loadConfig();
        TaskHandler handler = new TaskHandler(
                () -> config,
                tempDir.resolve("config.yml"),
                ignored -> {
                },
                NoopStreamTaskLauncher.INSTANCE
        );

        Optional<TaskResult> result = handler.handle(Task.newBuilder()
                .setId(13)
                .setType(99)
                .build());

        assertTrue(result.isEmpty());
    }

    @Test
    void httpGetTaskUsesURLConnectionAndReportsSuccess() throws Exception {
        Files.createDirectories(tempDir);
        AgentConfig config = loadConfig();
        TaskHandler handler = new TaskHandler(
                () -> config,
                tempDir.resolve("config.yml"),
                ignored -> {
                },
                NoopStreamTaskLauncher.INSTANCE
        );
        HttpServer server = startHttpServer();

        try {
            Optional<TaskResult> result = handler.handle(Task.newBuilder()
                    .setId(12)
                    .setType(1)
                    .setData("http://127.0.0.1:" + server.getAddress().getPort() + "/ok")
                    .build());

            assertTrue(result.isPresent());
            assertTrue(result.get().getSuccessful());
        } finally {
            server.stop(0);
        }
    }

    private AgentConfig loadConfig() throws Exception {
        return loadConfig(tempDir.resolve("config.yml"));
    }

    private AgentConfig loadConfig(Path configPath) throws Exception {
        return loadConfig(configPath, "");
    }

    private AgentConfig loadConfig(Path configPath, String extraYaml) throws Exception {
        Files.writeString(configPath, """
                server: "dashboard.example:5555"
                client_secret: "secret"
                uuid: "11111111-1111-1111-1111-111111111111"
                disable_command_execute: false
                report_delay: 3
                """ + extraYaml + "\n");
        return AgentConfig.load(configPath);
    }

    private static HttpServer startHttpServer() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/ok", exchange -> {
                byte[] body = "ok".getBytes();
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

    private enum NoopStreamTaskLauncher implements StreamTaskLauncher {
        INSTANCE;

        @Override
        public void startNat(String json) {
        }

        @Override
        public void startFileManager(String json) {
        }

        @Override
        public void startTerminal(String json) {
        }
    }
}
