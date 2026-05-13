package com.nezhahq.agent;

import com.nezhahq.agent.NezhaJavaAgent.AgentConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AgentConfigTest {
    private final Path tempDir = Path.of("target", "unit-temp", "config").toAbsolutePath();

    @Test
    void parseConfigPathUsesExplicitFlagOrExecutableDirectoryDefault() {
        Path explicit = NezhaJavaAgent.parseConfigPath(new String[]{"--config", "custom.yml"});
        Path equalsForm = NezhaJavaAgent.parseConfigPath(new String[]{"--config=custom2.yml"});
        Path defaultPath = NezhaJavaAgent.parseConfigPath(new String[0]);

        assertEquals(Path.of("custom.yml").toAbsolutePath().normalize(), explicit);
        assertEquals(Path.of("custom2.yml").toAbsolutePath().normalize(), equalsForm);
        assertEquals("config.yml", defaultPath.getFileName().toString());
        assertTrue(defaultPath.isAbsolute());
    }

    @Test
    void loadAppliesDefaultsAndKeepsYamlValues() throws Exception {
        Files.createDirectories(tempDir);
        Path configPath = tempDir.resolve("config.yml");
        Files.writeString(configPath, """
                server: "dashboard.example:5555"
                client_secret: "secret"
                uuid: "11111111-1111-1111-1111-111111111111"
                report_delay: 0
                ip_report_period: 5
                disable_nat: true
                dns: "1.1.1.1:53, 8.8.8.8:53"
                nic_allowlist:
                  eth0: true
                  wlan0: false
                """);

        AgentConfig config = NezhaJavaAgent.loadConfig(configPath);

        assertEquals("dashboard.example:5555", config.getServer());
        assertEquals("secret", config.getClientSecret());
        assertEquals("11111111-1111-1111-1111-111111111111", config.getUuid());
        assertEquals(3, config.getReportDelay());
        assertEquals(30, config.getIpReportPeriod());
        assertTrue(config.isDisableNat());
        assertEquals(List.of("1.1.1.1:53", "8.8.8.8:53"), config.getDns());
        assertEquals(Map.of("eth0", true, "wlan0", false), config.getNicAllowlist());
    }

    @Test
    void remoteOverlayUpdatesAndPersistsCompatibleKeys() throws Exception {
        Files.createDirectories(tempDir);
        Path configPath = tempDir.resolve("config.yml");
        Files.writeString(configPath, """
                server: "dashboard.example:5555"
                client_secret: "secret"
                uuid: "11111111-1111-1111-1111-111111111111"
                report_delay: 3
                """);
        AgentConfig config = AgentConfig.load(configPath);

        config.applyRemoteOverlay(Map.of(
                "disable_command_execute", true,
                "custom_ip_api", List.of("https://example.com/ip"),
                "report_delay", 4
        ));
        config.save();
        AgentConfig reloaded = AgentConfig.load(configPath);

        assertTrue(reloaded.isDisableCommandExecute());
        assertEquals(List.of("https://example.com/ip"), reloaded.getCustomIpApi());
        assertEquals(4, reloaded.getReportDelay());
        assertFalse(reloaded.isDisableNat());
    }

    @Test
    void createClientUsesNormalizedPathAndLoadConfigEntryPoint() throws Exception {
        Files.createDirectories(tempDir);
        Path configPath = tempDir.resolve("nested").resolve("..").resolve("entrypoint.yml");
        Files.writeString(configPath.normalize(), """
                server: "dashboard.example:5555"
                client_secret: "secret"
                uuid: "11111111-1111-1111-1111-111111111111"
                """);

        AgentConfig config = NezhaJavaAgent.loadConfig(configPath);
        NezhaJavaAgent.NezhaAgentClient client = NezhaJavaAgent.createClient(configPath);

        assertEquals("dashboard.example:5555", config.getServer());
        assertTrue(client != null);
        client.stop();
    }

    @Test
    void configSupportsHyphenatedKeysFromExternalFiles() throws Exception {
        Files.createDirectories(tempDir);
        Path configPath = tempDir.resolve("hyphenated.yml");
        Files.writeString(configPath, """
                server: "dashboard.example:5555"
                client-secret: "secret"
                uuid: "11111111-1111-1111-1111-111111111111"
                disable-nat: true
                report-delay: 4
                """);

        AgentConfig config = NezhaJavaAgent.loadConfig(configPath);

        assertEquals("secret", config.getClientSecret());
        assertEquals(4, config.getReportDelay());
        assertTrue(config.isDisableNat());
    }
}
