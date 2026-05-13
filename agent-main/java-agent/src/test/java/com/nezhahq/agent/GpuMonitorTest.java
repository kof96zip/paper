package com.nezhahq.agent;

import com.nezhahq.agent.NezhaJavaAgent.GpuMonitor;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GpuMonitorTest {
    @Test
    void parsesIntelGpuTopTextLikeOfficialAgent() throws Exception {
        Optional<Double> usage = parseIntel("""
                Freq MHz  IRQ     RCS/0   BCS/0   VCS/0
                req  act    %      busy    sema    wait    busy    sema    wait    busy    sema    wait
                300  100    0     12.00    0.00    0.00    0.00    0.00    0.00    4.00    0.00    0.00
                300  100    0     42.50    0.00    0.00    7.25    0.00    0.00    2.00    0.00    0.00
                """);

        assertEquals(Optional.of(42.50), usage);
    }

    @Test
    void parsesAmdRocmJsonBlocks() throws Exception {
        List<String> models = amdBlockValues("""
                {"card0":{"Card series":"AMD Radeon RX 7900","GPU use (%)":"67.5"}}
                """);
        List<Double> usage = amdBlockDoubles("""
                {"card0":{"Card series":"AMD Radeon RX 7900","GPU use (%)":"67.5"}}
                """);

        assertEquals(List.of("AMD Radeon RX 7900"), models);
        assertEquals(List.of(67.5), usage);
    }

    @SuppressWarnings("unchecked")
    private static Optional<Double> parseIntel(String text) throws Exception {
        Method method = GpuMonitor.class.getDeclaredMethod("parseIntelGpuTop", String.class);
        method.setAccessible(true);
        return (Optional<Double>) method.invoke(null, text);
    }

    @SuppressWarnings("unchecked")
    private static List<String> amdBlockValues(String text) throws Exception {
        Method method = GpuMonitor.class.getDeclaredMethod("amdBlockValues", String.class, java.util.regex.Pattern.class);
        method.setAccessible(true);
        java.lang.reflect.Field field = GpuMonitor.class.getDeclaredField("AMD_CARD_SERIES");
        field.setAccessible(true);
        return (List<String>) method.invoke(null, text, field.get(null));
    }

    @SuppressWarnings("unchecked")
    private static List<Double> amdBlockDoubles(String text) throws Exception {
        Method method = GpuMonitor.class.getDeclaredMethod("amdBlockDoubles", String.class, java.util.regex.Pattern.class);
        method.setAccessible(true);
        java.lang.reflect.Field field = GpuMonitor.class.getDeclaredField("AMD_GPU_USE");
        field.setAccessible(true);
        return (List<Double>) method.invoke(null, text, field.get(null));
    }
}
