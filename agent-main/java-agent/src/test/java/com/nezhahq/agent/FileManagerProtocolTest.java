package com.nezhahq.agent;

import com.nezhahq.agent.NezhaJavaAgent.FileManagerProtocol;
import com.nezhahq.agent.NezhaJavaAgent.IoStreamSession;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileManagerProtocolTest {
    @Test
    void listingHeaderUsesGoCompatibleWireFormat() {
        byte[] header = FileManagerProtocol.listingHeader("/tmp/");

        assertArrayEquals(new byte[]{0x4e, 0x5a, 0x46, 0x4e}, slice(header, 0, 4));
        assertArrayEquals(new byte[]{0, 0, 0, 5}, slice(header, 4, 8));
        assertEquals("/tmp/", new String(header, 8, header.length - 8, StandardCharsets.UTF_8));
    }

    @Test
    void appendsDirectoryEntryWithOneByteTypeAndLength() {
        byte[] payload = FileManagerProtocol.appendFileName(FileManagerProtocol.listingHeader("/"), "logs", true);

        int entryOffset = 9;
        assertEquals(1, payload[entryOffset]);
        assertEquals(4, payload[entryOffset + 1]);
        assertEquals("logs", new String(payload, entryOffset + 2, 4, StandardCharsets.UTF_8));
    }

    @Test
    void fileHeaderUsesBigEndianSize() {
        byte[] header = FileManagerProtocol.fileHeader(0x0102_0304_0506_0708L);

        assertArrayEquals(new byte[]{0x4e, 0x5a, 0x54, 0x44}, slice(header, 0, 4));
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}, slice(header, 4, 12));
        assertEquals(0x0102_0304_0506_0708L, FileManagerProtocol.readUint64(header, 4));
    }

    @Test
    void errorsUseNerrPrefix() {
        byte[] payload = FileManagerProtocol.error(new IllegalArgumentException("bad path"));

        assertArrayEquals(new byte[]{0x4e, 0x45, 0x52, 0x52}, slice(payload, 0, 4));
        assertEquals("bad path", new String(payload, 4, payload.length - 4, StandardCharsets.UTF_8));
    }

    @Test
    void ioStreamHandshakePrefixesStreamId() {
        List<byte[]> sent = new ArrayList<>();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        IoStreamSession session = new IoStreamSession(
                "test",
                new CapturingStreamObserver(sent),
                scheduler,
                () -> {
                }
        );

        try {
            session.sendStreamId("stream-1");
        } finally {
            session.close();
            scheduler.shutdownNow();
        }

        assertEquals(1, sent.size());
        assertArrayEquals(new byte[]{(byte) 0xff, 0x05, (byte) 0xff, 0x05}, slice(sent.get(0), 0, 4));
        assertEquals("stream-1", new String(sent.get(0), 4, sent.get(0).length - 4, StandardCharsets.UTF_8));
    }

    @Test
    void sendAfterCloseIsIgnored() {
        List<byte[]> sent = new ArrayList<>();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        IoStreamSession session = new IoStreamSession(
                "test",
                new CapturingStreamObserver(sent),
                scheduler,
                () -> {
                }
        );

        session.close();

        try {
            assertTrue(!session.send("ignored".getBytes(StandardCharsets.UTF_8)));
            assertEquals(0, sent.size());
        } finally {
            scheduler.shutdownNow();
        }
    }

    private static byte[] slice(byte[] bytes, int start, int end) {
        byte[] out = new byte[end - start];
        System.arraycopy(bytes, start, out, 0, out.length);
        return out;
    }

    private record CapturingStreamObserver(List<byte[]> sent)
            implements io.grpc.stub.StreamObserver<com.nezhahq.agent.proto.IOStreamData> {
        @Override
        public void onNext(com.nezhahq.agent.proto.IOStreamData value) {
            sent.add(value.getData().toByteArray());
        }

        @Override
        public void onError(Throwable throwable) {
        }

        @Override
        public void onCompleted() {
        }
    }
}
