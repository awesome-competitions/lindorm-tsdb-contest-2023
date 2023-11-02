package com.alibaba.lindorm.contest.util;

import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class XZ {
    public static void compress(ByteBuffer dst, ByteBuffer src) throws IOException {
        LZMA2Options options = new LZMA2Options();
        options.setPreset(9);
        options.setDictSize(1 << 12);
        try (XZOutputStream xzOutputStream = new XZOutputStream(new ByteBufferOutputStream(dst), options)) {
            WritableByteChannel channel = Channels.newChannel(xzOutputStream);
            channel.write(src);
        }
    }

    public static void decompress(ByteBuffer dst, ByteBuffer src) throws IOException {
        try (XZInputStream xzInputStream = new XZInputStream(new ByteBufferInputStream(src))) {
            dst.put(xzInputStream.readAllBytes());
        }
    }

    // Helper class to adapt a ByteBuffer to a ReadableByteChannel
    private static class ByteBufferInputStream extends InputStream {
        private final ByteBuffer buffer;

        public ByteBufferInputStream(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public int read() throws IOException {
            if (!buffer.hasRemaining()) {
                return -1;
            }
            return buffer.get() & 0xFF;
        }

        @Override
        public int read(byte[] bytes, int off, int len) throws IOException {
            if (!buffer.hasRemaining()) {
                return -1;
            }
            len = Math.min(len, buffer.remaining());
            buffer.get(bytes, off, len);
            return len;
        }
    }

    // Helper class to adapt a ByteBuffer to a WritableByteChannel
    private static class ByteBufferOutputStream extends OutputStream {
        private final ByteBuffer buffer;

        public ByteBufferOutputStream(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public void write(int b) throws IOException {
            buffer.put((byte) b);
        }

        @Override
        public void write(byte[] bytes, int off, int len) throws IOException {
            buffer.put(bytes, off, len);
        }
    }

    public static void main(String[] args) throws IOException {
        String originalString = "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111";
        byte[] originalBytes = originalString.getBytes();
        ByteBuffer srcBuffer = ByteBuffer.allocateDirect(originalBytes.length);
        srcBuffer.put(originalBytes);
        srcBuffer.flip();
        ByteBuffer compressedBuffer = ByteBuffer.allocateDirect(10000);
        ByteBuffer decompressedBuffer = ByteBuffer.allocateDirect(10000);

        // 压缩
        compress(compressedBuffer, srcBuffer);
        compressedBuffer.flip();
        System.out.println(compressedBuffer.remaining());

        // 解压缩
        decompress(decompressedBuffer, compressedBuffer);
        decompressedBuffer.flip();
        System.out.println(decompressedBuffer.remaining());

        // 将解压缩后的字节转换回字符串并输出
        byte[] decompressedBytes = new byte[decompressedBuffer.remaining()];
        decompressedBuffer.get(decompressedBytes);
        String decompressedString = new String(decompressedBytes);
        System.out.println("Decompressed String: " + decompressedString);
    }
}
