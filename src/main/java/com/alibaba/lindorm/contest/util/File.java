package com.alibaba.lindorm.contest.util;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

public class File {

    public static long getFileSize(Path path) throws IOException {
        return Files.size(path);
    }

    public static void compress(Path input) throws IOException {
        Path output = Path.of(input.toString() + ".z");
        try (FileInputStream fis = new FileInputStream(input.toString());
             FileOutputStream fos = new FileOutputStream(output.toString());
             ZstdOutputStream zos = new ZstdOutputStream(fos)) {

            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                zos.write(buffer, 0, bytesRead);
            }
            zos.flush();
        }
        Files.delete(input);
        Files.move(output, input);
    }

    public static ByteBuffer decompress(Path src, int size) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        byte[] bs = new byte[4096];
        try (FileInputStream fis = new FileInputStream(src.toString());
             ZstdInputStream zis = new ZstdInputStream(fis);) {
            int bytesRead;
            while ((bytesRead = zis.read(bs)) != -1) {
                buffer.put(bs, 0, bytesRead);
            }
        }
        buffer.flip();
        return buffer;
    }
}
