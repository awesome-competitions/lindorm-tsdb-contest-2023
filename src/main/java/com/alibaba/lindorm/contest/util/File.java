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

    public static ByteBuffer decompress(Path src) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (FileInputStream fis = new FileInputStream(src.toString());
             ZstdInputStream zis = new ZstdInputStream(fis);) {
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = zis.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
        }
        ByteBuffer buffer = ByteBuffer.allocate(baos.size());
        buffer.put(baos.toByteArray());
        buffer.flip();
        return buffer;
    }
}
