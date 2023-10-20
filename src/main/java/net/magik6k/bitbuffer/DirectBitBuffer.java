package net.magik6k.bitbuffer;

import java.lang.ref.Cleaner;
import java.lang.reflect.Field;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import sun.misc.Unsafe;

public class DirectBitBuffer extends SimpleBitBuffer{
    private final ByteBuffer buffer;

    public DirectBitBuffer(ByteBuffer buffer) {
        int bits = buffer.remaining() * 8;
        limit = bits;
        capacity = bits;
        this.buffer = buffer;
    }

    @Override
    protected byte rawGet(long index) {
        return buffer.get(buffer.position() + (int) index);
    }

    @Override
    protected void rawSet(long index, byte value) {
        buffer.put(buffer.position() + (int) index, value);
    }

    @Override
    protected long rawLength() {
        return buffer.remaining();
    }
}