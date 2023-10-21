package net.magik6k.bitbuffer;

import java.lang.ref.Cleaner;
import java.lang.reflect.Field;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import sun.misc.Unsafe;

public class DirectBitBuffer extends SimpleBitBuffer{
    private final ByteBuffer buffer;

    private final int initPosition;

    private final int initCapacity;

    public DirectBitBuffer(ByteBuffer buffer) {
        int bits = buffer.remaining() * 8;
        limit = bits;
        capacity = bits;
        this.buffer = buffer;
        this.initPosition = buffer.position();
        this.initCapacity = buffer.capacity();
    }

    @Override
    protected byte rawGet(long index) {
        return buffer.get(initPosition + (int) index);
    }

    @Override
    protected void rawSet(long index, byte value) {
        buffer.put(initPosition + (int) index, value);
    }

    @Override
    protected long rawLength() {
        return initCapacity;
    }

    @Override
    public BitBuffer flip() {
        super.flip();
        buffer.position(initPosition + (int)(super.limit() + 7)/8);
        return this;
    }
}