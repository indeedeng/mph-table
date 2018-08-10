package com.indeed.mph;

import it.unimi.dsi.bits.AbstractBitVector;

public class ByteArrayBitVector extends AbstractBitVector {
    final byte[] bytes;

    ByteArrayBitVector(final byte[] bytes) {
        this.bytes = bytes;
    }

    public boolean getBoolean(final int index) {
        final byte b = bytes[index / 8];
        return ((b >>> (index % 8L)) & 1L) == 1L;
    }

    public boolean getBoolean(final long index) {
        return getBoolean((int) index);
    }

    public long length() {
        return size64();
    }

    public long size64() {
        return bytes.length * 8;
    }
}
