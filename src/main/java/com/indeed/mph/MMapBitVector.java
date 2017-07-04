package com.indeed.mph;

import com.indeed.util.mmap.MMapBuffer;
import it.unimi.dsi.bits.AbstractBitVector;

import java.io.IOException;

public class MMapBitVector extends AbstractBitVector {
    final MMapBuffer buffer;    // we're not responsible for closing this

    MMapBitVector(final MMapBuffer buffer) {
        this.buffer = buffer;
    }

    public boolean getBoolean(final int index) {
        return getBoolean((long) index);
    }

    public boolean getBoolean(final long index) {
        final byte b = buffer.memory().getByte(index / 8);
        return ((b >>> (index % 8L)) & 1L) == 1L;
    }

    public long length() {
        return size64();
    }

    public long size64() {
        return buffer.memory().length() * 8;
    }
}
