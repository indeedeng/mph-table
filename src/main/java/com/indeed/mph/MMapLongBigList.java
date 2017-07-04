package com.indeed.mph;

import com.indeed.util.mmap.LongArray;
import it.unimi.dsi.fastutil.longs.AbstractLongBigList;

/**
 * @author xinjianz
 */
public class MMapLongBigList extends AbstractLongBigList {

    private final LongArray longArray;
    private final int width;

    public MMapLongBigList(final LongArray longArray, final int width) {
        this.longArray = longArray;
        this.width = width;
    }

    @Override
    public long getLong(final long index) {
        final long start = index * width;
        return getLong(start, start + width);
    }

    private long getLong(final long from, final long to) {
        ensureFromTo(longArray.length() * Long.SIZE, from, to);
        final long l = Long.SIZE - (to - from);
        final int startWord = word(from);
        final int startBit = bit(from);
        if (l == Long.SIZE) {
            return 0;
        }
        if (startBit <= l) {
            return (longArray.get(startWord) << (l - startBit)) >>> l;
        }
        return (longArray.get(startWord) >>> startBit) | ((longArray.get(startWord + 1) << ((Long.SIZE + l) - startBit)) >>> l);
    }

    private static void ensureFromTo(final long bitVectorLength, final long from, final long to) {
        if(from < 0L) {
            throw new ArrayIndexOutOfBoundsException("Start index (" + from + ") is negative");
        } else if(from > to) {
            throw new IllegalArgumentException("Start index (" + from + ") is greater than end index (" + to + ")");
        } else if(to > bitVectorLength) {
            throw new ArrayIndexOutOfBoundsException("End index (" + to + ") is greater than bit vector length (" + bitVectorLength + ")");
        }
    }

    private static int word(final long index) {
        return (int)(index >>> 6);
    }

    private static int bit(final long index) {
        return (int)(index & 63L);
    }

    @Override
    public long size64() {
        return longArray.length() * Long.SIZE / width;
    }
}
