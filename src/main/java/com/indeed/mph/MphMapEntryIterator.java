package com.indeed.mph;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

/**
 * An iterator over MphMap data, for use by the MphMapEntrySet.
 */
public class MphMapEntryIterator<K, V> implements Iterator<Map.Entry<K, V>> {
    private final MphMap<K, V> mphMap;
    private long hash;
    public MphMapEntryIterator(final MphMap<K, V> mphMap) {
        this.mphMap = mphMap;
        hash = 0;
    }
    public boolean hasNext() {
        return hash < mphMap.size();
    }
    public Map.Entry<K, V> next() {
        try {
            final K extractedKey = mphMap.getKeyForHash(hash);
            final V value = mphMap.get(extractedKey);
            return new AbstractMap.SimpleEntry<>(extractedKey, value);
        } catch (final IOException e) {
            throw new RuntimeException("error reading from MphMap: " + hash, e);
        } finally {
            hash++;
        }
    }
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
