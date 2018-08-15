package com.indeed.mph;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A wrapper around an MphMap implementing the set interface, for use by entrySet.
 */
public class MphMapEntrySet<K, V> implements Set<Map.Entry<K, V>> {
    final MphMap<K, V> mphMap;
    public MphMapEntrySet(final MphMap<K, V> mphMap) {
        this.mphMap = mphMap;
    }
    public boolean contains(final Object o) {
        if (!(o instanceof Map.Entry)) {
            return false;
        }
        final Map.Entry e = (Map.Entry) o;
        final V value = mphMap.get(e.getKey());
        return value != null && value.equals(e.getValue());
    }
    public boolean containsAll(final Collection c) {
        for (final Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }
        return true;
    }
    public boolean equals(final Object o) {
        if (!(o instanceof MphMapEntrySet)) {
            return false;
        }
        final Collection c = (Collection) o;
        return containsAll(c) && c.containsAll(this);
    }
    public boolean isEmpty() {
        return size() == 0;
    }
    public int size() {
        return mphMap.size();
    }
    public Object[] toArray() {
        return toArray(new Object[size()]);
    }
    public <T> T[] toArray(final T[] a) {
        final Object[] result = a.length >= size() ? a : new Object[size()];
        int i = 0;
        for (final Map.Entry<K, V> e : this) {
            a[i++] = (T) e;
        }
        return (T[]) result;
    }
    public Iterator<Map.Entry<K, V>> iterator() {
        return new MphMapEntryIterator<K, V>(mphMap);
    }
    public boolean add(final Map.Entry<K, V> e) {
        throw new UnsupportedOperationException("MphMaps are immutable");
    }
    public boolean addAll(final Collection<? extends Map.Entry<K, V>> c) {
        throw new UnsupportedOperationException("MphMaps are immutable");
    }
    public void clear() {
        throw new UnsupportedOperationException("MphMaps are immutable");
    }
    public boolean remove(final Object o) {
        throw new UnsupportedOperationException("MphMaps are immutable");
    }
    public boolean removeAll(final Collection c) {
        throw new UnsupportedOperationException("MphMaps are immutable");
    }
    public boolean retainAll(final Collection c) {
        throw new UnsupportedOperationException("MphMaps are immutable");
    }
}
