package com.indeed.mph;

import com.google.common.io.LittleEndianDataInputStream;
import com.indeed.util.core.Pair;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author alexs
 */
public class SerializedKeyValueIterator<K, V> implements Iterator<Pair<K, V>>, Closeable {
    private static final Logger LOGGER = Logger.getLogger(SerializedKeyValueIterator.class);
    private final InputStream input;
    private final DataInput in;
    private final SmartSerializer<K> keySerializer;
    private final SmartSerializer<V> valueSerializer;
    private Pair<K, V> nextResult;

    public SerializedKeyValueIterator(final InputStream input, final SmartSerializer<K> keySerializer, final SmartSerializer<V> valueSerializer) throws IOException {
        this.input = input;
        this.in = new LittleEndianDataInputStream(input);
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    @Override
    protected void finalize() throws Throwable {
        close();
    }

    @Override
    public boolean hasNext() {
        if (nextResult == null) {
            try {
                final K key = keySerializer.read(in);
                final V value = valueSerializer.read(in);
                if (key == null || value == null) {
                    close();
                } else {
                    nextResult = new Pair<>(key, value);
                }
            } catch (final IOException e) {
                if (!(e instanceof EOFException)) {
                    LOGGER.error("couldn't read serialized input", e);
                }
                return false;
            }
        }
        return nextResult != null;
    }

    @Override
    public Pair<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final Pair<K, V> result = nextResult;
        nextResult = null;
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
