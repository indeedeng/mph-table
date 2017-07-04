package com.indeed.mph;

import com.indeed.util.core.reference.AtomicSharedReference;
import com.indeed.util.core.reference.SharedReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * Thread-safe reference counting version of TableReader.
 *
 * @author alexs
 */
public class SharedTableReader<K, V> implements Closeable {
    private final AtomicSharedReference<TableReader<K, V>> reader;

    public SharedTableReader(@Nonnull final TableReader<K, V> reader) {
        this.reader = AtomicSharedReference.create(reader);
    }

    @Override
    public void close() throws IOException {
        reader.unset();
    }

    public static <K, V> SharedTableReader<K, V> open(@Nonnull final File metaPath, @Nullable final File offsetsPath, @Nullable final File dataPath) throws IOException {
        return new SharedTableReader(TableReader.open(metaPath, offsetsPath, dataPath));
    }

    public static <K, V> SharedTableReader<K, V> open(@Nonnull final File metaPath) throws IOException {
        return open(metaPath, null, null);
    }

    public static <K, V> SharedTableReader<K, V> open(@Nonnull final String metaPath) throws IOException {
        return open(new File(metaPath));
    }

    public SharedReference<TableReader<K, V>> getCopy() {
        return reader.getCopy();
    }

    public TableConfig<K, V> getConfig() throws IOException {
        try (final SharedReference<TableReader<K, V>> reader = getCopy()) {
            return reader.get().getMeta().getConfig();
        }
    }

    public TableReader.TableStats getStats() throws IOException {
        try (final SharedReference<TableReader<K, V>> reader = getCopy()) {
            return reader.get().getStats();
        }
    }

    public boolean containsKey(@Nonnull final K key) {
        try (final SharedReference<TableReader<K, V>> reader = getCopy()) {
            if (reader.get().getMeta().getConfig().getKeyValidator() == null) {
                return reader.get().getOffset(key) >= 0;
            }
            return get(key) != null;
        } catch (final IOException e) {
            return false;
        }
    }

    public V get(@Nonnull final K key) throws IOException {
        try (final SharedReference<TableReader<K, V>> reader = getCopy()) {
            final TableReader<K, V> r = reader == null ? null : reader.get();
            if (r == null) {
                throw new IOException("table was already closed fetching: " + key);
            }
            return r.get(key);
        }
    }

    public long size() {
        try (final SharedReference<TableReader<K, V>> reader = getCopy()) {
            return reader.get().getMeta().numEntries();
        } catch (final IOException e) {
            return 0L;
        }
    }

    public long getSizeInBytes() {
        try (final SharedReference<TableReader<K, V>> reader = getCopy()) {
            return reader.get().getSizeInBytes();
        } catch (final IOException e) {
            return 0L;
        }
    }

    public long getTimestamp() {
        try (final SharedReference<TableReader<K, V>> reader = getCopy()) {
            return reader.get().getMeta().getTimestamp();
        } catch (final IOException e) {
            return 0L;
        }
    }
}
