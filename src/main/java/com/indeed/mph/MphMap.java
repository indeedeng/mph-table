package com.indeed.mph;

import com.indeed.util.core.Pair;
import com.indeed.util.mmap.HeapMemory;
import com.indeed.util.mmap.MMapBuffer;
import com.indeed.util.mmap.Memory;
import com.indeed.util.mmap.MemoryDataInput;
import it.unimi.dsi.sux4j.bits.HintedBsearchSelect;
import it.unimi.dsi.sux4j.bits.Rank9;
import it.unimi.dsi.sux4j.bits.Select;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.indeed.util.io.Files.loadFileAsByteArray;

/**
 * An in-heap mph table implementing the Map interface, with
 * constructors to load from on-disk tables or convert directly from
 * another Map.  All of the optional methods (those involving
 * mutation) are unsupported.  If keys are stored implicitly, in
 * addition to the normal chance of false positive lookups, the
 * entrySet method will raise an exception.  This introduces some
 * computation overhead compared to other Maps but can allow for
 * extremely compact in-heap lookups.
 *
 * As a convenience also implements Serializable, allowing you to
 * store mph tables in resources, use with spark and other distributed
 * processing, etc. at the expense of using more heap and imposing a
 * 2GB limit on the (total, serialized) data size.
 *
 * @author alexs
 */
public class MphMap<K, V> implements Map<K, V>, Serializable {
    private static final long serialVersionUID = -607723193592825808L;
    private final TableMeta<K, V> meta;
    private final Memory memory;
    private final Select select;
    private Memory dataMemory;
    private byte[] data;
    private byte[] offsets;
    private final K minKey;
    private final K maxKey;

    private MphMap(final TableMeta<K, V> meta, final byte[] data, final byte[] offsets) {
        this.meta = meta;
        this.data = data;
        this.offsets = offsets;
        dataMemory = new HeapMemory(data, ByteOrder.nativeOrder());
        minKey = meta.getMinKey();
        maxKey = meta.getMaxKey();
        if (offsets != null) {
            if (TableConfig.OffsetStorage.SELECTED.equals(meta.getConfig().getOffsetStorage())) {
                select = new HintedBsearchSelect(new Rank9(new ByteArrayBitVector(offsets)));
                memory = null;
            } else {
                select = null;
                memory = new HeapMemory(offsets, ByteOrder.nativeOrder());
            }
        } else {
            select = null;
            memory = null;
        }
    }

    /**
     * Static constructor to load a disk-based mph table (as created
     * by {@link TableWriter}) into memory.
     *
     * @param metaPath          path to the table's meta-data, either the exact file or the containing directory
     * @param offsetsPath       path to the raw offsets if different from the meta-data setting
     * @param dataPath          path to the raw serialized data if different from the meta-data setting
     *
     * @param <K> key type
     * @param <V> value type
     * @return a new MphMap holding the data in memory
     * @throws IOException if unable to open any of the files, or deserialize the metadata
     */
    public static <K, V> MphMap<K, V> load(
            @Nonnull final File metaPath,
            @Nullable final File offsetsPath,
            @Nullable final File dataPath) throws IOException {
        final TableMeta<K, V> meta = TableMeta.load(metaPath, offsetsPath, dataPath);
        final byte[] data = loadFileAsByteArray(meta.getDataPath().getPath());
        final byte[] offsets = (meta.getOffsetsPath() != null && meta.getOffsetsPath().exists()) ?
            loadFileAsByteArray(meta.getOffsetsPath().getPath()) : null;
        return new MphMap<>(meta, data, offsets);
    }

    public static <K, V> MphMap<K, V> load(@Nonnull final File metaPath) throws IOException {
        return load(metaPath, null, null);
    }

    /**
     * Static constructor to deserialize an MphMap. This is not the disk-based mph table format.
     *
     * @param in    an input stream to read the serialized object from
     *
     * @param <K> key type
     * @param <V> value type
     * @return a new MphMap holding the data in memory
     * @throws IOException if unable to deserialize
     */
    public static <K, V> MphMap<K, V> deserialize(@Nonnull final InputStream in) throws IOException {
        try (final ObjectInputStream objIn = new ObjectInputStream(in)) {
            return (MphMap<K, V>) objIn.readObject();
        } catch (final ClassNotFoundException e) {
            throw new IOException("couldn't open table", e);
        }
    }

    /**
     * Static constructor to build a table from another Map.  This can
     * be a convenient way to compress a Map generated by other means.
     *
     * @param config   the TableConfig specifying at least the key/value serializers
     * @param entries  the Map to copy from
     *
     * @param <K> key type
     * @param <V> value type
     * @return a new MphMap holding the data in memory
     * @throws IOException if unable to serialize
     */
    public static <K, V> MphMap<K, V> fromMap(
            @Nonnull final TableConfig<K, V> config,
            @Nonnull final Map<K, V> entries) throws IOException {
        return fromKeyValues(config, new Iterable<Pair<K, V>>() {
                public Iterator<Pair<K, V>> iterator() {
                    return new MapEntryToPairIterator<K, V>(entries.entrySet().iterator());
                }
            });
    }

    /**
     * Static constructor to build a table from key/value pairs, as
     * would be passed to the {@link TableWriter} constructor.
     *
     * @param config   the TableConfig specifying at least the key/value serializers
     * @param entries  an iterable of key/value pairs to covnert to a Map
     *
     * @param <K> key type
     * @param <V> value type
     * @return a new MphMap holding the data in memory
     * @throws IOException if unable to serialize
     */
    public static <K, V> MphMap<K, V> fromKeyValues(
            @Nonnull final TableConfig<K, V> config,
            @Nonnull final Iterable<Pair<K, V>> entries) throws IOException {
        final File tmpDir = Files.createTempDirectory("mph").toFile();
        try {
            TableWriter.write(tmpDir, config, entries);
            return load(tmpDir);
        } finally {
            com.indeed.util.io.Files.delete(tmpDir.getPath());
        }
    }

    private static class MapEntryToPairIterator<K, V> implements Iterator<Pair<K, V>> {
        private final Iterator<Map.Entry<K, V>> iter;
        public MapEntryToPairIterator(final Iterator<Map.Entry<K, V>> iter) {
            this.iter = iter;
        }
        public boolean hasNext() {
            return iter.hasNext();
        }
        public Pair<K, V> next() {
            final Map.Entry<K, V> e = iter.next();
            return new Pair<K, V>(e.getKey(), e.getValue());
        }
    }

    /**
     * Returns the meta-data associated with this mph table.
     */
    private TableMeta<K, V> getMeta() {
        return meta;
    }

    /**
     * Returns the config for this mph table, specifying serializers and other info.
     *
     * @return the TableConfig for this mph
     */
    public TableConfig<K, V> getConfig() {
        return meta.getConfig();
    }

    public boolean containsKey(@Nonnull final Object o) {
        try {
            final K key = (K) o;
            if (meta.getConfig().getKeyValidator() == null) {
                return getOffset(key) >= 0;
            }
            return get(key) != null;
        } catch (final Exception e) {
            return false;
        }
    }

    public boolean containsValue(final Object o) {
        return values().contains(o);
    }

    public V get(@Nonnull final Object o) {
        try {
            final K key = (K) o;
            final long offset = getOffset(key);
            if (offset < 0) {
                return null;
            }
            final MemoryDataInput in = new MemoryDataInput(dataMemory);
            in.seek(offset);
            final TableConfig<K, V> config = meta.getConfig();
            final K extractedKey = config.readKey(in);
            final V value = config.readValue(in);
            if (config.getKeyValidator() != null) {
                final V result = config.getKeyValidator().validate(key, extractedKey, value);
                return result;
            }
            return value;
        } catch (final IOException e) {
            throw new RuntimeException("corrupt serialized data in MphMap", e);
        }
    }

    // no validation other than range, just get the value associated with the hash
    public MemoryDataInput getMemoryForHash(final long hash) throws IOException {
        if (dataMemory == null) {
            throw new IOException("table has been closed!");
        }
        if (hash < 0 || hash >= size()) {
            return null;
        }
        final long offset = getHashOffset(hash);
        if (offset < 0) {
            return null;
        }
        final MemoryDataInput in = new MemoryDataInput(dataMemory);
        in.seek(offset);
        return in;
    }

    public K getKeyForHash(final long hash) throws IOException {
        final MemoryDataInput in = getMemoryForHash(hash);
        if (in == null) {
            return null;
        }
        return meta.getConfig().readKey(in);
    }

    public V getForHash(final long hash) throws IOException {
        final MemoryDataInput in = getMemoryForHash(hash);
        if (in == null) {
            return null;
        }
        meta.getConfig().readKey(in);
        return meta.getConfig().readValue(in);
    }

    public long getHash(@Nonnull final K key) {
        if ((minKey != null && ((Comparable) minKey).compareTo(key) > 0)
            || (maxKey != null && ((Comparable) maxKey).compareTo(key) < 0)) {
            return -1;
        }
        return meta.getHash(key);
    }

    public long getOffset(@Nonnull final K key) {
        if ((minKey != null && ((Comparable) minKey).compareTo(key) > 0)
            || (maxKey != null && ((Comparable) maxKey).compareTo(key) < 0)) {
            return -1;
        }
        return meta.getOffset(key, memory, select);
    }

    public long getHashOffset(final long hash) {
        return meta.getHashOffset(hash, memory, select);
    }

    public int size() {
        return (int) meta.numEntries();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public long getSizeInBytes() {
        return meta.getSizeInBytes();
    }

    public long getTimestamp() {
        return meta.getTimestamp();
    }

    private void writeObject(final ObjectOutputStream out) throws IOException {
        dataMemory = null;
        out.defaultWriteObject();
    }

    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        dataMemory = new HeapMemory(data, ByteOrder.nativeOrder());
    }

    public Set<Map.Entry<K, V>> entrySet() {
        if (TableConfig.KeyStorage.IMPLICIT.equals(meta.getConfig().getKeyStorage())) {
            throw new UnsupportedOperationException("can't iterate over MphMap with implicit keys");
        }
        return new MphMapEntrySet(this);
    }

    public Set<K> keySet() {
        return entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet());
    }

    public Collection<V> values() {
        return entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList());
    }

    public void clear() {
        throw new UnsupportedOperationException("MphMaps are immutable");
    }
    public V put(final K key, final V value) {
        throw new UnsupportedOperationException("MphMaps are immutable");
    }
    public void putAll(final Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException("MphMaps are immutable");
    }
    public V remove(final Object key) {
        throw new UnsupportedOperationException("MphMaps are immutable");
    }
}
