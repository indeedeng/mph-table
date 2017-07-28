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
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class to read from arbitrary mph tables.  Mph tables are
 * self-descriptive, so no config is needed.  Use SharedTableReader if
 * you need to use the same reader from multiple threads.
 * <p>
 * Example:
 * <p>
 * <code>
 *   try (final TableReader&lt;KeyClass, ValueClass&gt; reader = TableReader.open(dir)) {
 *     final ValueClass value = reader.get(key);
 *   }
 * </code>
 * <p>
 * As a convenience, the main method lets you query existing tables
 * from the command line:
 * <p>
 *   java com.indeed.mph.TableReader [options] table_dir [keys.txt]
 * <p>
 * where options are
 * <p>
 *    --info: print the meta info
 *    --dump: print the key/values of the entire table as TSV
 *    --get key: print the value for a specific key
 *    --random: print the value for a randomly generated long
 * <p>
 * otherwise prints all values for the associated keys taken from
 * keys.txt (defaulting to stdin).  Additional options useful for
 * benchmarking are:
 * <p>
 *    --time: print the time taken to finish all reads
 *    --quiet: don't actually print the values
 *    --repeat n: repeat the read n times
 *    --threads n: duplicate all reads simultaneously among n threads
 *
 * @author alexs
 */
public class TableReader<K, V> implements Closeable, Iterable<Pair<K, V>> {
    private final TableMeta<K, V> meta;
    private final MMapBuffer offsets;
    private final Memory memory;
    private final Select select;
    private MMapBuffer data;
    private Memory dataMemory;
    private final AtomicLong filteredCount = new AtomicLong(0L);
    private final AtomicLong missingCount = new AtomicLong(0L);
    private final AtomicLong retrievedCount = new AtomicLong(0L);

    public TableReader(@Nonnull final TableMeta<K, V> meta,
                       @Nonnull final byte[] rawData) {
        this.meta = meta;
        dataMemory = new HeapMemory(rawData, ByteOrder.nativeOrder());
        data = null;
        offsets = null;
        select = null;
        memory = null;
    }

    public TableReader(@Nonnull final TableMeta<K, V> meta,
                       @Nonnull final MMapBuffer data,
                       @Nullable final MMapBuffer offsets) {
        this.meta = meta;
        this.data = data;
        this.dataMemory = data.memory();
        this.offsets = offsets;
        if (offsets != null) {
            if (TableConfig.OffsetStorage.SELECTED.equals(meta.getConfig().getOffsetStorage())) {
                select = new HintedBsearchSelect(new Rank9(new MMapBitVector(offsets)));
                memory = null;
            } else {
                select = null;
                memory = offsets.memory();
            }
        } else {
            select = null;
            memory = null;
        }
    }

    @Override
    public void close() throws IOException {
        if (offsets != null) offsets.close();
        if (data != null) data.close();
        data = null;
        dataMemory = null;
    }

    /**
     * General interface to opening a TableReader.  Only the metaPath
     * is required, variants below are conveniences with default
     * parameters.
     *
     * @param metaPath          path to the table's meta-data, either the exact file or the containing directory
     * @param offsetsPath       path to the raw offsets if different from the meta-data setting
     * @param dataPath          path to the raw serialized data if different from the meta-data setting
     * @param maxDataHeapUsage  if specified and positive, raw data less than this size will be
     *                            stored directly in the heap instead of mmaped
     * @param <K> key type
     * @param <V> value type
     * @return a new TableReader on the data
     * @throws IOException if unable to open any of the files, or deserialize the metadata
     */
    public static <K, V> TableReader<K, V> open(@Nonnull final File metaPath,
                                                @Nullable final File offsetsPath,
                                                @Nullable final File dataPath,
                                                @Nullable final Long maxDataHeapUsage) throws IOException {
        final TableMeta<K, V> meta = TableMeta.load(metaPath, offsetsPath, dataPath);
        final MMapBuffer data =
            new MMapBuffer(meta.getDataPath(), FileChannel.MapMode.READ_ONLY, ByteOrder.nativeOrder());
        final MMapBuffer offsets = TableConfig.OffsetStorage.INDEXED.equals(meta.getConfig().getOffsetStorage()) ||
            (TableConfig.OffsetStorage.SELECTED.equals(meta.getConfig().getOffsetStorage()) && meta.getSelectOffsets() == null) ?
            new MMapBuffer(meta.getOffsetsPath(), FileChannel.MapMode.READ_ONLY, ByteOrder.nativeOrder()) :
            null;
        final long maxDataHeap =
            maxDataHeapUsage != null ? maxDataHeapUsage : meta.getConfig().getMaxDataHeapUsage();
        if (offsets == null && data.memory().length() < maxDataHeap) {
            final byte[] rawData = new byte[(int) data.memory().length()];
            data.memory().getBytes(0, rawData);
            data.close();
            return new TableReader(meta, rawData);
        } else {
            return new TableReader(meta, data, offsets);
        }
    }

    public static <K, V> TableReader<K, V> open(@Nonnull final File metaPath, @Nullable final File offsetsPath, @Nullable final File dataPath) throws IOException {
        return open(metaPath, offsetsPath, dataPath, null);
    }

    public static <K, V> TableReader<K, V> open(@Nonnull final File metaPath, @Nullable final Long maxDataHeapUsage) throws IOException {
        return open(metaPath, null, null, maxDataHeapUsage);
    }

    public static <K, V> TableReader<K, V> open(@Nonnull final File metaPath) throws IOException {
        return open(metaPath, null, null, null);
    }

    public static <K, V> TableReader<K, V> open(@Nonnull final String metaPath) throws IOException {
        return open(new File(metaPath));
    }

    public static <K, V> TableReader<K, V> open(@Nonnull final String metaPath, @Nullable final Long maxDataHeapUsage) throws IOException {
        return open(new File(metaPath), maxDataHeapUsage);
    }

    public boolean isDiskBased() {
        return data != null || memory != null;
    }

    public TableMeta<K, V> getMeta() {
        return meta;
    }

    public TableConfig<K, V> getConfig() {
        return meta.getConfig();
    }

    public TableStats getStats() {
        return new TableStats(filteredCount.get(), missingCount.get(), retrievedCount.get());
    }

    public boolean containsKey(@Nonnull final K key) {
        try {
            if (meta.getConfig().getKeyValidator() == null) {
                return getOffset(key) >= 0;
            }
            return get(key) != null;
        } catch (final IOException e) {
            return false;
        }
    }

    public V get(@Nonnull final K key) throws IOException {
        if (dataMemory == null) {
            throw new IOException("table has been closed!");
        }
        final long offset = getOffset(key);
        if (offset < 0) {
            filteredCount.incrementAndGet();
            return null;
        }
        final MemoryDataInput in = new MemoryDataInput(dataMemory);
        in.seek(offset);
        final TableConfig<K, V> config = meta.getConfig();
        final K extractedKey = config.readKey(in);
        final V value = config.readValue(in);
        if (config.getKeyValidator() != null) {
            final V result = config.getKeyValidator().validate(key, extractedKey, value);
            if (result == null) {
                missingCount.incrementAndGet();
            } else {
                retrievedCount.incrementAndGet();
            }
            return result;
        }
        retrievedCount.incrementAndGet();
        return value;
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
        return meta.getHash(key);
    }

    public long getOffset(@Nonnull final K key) {
        return meta.getOffset(key, memory, select);
    }

    public long getHashOffset(final long hash) {
        return meta.getHashOffset(hash, memory, select);
    }

    public long size() {
        return meta.numEntries();
    }

    public long getSizeInBytes() {
        return meta.getSizeInBytes();
    }

    public long getTimestamp() {
        return meta.getTimestamp();
    }

    public TableIterator iterator() {
        return new TableIterator();
    }

    public class TableIterator implements Iterator<Pair<K, V>> {
        private final MemoryDataInput in;
        private long hash;
        public TableIterator() {
            in = new MemoryDataInput(dataMemory);
            hash = 0;
        }
        @Override
        public boolean hasNext() {
            return hash < meta.numEntries();
        }
        @Override
        public Pair<K, V> next() {
            final long offset = meta.getHashOffset(hash++, memory, select);
            try {
                in.seek(offset);
                final TableConfig<K, V> config = meta.getConfig();
                final K extractedKey = config.readKey(in);
                final V value = config.readValue(in);
                return new Pair<>(extractedKey, value);
            } catch (final IOException e) {
                throw new RuntimeException("error reading from TableIterator: " + offset, e);
            }
        }
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public static class TableStats {
        public final long filteredCount;
        public final long missingCount;
        public final long retrievedCount;
        public TableStats(final long filteredCount, final long missingCount, final long retrievedCount) {
            this.filteredCount = filteredCount;
            this.missingCount = missingCount;
            this.retrievedCount = retrievedCount;
        }
        public String toString() {
            final long total = filteredCount + missingCount + retrievedCount;
            if (total == 0) {
                return "[filtered: 0 missing: 0 retrived: 0]";
            }
            return "[filtered: " + filteredCount + " (" + (100.0 * filteredCount / total) +
                "%) missing: " + missingCount + " (" + (100.0 * missingCount / total) +
                "%) retrieved: " + retrievedCount + " (" + (100.0 * retrievedCount / total) +"%)]";
        }
    }

    public static void main(final String[] args) throws IOException, InterruptedException {
        String keyStr = null;
        boolean quiet = false;
        boolean info = false;
        boolean dump = false;
        boolean time = false;
        boolean random = false;
        int repetitions = 1;
        int threads = 1;
        int i = 0;
        parse_opts:
        for ( ; i < args.length && args[i].startsWith("-"); ++i) {
            switch (args[i]) {
            case "--": break parse_opts;
            case "--dump": dump = true; break;
            case "--info": info = true; break;
            case "--quiet": quiet = true; break;
            case "--time": time = true; break;
            case "--random": random = true; break;
            case "--get": keyStr = args[++i]; break;
            case "--repeat": repetitions = Integer.parseInt(args[++i]); break;
            case "--threads": threads = Integer.parseInt(args[++i]); break;
            default: throw new RuntimeException("unknown option: " + args[i]);
            }
        }
        if (args.length - i < 1) {
            throw new RuntimeException("usage: TableReader [--dump|--info|--quiet|--time|--random|--repeat n|--threads n|--get k] <db> [<keys.txt>]");
        }
        final File tablePath = new File(args[i]);
        final String keyInput = keyStr != null || random || i + 1 >= args.length ? null : args[i + 1];
        if (info || dump) {
            try (final TableReader<Object, Object> reader = TableReader.<Object, Object>open(tablePath)) {
                if (info) {
                    System.out.println(reader.meta);
                }
                if (dump) {
                    final TableConfig<Object, Object> config = reader.getConfig();
                    for (final Pair<Object, Object> entry : reader) {
                        System.out.println(config.getKeySerializer().printToString(entry.getFirst()) + "\t" +
                                           (config.getValueSerializer() == null ? null :
                                            config.getValueSerializer().printToString(entry.getSecond())));
                    }
                }
            }
            return;
        }
        final List<Thread> workers = new ArrayList<Thread>();
        final int reps = repetitions;
        final boolean rand = random;
        final boolean quietly = quiet;
        final long startTime = System.currentTimeMillis();
        try (final TableReader<Object, Object> reader = TableReader.<Object, Object>open(tablePath)) {
            final TableConfig<Object, Object> config = reader.getConfig();
            final Object defaultKey = keyStr != null ? config.getKeySerializer().parseFromString(keyStr) : null;
            for (int t = 0; t < threads; ++t) {
                workers.add(new Thread() {
                        public void run() {
                            final Random rng = new Random();
                            try {
                                final Object key = defaultKey != null ? defaultKey : rand ? rng.nextLong() : null;
                                for (int j = 0; j < reps; ++j) {
                                    if (key != null) {
                                        final Object value = reader.get(key);
                                        if (!quietly) {
                                            System.out.println(config.getValueSerializer().printToString(value));
                                        }
                                    } else {
                                        try (final BufferedReader in = new BufferedReader(
                                            keyInput == null ? new FileReader(FileDescriptor.in) : new FileReader(keyInput))) {
                                            while (true) {
                                                final String line = in.readLine();
                                                if (line == null) {
                                                    break;
                                                }
                                                final Object value = reader.get(config.getKeySerializer().parseFromString(line));
                                                if (!quietly) {
                                                    System.out.println(config.getValueSerializer().printToString(value));
                                                }
                                            }
                                        }
                                    }
                                }
                            } catch (final IOException e) {
                                throw new RuntimeException("error reading table", e);
                            }
                        }
                    });
            }
            for (final Thread worker : workers) {
                worker.start();
            }
            for (final Thread worker : workers) {
                worker.join();
            }
        }
        if (time) {
            System.out.println("read complete in " + (System.currentTimeMillis() - startTime) + " ms");
        }
    }
}
