package com.indeed.mph;

import com.google.common.io.CountingOutputStream;
import com.google.common.io.LittleEndianDataOutputStream;
import com.indeed.mph.serializers.SmartStringSerializer;
import com.indeed.util.core.Pair;
import com.indeed.util.io.BufferedFileDataOutputStream;
import com.indeed.util.mmap.DirectMemory;
import com.indeed.util.mmap.MMapBuffer;
import it.unimi.dsi.bits.AbstractBitVector;
import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.bits.TransformationStrategy;
import it.unimi.dsi.sux4j.bits.HintedBsearchSelect;
import it.unimi.dsi.sux4j.bits.Rank9;
import it.unimi.dsi.sux4j.bits.Select;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Utility class to write mph tables to disk.
 * <p>
 * The static methods write and writeWithTempStorage are all you need
 * to write an Iterable or Iterator of key-value
 * {@link com.indeed.util.core.Pair}s to a directory, given a config.
 * The minimum config is just the key serializer - see TableConfig for
 * details.  Example:
 * <p>
 * <code>
 *   final TableConfig&lt;Long, Long&gt; config =
 *       new TableConfig()
 *       .withKeySerializer(new SmartLongSerializer())
 *       .withValueSerializer(new SmartVLongSerializer());
 *   final Set&lt;Pair&lt;Long, Long&gt;&gt; entries = new HashSet&lt;&gt;();
 *   for (long i = 0; i &lt; 20; ++i) {
 *       entries.add(new Pair(i, i * i));
 *   }
 *   TableWriter.write(new File("squares"), config, entries);
 * </code>
 * <p>
 * As a convenience, the main method lets you build tables from TSV files:
 * <p>
 *   java com.indeed.recommend.common.platform.mph.TableWriter [options] output_dir input.tsv...
 * <p>
 * where options are:
 * <p>
 *   --separator: separator to use instead of tab
 *   --keySerializer: full class name of the key serializer (default .SmartStringSerializer)
 *   --valueSerializer: full class name of the value serializer (default .SmartStringSerializer)
 *   --keyStorage: set to IMPLICIT to remove keys from table
 *   --offsetStorage: override the default choice
 *   --maxHeapUsage: the limit beyond which offsets are mmapped instead of being stored in the heap
 *   --signatureWidth: bits per key to use in a bloom filter (required for IMPLICIT keyStorage)
 * <p>
 * Serializers in the com.indeed.mph.serializers package can be
 * abbreviated with a leading ".", e.g. ".SmartStringSerializer".
 * <p>
 * The serializer syntax also allows simple expressions of the form
 * Class(args...), notably useful for generic serializers, e.g.:
 * <p>
 *   ".SmartListSerializer(.SmartLongSerializer, 20)"
 * <p>
 * is a serializer for lists of up to 20 Longs.
 *
 * @author alexs
 */
public class TableWriter {
    private static final Logger LOGGER = Logger.getLogger(TableWriter.class);
    public static final int MAX_TEMP_SHARDS = 512;

    private TableWriter() {}

    /**
     * Constructs and writes a minimal perfect hash table to
     * outputDir.  The entries may be iterated over multiple times.
     *
     * @param outputDir directory to write the hash table files to
     * @param config    a {@link TableConfig} specifying at least a key serializer
     * @param entries   an iterable of key-value Pairs representing entries in the table
     */
    public static <K, V> void write(
            final File outputDir,
            final TableConfig<K, V> config,
            final Iterable<Pair<K, V>> entries) throws IOException {
        long dataSize = 0;
        for (final Pair<K, V> e : entries) {
            if (e.getFirst() == null || (e.getSecond() == null && config.getValueSerializer() != null)) {
                throw new IllegalArgumentException("can't store nulls: " + e);
            }
            dataSize += config.sizeOf(e.getFirst(), e.getSecond());
        }
        write(outputDir, config, entries, dataSize);
    }

    /**
     * As above, with a pre-computed dataSize, the final size of the
     * raw serialized data in the table (omitting keys if using
     * implicit storage, and omitting values if not used).
     */
    public static <K, V> void write(
            final File outputDir,
            final TableConfig<K, V> config,
            final Iterable<Pair<K, V>> entries,
            final long dataSize) throws IOException {
        if (!config.isValid()) {
            throw new IOException("invalid table config: " + config);
        }
        ensureOutputDirectory(outputDir);
        final TransformationStrategy transformationStrategy =
            new SerializerTransformationStrategy(config.getKeySerializer());
        final GOVMinimalPerfectHashFunction<K> mph = new GOVMinimalPerfectHashFunction.Builder<K>()
            .transform(transformationStrategy)
            .signed(config.getSignatureWidth())
            .keys(new PairFirstIterable(entries))
            .build();
        LOGGER.info("dataSize: " + dataSize + " numEntries: " + mph.size());
        writeWithMinimalPerfectHashFunction(null, outputDir, config, entries, mph, dataSize);
    }

    /**
     * As above, using a one-time iterator.  The entries are written
     * to local temp data, making this suitable for use e.g. when
     * reading from a slow source such as hdfs.
     */
    public static <K, V> void writeWithTempStorage(
            final File outputDir,
            final TableConfig<K, V> config,
            final Iterator<Pair<K, V>> entries,
            final File tempDir) throws IOException {
        if (!config.isValid()) {
            throw new IOException("invalid table config: " + config);
        }
        ensureOutputDirectory(tempDir);
        final File tempDataFile = File.createTempFile("tmp_entries", ".bin", tempDir);
        final BufferedFileDataOutputStream fileOut = new BufferedFileDataOutputStream(tempDataFile);
        long dataSize = 0;
        try (final LittleEndianDataOutputStream out = new LittleEndianDataOutputStream(fileOut)) {
            while (entries.hasNext()) {
                final Pair<K, V> e = entries.next();
                if (e.getFirst() == null || (e.getSecond() == null && config.getValueSerializer() != null)) {
                    throw new IllegalArgumentException("can't store nulls: " + e);
                }
                dataSize += config.sizeOf(e.getFirst(), e.getSecond());
                config.getKeySerializer().write(e.getFirst(), out); // write here even if implicit
                if (config.getValueSerializer() != null) {
                    config.getValueSerializer().write(e.getSecond(), out);
                }
            }
        }
        try {
            final Iterable<Pair<K, V>> tempEntries =
                new SerializedKeyValueIterable(tempDataFile, config.getKeySerializer(), config.getValueSerializer());
            write(outputDir, config, tempEntries, dataSize);
        } finally {
            tempDataFile.delete();
        }
    }

    /**
     * As above, using the outputDir for temp storage.
     */
    public static <K, V> void writeWithTempStorage(final File outputDir, final TableConfig<K, V> config, final Iterator<Pair<K, V>> entries) throws IOException {
        writeWithTempStorage(outputDir, config, entries, outputDir);
    }

    public static void ensureOutputDirectory(final File outputDir) throws IOException {
        if (outputDir.exists()) {
            if (!outputDir.isDirectory()) {
                throw new IOException("can't overwrite regular file with directory: " + outputDir);
            }
        } else {
            outputDir.mkdirs();
            if (!outputDir.exists()) {
                throw new IOException("couldn't create directory: " + outputDir);
            }
        }
    }

    private static <K, V> void writeWithMinimalPerfectHashFunction(
            final File inputData,
            final File outputDir,
            final TableConfig origConfig,
            final Iterable<Pair<K, V>> entries,
            final GOVMinimalPerfectHashFunction<K> mph,
            final long dataSize) throws IOException {
        final TableConfig config = TableConfig.OffsetStorage.AUTOMATIC.equals(origConfig.getOffsetStorage()) ?
            origConfig.withOffsetStorage(origConfig.chooseBestOffsetStorage(mph.size(), dataSize)) :
            origConfig;
        final TableMeta<K, V> meta;
        switch (config.getOffsetStorage()) {
        case FIXED:
            LOGGER.info("writing with fixed offset storage: " + config);
            meta = new TableMeta(config, mph, null, dataSize);
            writeToHashOffsets(outputDir, meta, entries, dataSize);
            break;
        case INDEXED:
            LOGGER.info("writing with indexed offset storage: " + config);
            meta = new TableMeta(config, mph, null, dataSize);
            writeToIndexedOffsets(inputData, new File(outputDir, meta.DEFAULT_DATA_PATH), new File(outputDir, meta.DEFAULT_OFFSETS_PATH), meta, entries, dataSize);
            break;
        case SELECTED:
            LOGGER.info("writing with selected offset storage: " + config);
            final File sizes = writeToHashOffsets(outputDir, new TableMeta(config, mph, null, dataSize), entries, dataSize);
            final Select select = sizesToSelect(config, sizes, dataSize);
            sizes.delete();
            if (select.bitVector() instanceof LongArrayBitVector &&
                (config.getMaxHeapUsage() > 0 && select.numBits() / 8L > config.getMaxHeapUsage())) {
                meta = new TableMeta(config, mph, null, dataSize);
                writeLongs(new File(outputDir, meta.DEFAULT_OFFSETS_PATH), select.bitVector().bits());
            } else {
                meta = new TableMeta(config, mph, select, dataSize);
            }
            break;
        default:
            throw new IllegalArgumentException("unknown offset storage: " + config.getOffsetStorage());
        }
        meta.store(new File(outputDir, meta.DEFAULT_META_PATH));
    }

    private static <K, V> File writeToHashOffsets(
            final File outputDir,
            final TableMeta<K, V> meta,
            final Iterable<Pair<K, V>> entries,
            final long dataSize) throws IOException {
        // integer serialized size of each entry by hash
        final File tempSizes = File.createTempFile("tmpsizes", ".bin");
        // integer hash (offset from start of shard) of each entry by output order
        final File tempHashes = File.createTempFile("tmphashes", ".bin");
        try (final MMapBuffer sizes = new MMapBuffer(tempSizes, 0L, 4L * meta.numEntries(), FileChannel.MapMode.READ_WRITE, ByteOrder.nativeOrder());
             final MMapBuffer hashes = new MMapBuffer(tempHashes, 0L, 4L * meta.numEntries(), FileChannel.MapMode.READ_WRITE, ByteOrder.nativeOrder())) {
            final List<File> shards = splitToShards(outputDir, meta, entries, dataSize, sizes, hashes);
            rewriteShardsInOrder(new File(outputDir, meta.DEFAULT_DATA_PATH), meta, shards, sizes, hashes);
        } finally {
            tempHashes.delete();
        }
        return tempSizes;
    }

    private static <K, V> List<File> splitToShards(
            final File outputDir,
            final TableMeta<K, V> meta,
            final Iterable<Pair<K, V>> entries,
            final long dataSize,
            final MMapBuffer sizes,
            final MMapBuffer hashes) throws IOException {
        final long requestedShardSize = meta.getConfig().getTempShardSize();
        final int baseNumShards = Math.min(MAX_TEMP_SHARDS, (int) (1 + (dataSize / requestedShardSize)));
        final long shardSize = Math.max(1L, (meta.numEntries() + baseNumShards - 1) / baseNumShards);
        final int numShards = Math.max(1, (int) ((meta.numEntries() + shardSize - 1) / shardSize));
        LOGGER.info("splitting " + dataSize + " bytes to " + numShards + " temp shards of " + shardSize + " entries each");
        final List<File> shards = new ArrayList<>(numShards);
        final List<CountingOutputStream> counters = new ArrayList<>(numShards);
        final List<DataOutput> outs = new ArrayList<>(numShards);
        final List<Integer> counts = new ArrayList<>(numShards);
        final long startMillis = System.currentTimeMillis();
        try {
            for (int i = 0; i < numShards; ++i) {
                final File shard = File.createTempFile("tmpshard_" + i, ".bin", outputDir);
                shards.add(shard);
                final CountingOutputStream counter =
                    new CountingOutputStream(new BufferedFileDataOutputStream(shard));
                counters.add(counter);
                outs.add(new LittleEndianDataOutputStream(new DataOutputStream(counter)));
                counts.add(0);
            }
            final DirectMemory sizesMemory = sizes.memory();
            final DirectMemory hashesMemory = hashes.memory();
            for (final Pair<K, V> e : entries) {
                final K key = e.getFirst();
                final V value = e.getSecond();
                final long hash = meta.getHash(key);
                final int shard = (int) (hash / shardSize);  // not modulo
                final DataOutput out = outs.get(shard);
                final CountingOutputStream counter = counters.get(shard);
                final long offset = counter.getCount();
                final int count = counts.get(shard);
                meta.getConfig().write(key, value, out);
                final int size = (int) (counter.getCount() - offset);
                sizesMemory.putInt(hash * 4, size);
                hashesMemory.putInt(((shard * shardSize) + count) * 4, (int) (hash - shard * shardSize));
                counts.set(shard, count + 1);
            }
        } finally {
            for (final OutputStream out : counters) {
                if (out != null) {
                    out.close();
                }
            }
        }
        LOGGER.info("split " + numShards + " shards in " + (System.currentTimeMillis() - startMillis) + " ms");
        return shards;
    }

    private static <K, V> void rewriteShardsInOrder(
            final File outputPath,
            final TableMeta<K, V> meta,
            final List<File> shards,
            final MMapBuffer sizes,
            final MMapBuffer hashes) throws IOException {
        final long startMillis = System.currentTimeMillis();
        try (final DataOutputStream out = new DataOutputStream(new BufferedFileDataOutputStream(outputPath))) {
            final int numShards = shards.size();
            final long shardSize = Math.max(1L, (meta.numEntries() + numShards - 1) / numShards);
            for (int i = 0; i < numShards; ++i) {
                final long start = i * shardSize;
                final long end = Math.min((i + 1) * shardSize, meta.numEntries());
                try {
                    rewriteShardInOrder(out, meta, shards.get(i), shardSize, sizes, hashes, start, end);
                } finally {
                    shards.get(i).delete();
                }
            }
            out.flush();
        }
        outputPath.setReadOnly();
        LOGGER.info("rewrote shards in " + (System.currentTimeMillis() - startMillis) + " ms");
    }

    private static <K, V> void rewriteShardInOrder(
            final DataOutputStream out,
            final TableMeta<K, V> meta,
            final File shard,
            final long shardSize,
            final MMapBuffer sizes,   // by hash
            final MMapBuffer hashes,  // by output order in shard
            final long start,
            final long end) throws IOException {
        // compute offsets by hash
        final DirectMemory sizesMemory = sizes.memory();
        final DirectMemory hashesMemory = hashes.memory();
        final long[] offsets = new long[(int) (end - start)];
        long offset = 0;
        int maxSize = 0;
        for (long i = start; i < end; ++i) {
            final int hash = hashesMemory.getInt(i * 4);
            offsets[hash] = offset;
            final int size = sizesMemory.getInt((hash + start) * 4);
            offset += size;
            if (size > maxSize) {
                maxSize = size;
            }
        }
        final byte[] tmpBuf = new byte[maxSize + 1];
        try (final MMapBuffer inbuf = new MMapBuffer(shard, 0L, shard.length(), FileChannel.MapMode.READ_ONLY, ByteOrder.nativeOrder())) {
            final DirectMemory memory = inbuf.memory();
            for (long i = start; i < end; ++i) {
                final long sourceOffset = offsets[(int) (i - start)];
                final int sourceSize = sizesMemory.getInt(i * 4);
                memory.getBytes(sourceOffset, tmpBuf, 0, sourceSize);
                out.write(tmpBuf, 0, sourceSize);
            }
        }
    }

    private static <K, V> void writeToIndexedOffsets(
            final File inputData,
            final File outputData,
            final File outputOffsets,
            final TableMeta<K, V> meta,
            final Iterable<Pair<K, V>> entries,
            final long dataSize) throws IOException {
        final long numEntries = meta.numEntries();
        final int offsetSize = meta.getConfig().bytesPerOffset(numEntries, dataSize);
        final long totalOffsetSize = numEntries * offsetSize;
        final BufferedFileDataOutputStream fileOut = new BufferedFileDataOutputStream(outputData);
        final CountingOutputStream countOut = new CountingOutputStream(fileOut);
        final long startMillis = System.currentTimeMillis();
        try (final MMapBuffer offsets = new MMapBuffer(outputOffsets, 0L, totalOffsetSize, FileChannel.MapMode.READ_WRITE, ByteOrder.nativeOrder());
             final LittleEndianDataOutputStream out = new LittleEndianDataOutputStream(countOut)) {
            for (final Pair<K, V> e : entries) {
                final long hash = meta.getHash(e.getFirst());
                if (hash < 0) {
                    throw new IOException("inconsistent mph, known key hashed to -1: " + e.getFirst());
                }
                final long offset = countOut.getCount();
                if (offsetSize == 2) {
                    offsets.memory().putShort(hash * 2L, (short) offset);
                } else if (offsetSize == 4) {
                    offsets.memory().putInt(hash * 4L, (int) offset);
                } else {
                    offsets.memory().putLong(hash * 8L, offset);
                }
                meta.getConfig().write(e.getFirst(), e.getSecond(), out);
            }
            offsets.sync(0L, totalOffsetSize);
            out.flush();
        }
        outputData.setReadOnly();
        outputOffsets.setReadOnly();
        LOGGER.info("wrote " + numEntries + " offsets for " + dataSize + " bytes of data in " +
                    (System.currentTimeMillis() - startMillis) + " ms");
    }

    private static <K, V> Select sizesToSelect(final TableConfig<K, V> config,
                                               final File tempSizes,
                                               final long dataSize) throws IOException {
        final long numEntries = tempSizes.length() / 4;
        try (final MMapBuffer sizes = new MMapBuffer(tempSizes, 0L, numEntries * 4, FileChannel.MapMode.READ_ONLY, ByteOrder.nativeOrder())) {
            final DirectMemory sizesMemory = sizes.memory();
            final long maxValue = config.compressOffset(dataSize, numEntries);
            final BitVector bits = LongArrayBitVector.ofLength(maxValue);
            for (long i = 0, offset = 0; i < numEntries; offset += sizesMemory.getInt(i * 4), ++i) {
                final long value = config.compressOffset(offset, i);
                bits.set(value);
            }
            return new HintedBsearchSelect(new Rank9(bits));
        }
    }

    private static void writeLongs(final File outputFile, final long[] values) throws IOException {
        try (final LittleEndianDataOutputStream out = new LittleEndianDataOutputStream(new BufferedFileDataOutputStream(outputFile))) {
            for (final long value : values) {
                out.writeLong(value);
            }
            out.flush();
        }
        outputFile.setReadOnly();
    }

    public static class SerializerTransformationStrategy<K> implements TransformationStrategy<K> {
        private static final long serialVersionUID = 8186081021441487460L;

        final SmartSerializer<K> serializer;

        public SerializerTransformationStrategy(final SmartSerializer<K> serializer) {
            this.serializer = serializer;
        }

        @Override
        public TransformationStrategy<K> copy() {
            return this;
        }

        @Override
        public long length(final K k) {
            return toBytes(k).length * 8;
        }

        @Override
        public long numBits() {
            return 0L;
        }

        @Override
        public BitVector toBitVector(final K k) {
            return new ByteArrayBitVector(toBytes(k));
        }

        private byte[] toBytes(final K k) {
            final ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
            final DataOutputStream dataOutput = new DataOutputStream(byteOutput);
            try {
                serializer.write(k, dataOutput);
            } catch (final IOException e) {
                throw new RuntimeException("failed to serialize", e);
            }
            final byte[] res = byteOutput.toByteArray();
            return res;
        }

        private static class ByteArrayBitVector extends AbstractBitVector {
            final byte[] bytes;

            ByteArrayBitVector(final byte[] bytes) {
                this.bytes = bytes;
            }

            public boolean getBoolean(final int index) {
                return ((bytes[index / 8] >>> (index % 8)) & 1) == 1;
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
    }

    public static class PairFirstIterable<K, V> implements Iterable<K> {
        private final Iterable<Pair<K, V>> iter;
        public PairFirstIterable(final Iterable<Pair<K, V>> iter) {
            this.iter = iter;
        }
        @Override
        public Iterator<K> iterator() {
            return new PairFirstIterator<>(iter.iterator());
        }
    }

    public static class PairFirstIterator<K, V> implements Iterator<K> {
        private final Iterator<Pair<K, V>> iter;
        public PairFirstIterator(final Iterator<Pair<K, V>> iter) {
            this.iter = iter;
        }
        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }
        @Override
        public K next() {
            return iter.next().getFirst();
        }
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public static class SerializedKeyValueIterable<K, V> implements Iterable<Pair<K, V>> {
        private final File file;
        private final SmartSerializer keySerializer;
        private final SmartSerializer valueSerializer;
        public SerializedKeyValueIterable(final File file, final SmartSerializer keySerializer, final SmartSerializer valueSerializer) {
            this.file = file;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }
        @Override
        public Iterator<Pair<K, V>> iterator() {
            try {
                final FileInputStream fileIn = new FileInputStream(file);
                final InputStream in = new BufferedInputStream(fileIn);
                return new SerializedKeyValueIterator<>(in, keySerializer, valueSerializer);
            } catch (final IOException e) {
                throw new IllegalArgumentException("can't iterate on file: " + file, e);
            }
        }
    }

    public static class TsvFileReader<K, V> implements Iterable<Pair<K, V>> {
        private final List<File> files;
        private final Parseable<K> keyParser;
        private final Parseable<V> valueParser;
        private final String separator;
        private final String replace;
        private final String to;
        private final double maxErrorRatio;

        public TsvFileReader(final List<File> files, final Parseable<K> keyParser, final Parseable<V> valueParser, final String separator, final String replace, final String to, final double maxErrorRatio) {
            this.files = files;
            this.keyParser = keyParser;
            this.valueParser = valueParser;
            this.separator = separator;
            this.replace = replace;
            this.to = to;
            this.maxErrorRatio = maxErrorRatio;
        }

        public TsvFileReader(final List<File> files, final Parseable<K> keyParser, final Parseable<V> valueParser, final String separator, final String replace, final String to, final boolean ignoreErrors) {
            this(files, keyParser, valueParser, separator, replace, to, ignoreErrors ? 1.0 : 0.0);
        }

        public TsvFileReader(final File file, final Parseable<K> keyParser, final Parseable<V> valueParser, final String separator, final String replace, final String to, final boolean ignoreErrors) {
            this(Collections.singletonList(file), keyParser, valueParser, separator, replace, to, ignoreErrors);
        }

        public TsvFileReader(final File file, final Parseable<K> keyParser, final Parseable<V> valueParser, final String separator, final String replace, final String to, final double maxErrorRatio) {
            this(Collections.singletonList(file), keyParser, valueParser, separator, replace, to, maxErrorRatio);
        }

        public TsvFileReader(final File file, final Parseable<K> keyParser, final Parseable<V> valueParser, final String separator) {
            this(file, keyParser, valueParser, separator, null, null, false);
        }

        @Override
        public Iterator<Pair<K, V>> iterator() {
            try {
                return new ParseableInputKeyValueIterator(makeSequenceFileInputStream(files), keyParser, valueParser, separator, replace, to, maxErrorRatio);
            } catch (final IOException e) {
                throw new IllegalArgumentException("can't iterate on file: " + files, e);
            }
        }

        private InputStream makeSequenceFileInputStream(final List<File> files) throws IOException {
            InputStream result = new FileInputStream(files.get(0));
            for (int i = 1; i < files.size(); ++i) {
                result = new SequenceInputStream(result, new FileInputStream(files.get(i)));
            }
            return result;
        }
    }

    static Object parseSerializerArg(final String str) throws IOException {
        if (str.startsWith("\"")) {
            return str.substring(1, str.length() - 1); // TODO: handle escapes
        } else if ("true".equalsIgnoreCase(str)) {
            return true;
        } else if ("false".equalsIgnoreCase(str)) {
            return false;
        }
        try {
            if ('L' == str.charAt(str.length() - 1)) {
                return Long.parseLong(str.substring(0, str.length() - 1));
            } else if ('f' == str.charAt(str.length() - 1)) {
                return Float.parseFloat(str.substring(0, str.length() - 1));
            } else {
                return Integer.parseInt(str);
            }
        } catch (final Exception e) {
            return parseSerializerObject(str);
        }
    }

    static Object parseSerializerObject(final String name) throws IOException {
        if ("null".equalsIgnoreCase(name)) {
            return null;
        }
        final List<Object> args = new ArrayList<>();
        final int paren = name.indexOf('(');
        if (paren >= 0) {
            int depth = 0;
            int from = paren + 1;
            for (int i = from; i < name.length(); ++i) {
                switch (name.charAt(i)) {
                case '(':
                    ++depth;
                    break;
                case ')':
                    if (depth == 0) {
                        args.add(parseSerializerArg(name.substring(from, i).trim()));
                        i = name.length();
                    } else {
                        --depth;
                    }
                    break;
                case ',':
                    if (depth == 0) {
                        final String arg = name.substring(from, i).trim();
                        if (!arg.isEmpty()) {
                            args.add(parseSerializerArg(arg));
                        }
                        from = i + 1;
                    }
                    break;
                case '"':
                    for (++i; i < name.length(); ++i) {
                        final char ch = name.charAt(i);
                        if (ch == '"') {
                            break;
                        } else if (ch == '\\') {
                            ++i;
                        }
                    }
                    break;
                }
            }
        }
        final String baseName = paren >= 0 ? name.substring(0, paren) : name;
        final String fullName = baseName.startsWith(".") ?
            "com.indeed.mph.serializers" + baseName : baseName;
        try {
            for (final Constructor constructor : Class.forName(fullName).getConstructors()) {
                try {
                    return constructor.newInstance(args.toArray(new Object[args.size()]));
                } catch (final InstantiationException | IllegalAccessException | InvocationTargetException | IllegalArgumentException e) {
                    // try the next method
                }
            }
        } catch (final ClassNotFoundException e) {
            throw new IOException("unknown class: " + fullName);
        }
        throw new IOException("couldn't find a suitable SmartSerializer constructor: " + name);
    }

    static SmartSerializer<Object> parseSerializer(final String name) throws IOException {
        final Object obj = parseSerializerObject(name);
        if (obj instanceof SmartSerializer) {
            return (SmartSerializer<Object>) obj;
        }
        throw new IOException("not a SmartSerializer: " + name + " -> " + obj);
    }

    public static void main(final String[] args) throws IOException {
        TableConfig<Object, Object> config = new TableConfig()
            .withKeySerializer(new SmartStringSerializer())
            .withValueSerializer(new SmartStringSerializer());
        Parseable<Object> keyParser = null;
        Parseable<Object> valueParser = null;
        String separator = "\t";
        String replace = null;
        String to = "";
        double maxErrorRatio = 0.0;
        boolean withTemp = false;
        int i = 0;
        parse_opts:
        for ( ; i < args.length && args[i].startsWith("-"); ++i) {
            switch (args[i]) {
            case "--":
                break parse_opts;
            case "--keyParser":
                keyParser = (Parseable<Object>) parseSerializerObject(args[++i]); break;
            case "--valueParser":
                valueParser = (Parseable<Object>) parseSerializerObject(args[++i]); break;
            case "--keySerializer":
                config = config.withKeySerializer(parseSerializer(args[++i])); break;
            case "--valueSerializer":
                config = config.withValueSerializer(parseSerializer(args[++i])); break;
            case "--keyStorage":
                config = config.withKeyStorage(TableConfig.KeyStorage.valueOf(args[++i])); break;
            case "--offsetStorage":
                config = config.withOffsetStorage(TableConfig.OffsetStorage.valueOf(args[++i])); break;
            case "--signatureWidth":
                config = config.withSignatureWidth(Integer.parseInt(args[++i])); break;
            case "--maxHeapUsage":
                config = config.withMaxHeapUsage(Long.parseLong(args[++i])); break;
            case "--separator":
                separator = args[++i]; break;
            case "--replace":
                replace = args[++i]; break;
            case "--to":
                to = args[++i]; break;
            case "--ignoreErrors":
                maxErrorRatio = 1.0; break;
            case "--maxErrorRatio":
                maxErrorRatio = Double.parseDouble(args[++i]); break;
            case "--withTempStorage":
                withTemp = true; break;
            default:
                throw new RuntimeException("unknown option: " + args[i]);
            }
        }
        if (args.length - i < 2) {
            throw new RuntimeException("usage: TableWriter [options] <output_dir> <input.tsv> ...");
        }
        final File outputDir = new File(args[i]);
        final List<File> files = new ArrayList<>();
        for (int j = i + 1; j < args.length; ++j) {
            files.add(new File(args[j]));
        }
        final Iterable<Pair<Object, Object>> reader =
            new TsvFileReader(files,
                              ((keyParser != null) ? keyParser : config.getKeySerializer()),
                              ((valueParser != null) ? valueParser : config.getValueSerializer()),
                              separator, replace, to, maxErrorRatio);
        if (withTemp) {
            writeWithTempStorage(outputDir, config, reader.iterator(), outputDir);
        } else {
            write(outputDir, config, reader);
        }
    }
}
