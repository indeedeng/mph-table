package com.indeed.mph;

import com.google.common.collect.ImmutableList;
import com.indeed.mph.serializers.SmartByteSerializer;
import com.indeed.mph.serializers.SmartDictionarySerializer;
import com.indeed.mph.serializers.SmartListSerializer;
import com.indeed.mph.serializers.SmartLongSerializer;
import com.indeed.mph.serializers.SmartStringSerializer;
import com.indeed.mph.serializers.SmartVLongSerializer;
import com.indeed.util.core.Pair;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTableWriter {

    File tmpDir;

    @Before
    public void setUp() throws Exception {
        tmpDir = File.createTempFile("tmptablewriter", "", new File("."));
        tmpDir.delete();
        tmpDir.mkdirs();
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(tmpDir);
    }

    @Test
    public void testDups() throws Exception {
        final File table = new File(tmpDir, "dups");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartLongSerializer());
        final List<Pair<Long, Long>> entries = new ArrayList<>();
        for (long i = 0; i < 20; ++i) {
            entries.add(new Pair(i, i * i));
        }
        entries.add(new Pair(3L, 5L));
        boolean failed = false;
        try {
            TableWriter.write(table, config, entries);
        } catch (final IllegalArgumentException e) {
            failed = true;
        }
        assertTrue(failed);
    }

    @Test
    public void testDupsWithDebugging() throws Exception {
        final File table = new File(tmpDir, "debugdups");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartLongSerializer())
            .withDebugDuplicateKeys(true);
        final List<Pair<Long, Long>> entries = new ArrayList<>();
        for (long i = 0; i < 20; ++i) {
            entries.add(new Pair(i, i * i));
        }
        entries.add(new Pair(3L, 5L));
        boolean failed = false;
        String failedMessage = null;
        try {
            TableWriter.write(table, config, entries);
        } catch (final IllegalArgumentException e) {
            failed = true;
            failedMessage = e.getMessage();
        }
        assertTrue(failed);
        assertEquals("Found duplicate key: [0, 0, 0, 0, 0, 0, 0, 3]: 3 (9) == 3 (5)", failedMessage);
    }

    @Test
    public void testNullKeys() throws Exception {
        final File table = new File(tmpDir, "nullkeys");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartLongSerializer());
        final List<Pair<Long, Long>> entries = new ArrayList<>();
        for (long i = 0; i < 20; ++i) {
            entries.add(new Pair(i, i * i));
        }
        entries.add(new Pair(null, 5L));
        boolean failed = false;
        try {
            TableWriter.write(table, config, entries);
        } catch (final IllegalArgumentException e) {
            failed = true;
        }
        assertTrue(failed);
    }

    @Test
    public void testNullValues() throws Exception {
        final File table = new File(tmpDir, "nullvalues");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartLongSerializer());
        final List<Pair<Long, Long>> entries = new ArrayList<>();
        for (long i = 0; i < 20; ++i) {
            entries.add(new Pair(i, i * i));
        }
        entries.add(new Pair(20L, null));
        boolean failed = false;
        try {
            TableWriter.write(table, config, entries);
        } catch (final IllegalArgumentException e) {
            failed = true;
        }
        assertTrue(failed);
    }

    @Test
    public void testUnusedNullValues() throws Exception {
        final File table = new File(tmpDir, "unusednullvalues");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer());
        final Set<Pair<Long, Long>> entries = new HashSet<>();
        for (long i = 0; i < 20; ++i) {
            entries.add(new Pair(i, null));
        }
        TableWriter.write(table, config, entries);
        try (final TableReader<Long, Long> reader = TableReader.open(table)) {
            final Set<Pair<Long, Long>> extracted = new HashSet<>();
            for (final Pair<Long, Long> e : reader) {
                extracted.add(e);
            }
            assertEquals(entries, extracted);
        }
    }

    @Test
    public void testReadClosed() throws Exception {
        final File table = new File(tmpDir, "closed");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartVLongSerializer());
        final Set<Pair<Long, Long>> entries = new HashSet<>();
        for (long i = 0; i < 20; ++i) {
            entries.add(new Pair(i, i * i));
        }
        TableWriter.write(table, config, entries);
        try (final TableReader<Long, Long> reader = TableReader.open(table)) {
            for (long i = 0; i < 20; ++i) {
                assertEquals(new Long(i * i), reader.get(i));
            }
            assertEquals(null, reader.get(21L));
            final Set<Pair<Long, Long>> extracted = new HashSet<>();
            for (final Pair<Long, Long> e : reader) {
                extracted.add(e);
            }
            assertEquals(entries, extracted);
            reader.close();
            boolean failed = false;
            try {
                reader.get(0L);
            } catch (final IOException e) {
                failed = true;
            }
            assertTrue(failed);
        }
    }

    @Test
    public void testWriteWithTempStorage() throws Exception {
        final File table = new File(tmpDir, "tablefromtemp");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartLongSerializer());
        final Set<Pair<Long, Long>> entries = new HashSet<>();
        for (long i = 0; i < 20; ++i) {
            entries.add(new Pair(i, i * i));
        }
        TableWriter.writeWithTempStorage(table, config, entries.iterator());
        try (final TableReader<Long, Long> reader = TableReader.open(table)) {
            for (long i = 0; i < 20; ++i) {
                assertEquals(new Long(i * i), reader.get(i));
            }
            assertEquals(null, reader.get(21L));
        }
    }

    @Test
    public void testWriteFixed() throws Exception {
        final File fixedTable = new File(tmpDir, "fixed");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartLongSerializer());
        final Set<Pair<Long, Long>> entries = new HashSet<>();
        for (long i = 0; i < 20; ++i) {
            entries.add(new Pair(i, i * i));
        }
        TableWriter.write(fixedTable, config, entries);
        try (final TableReader<Long, Long> reader = TableReader.open(fixedTable)) {
            assertEquals(TableConfig.OffsetStorage.FIXED, reader.getConfig().getOffsetStorage());
            for (long i = 0; i < 20; ++i) {
                assertEquals(new Long(i * i), reader.get(i));
            }
            assertEquals(null, reader.get(21L));
            final Set<Pair<Long, Long>> extracted = new HashSet<>();
            for (final Pair<Long, Long> e : reader) {
                extracted.add(e);
            }
            assertEquals(entries, extracted);
        }
    }

    @Test
    public void testGetForHash() throws Exception {
        final File table = new File(tmpDir, "hashed");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartLongSerializer());
        final Set<Pair<Long, Long>> entries = new HashSet<>();
        for (long i = 0; i < 20; ++i) {
            entries.add(new Pair(i, i * i));
        }
        TableWriter.write(table, config, entries);
        try (final TableReader<Long, Long> reader = TableReader.open(table)) {
            for (long i = 0; i < 20; ++i) {
                assertEquals(new Long(i * i), reader.getForHash(reader.getHash(i)));
            }
            assertEquals(null, reader.getForHash(-1));
            assertEquals(null, reader.getForHash(42));
        }
    }

    @Test
    public void testWriteFixedBloom() throws Exception {
        final File fixedTable = new File(tmpDir, "fixedbloom");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartLongSerializer())
            .withKeyStorage(TableConfig.KeyStorage.IMPLICIT)
            .withSignatureWidth(10);
        final Set<Pair<Long, Long>> entries = new HashSet<>();
        for (long i = 0; i < 20; ++i) {
            entries.add(new Pair(i, i * i));
        }
        TableWriter.write(fixedTable, config, entries);
        try (final TableReader<Long, Long> reader = TableReader.open(fixedTable)) {
            assertEquals(TableConfig.OffsetStorage.FIXED, reader.getConfig().getOffsetStorage());
            assertEquals(TableConfig.KeyStorage.IMPLICIT, reader.getConfig().getKeyStorage());
            for (long i = 0; i < 20; ++i) {
                assertEquals(new Long(i * i), reader.get(i));
            }
            assertEquals(null, reader.get(21L));
        }
    }

    @Test
    public void testWriteCtkList() throws Exception {
        final File table = new File(tmpDir, "ctklist");
        final TableConfig<String, List<String>> config =
            new TableConfig()
            .withKeySerializer(new SmartStringSerializer())
            .withValueSerializer(new SmartListSerializer(new SmartStringSerializer()));
        final Set<Pair<String, List<String>>> entries = new HashSet<>();
        entries.add(new Pair<>("19v1aknlabqf7cm7", Arrays.<String>asList("1a6v81ne19u7qecd")));
        entries.add(new Pair<>("1al70abipbt79e42", Arrays.<String>asList("1al70aierbs918ih", "1amf3ln7451blerj")));
        entries.add(new Pair<>("14tal9hke04ig4e1", Arrays.<String>asList()));
        entries.add(new Pair<>("19tn7hlbe76f5b7i", Arrays.<String>asList("1a2mejhl47bi798j", "1agjc80c6ahbkbi2", "1amtdngkgausqa78")));
        TableWriter.write(table, config, entries);
        try (final TableReader<String, List<String>> reader = TableReader.open(table)) {
            final Set<Pair<String, List<String>>> extracted = new HashSet<>();
            for (final Pair<String, List<String>> e : reader) {
                extracted.add(e);
            }
            assertEquals(entries, extracted);
            assertEquals(null, reader.get("1a37dp50i9tpien8"));
        }
    }

    @Test
    public void testWriteDictionary() throws Exception {
        final SmartSerializer keySerializer = new SmartDictionarySerializer();
        final SmartSerializer valueSerializer = new SmartDictionarySerializer();
        {
            final TableConfig<String, String> config =
                new TableConfig().withKeySerializer(keySerializer).withValueSerializer(valueSerializer);
            final Set<Pair<String, String>> entries = new HashSet<>();
            entries.add(new Pair<>("cat", "black"));
            entries.add(new Pair<>("dog", "white"));
            entries.add(new Pair<>("elephant", "pink"));
            entries.add(new Pair<>("fox", "red"));
            entries.add(new Pair<>("gorilla", "brown"));
            final File table = new File(tmpDir, "dict1");
            TableWriter.write(table, config, entries);
            try (final TableReader<String, String> reader = TableReader.open(table)) {
                final Set<Pair<String, String>> extracted = new HashSet<>();
                for (final Pair<String, String> e : reader) {
                    extracted.add(e);
                }
                assertEquals(entries, extracted);
                assertEquals(null, reader.get("alligator"));
            }
        }
        // re-use the serializers with some new values
        {
            final TableConfig<String, String> config =
                new TableConfig().withKeySerializer(keySerializer).withValueSerializer(valueSerializer);
            final Set<Pair<String, String>> entries = new HashSet<>();
            entries.add(new Pair<>("alligator", "green"));
            entries.add(new Pair<>("baboon", "beige"));
            entries.add(new Pair<>("cat", "black"));
            entries.add(new Pair<>("dog", "white"));
            entries.add(new Pair<>("elephant", "pink"));
            entries.add(new Pair<>("fox", "red"));
            entries.add(new Pair<>("gorilla", "silver"));
            entries.add(new Pair<>("bear", "black"));
            entries.add(new Pair<>("kangaroo", "red"));
            final File table = new File(tmpDir, "dict2");
            TableWriter.write(table, config, entries);
            try (final TableReader<String, String> reader = TableReader.open(table)) {
                final Set<Pair<String, String>> extracted = new HashSet<>();
                for (final Pair<String, String> e : reader) {
                    extracted.add(e);
                }
                assertEquals(entries, extracted);
                assertEquals(null, reader.get("hawk"));
            }
        }
    }

    @Test
    public void testWriteIndexed() throws Exception {
        final File indexedTable = new File(tmpDir, "indexed");
        final TableConfig<String, String> config =
            new TableConfig()
            .withKeySerializer(new SmartStringSerializer())
            .withValueSerializer(new SmartStringSerializer());
        final String pad = "_0123456789012345678901234567890123456789";
        final Set<Pair<String, String>> entries = new HashSet<>();
        entries.add(new Pair("black", "cat" + pad));
        entries.add(new Pair("white", "dog" + pad));
        entries.add(new Pair("pink", "elephant" + pad));
        entries.add(new Pair("red", "fox" + pad));
        TableWriter.write(indexedTable, config, entries);
        try (final TableReader<String, String> reader = TableReader.open(indexedTable)) {
            assertEquals(TableConfig.OffsetStorage.INDEXED, reader.getConfig().getOffsetStorage());
            assertTrue(reader.get("black").startsWith("cat"));
            assertTrue(reader.get("white").startsWith("dog"));
            assertTrue(reader.get("pink").startsWith("elephant"));
            assertTrue(reader.get("red").startsWith("fox"));
            assertEquals(null, reader.get("purple"));
            final Set<Pair<String, String>> extracted = new HashSet<>();
            for (final Pair<String, String> e : reader) {
                extracted.add(e);
            }
            assertEquals(entries, extracted);
        }
    }

    @Test
    public void testWriteIndexedForced() throws Exception {
        final File indexedTable = new File(tmpDir, "forceindex");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartVLongSerializer())
            .withOffsetStorage(TableConfig.OffsetStorage.INDEXED);
        final Set<Pair<Long, Long>> entries = new HashSet<>();
        for (long i = 0; i < 20; ++i) {
            entries.add(new Pair(i, i * i));
        }
        TableWriter.write(indexedTable, config, entries);
        try (final TableReader<Long, Long> reader = TableReader.open(indexedTable)) {
            assertEquals(TableConfig.OffsetStorage.INDEXED, reader.getConfig().getOffsetStorage());
            for (long i = 0; i < 20; ++i) {
                assertEquals(new Long(i * i), reader.get(i));
            }
            assertEquals(null, reader.get(21L));
            final Set<Pair<Long, Long>> extracted = new HashSet<>();
            for (final Pair<Long, Long> e : reader) {
                extracted.add(e);
            }
            assertEquals(entries, extracted);
        }
    }

    @Test
    public void testWriteSelected() throws Exception {
        final File selectedTable = new File(tmpDir, "selected");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartVLongSerializer());
        final Set<Pair<Long, Long>> entries = new HashSet<>();
        for (long i = 0; i < 20; ++i) {
            entries.add(new Pair(i, i * i));
        }
        TableWriter.write(selectedTable, config, entries);
        try (final TableReader<Long, Long> reader = TableReader.open(selectedTable)) {
            assertEquals(TableConfig.OffsetStorage.SELECTED, reader.getConfig().getOffsetStorage());
            for (long i = 0; i < 20; ++i) {
                assertEquals(new Long(i * i), reader.get(i));
            }
            assertEquals(null, reader.get(21L));
            final Set<Pair<Long, Long>> extracted = new HashSet<>();
            for (final Pair<Long, Long> e : reader) {
                extracted.add(e);
            }
            assertEquals(entries, extracted);
        }
    }

    @Test
    public void testWriteSelectedAndSharded() throws Exception {
        final File selectedTable = new File(tmpDir, "selectedandsharded");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartVLongSerializer())
            .withTempShardSize(25L);
        final Set<Pair<Long, Long>> entries = new HashSet<>();
        for (long i = 0; i < 20; ++i) {
            entries.add(new Pair(i, i * i));
        }
        TableWriter.write(selectedTable, config, entries);
        try (final TableReader<Long, Long> reader = TableReader.open(selectedTable)) {
            assertEquals(TableConfig.OffsetStorage.SELECTED, reader.getConfig().getOffsetStorage());
            for (long i = 0; i < 20; ++i) {
                assertEquals(new Long(i * i), reader.get(i));
            }
            assertEquals(null, reader.get(21L));
            final Set<Pair<Long, Long>> extracted = new HashSet<>();
            for (final Pair<Long, Long> e : reader) {
                extracted.add(e);
            }
            assertEquals(entries, extracted);
        }
    }

    @Test
    public void testWriteSelectedAndMMapped() throws Exception {
        final File selectedTable = new File(tmpDir, "selectedandmmapped");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartVLongSerializer())
            .withMaxHeapUsage(1L);
        final Set<Pair<Long, Long>> entries = new HashSet<>();
        for (long i = 0; i < 20; ++i) {
            entries.add(new Pair(i, i * i));
        }
        TableWriter.write(selectedTable, config, entries);
        try (final TableReader<Long, Long> reader = TableReader.open(selectedTable)) {
            assertEquals(TableConfig.OffsetStorage.SELECTED, reader.getConfig().getOffsetStorage());
            for (long i = 0; i < 20; ++i) {
                assertEquals(new Long(i * i), reader.get(i));
            }
            assertEquals(null, reader.get(21L));
            final Set<Pair<Long, Long>> extracted = new HashSet<>();
            for (final Pair<Long, Long> e : reader) {
                extracted.add(e);
            }
            assertEquals(entries, extracted);
        }
    }

    @Test
    public void testWriteHeapData() throws Exception {
        final File heapTable = new File(tmpDir, "heapdata");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartVLongSerializer())
            .withMaxDataHeapUsage(10000000);
        final Set<Pair<Long, Long>> entries = new HashSet<>();
        for (long i = 0; i < 20; ++i) {
            entries.add(new Pair(i, i * i));
        }
        TableWriter.write(heapTable, config, entries);
        try (final TableReader<Long, Long> reader = TableReader.open(heapTable)) {
            FileUtils.deleteDirectory(heapTable);
            for (long i = 0; i < 20; ++i) {
                assertEquals(new Long(i * i), reader.get(i));
            }
            assertEquals(null, reader.get(21L));
            final Set<Pair<Long, Long>> extracted = new HashSet<>();
            for (final Pair<Long, Long> e : reader) {
                extracted.add(e);
            }
            assertEquals(entries, extracted);
        }
    }

    @Test
    public void testWriteMMapDataLoadAsHeap() throws Exception {
        final File heapTable = new File(tmpDir, "heapdata2");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartVLongSerializer());
        final Set<Pair<Long, Long>> entries = new HashSet<>();
        for (long i = 0; i < 20; ++i) {
            entries.add(new Pair(i, i * i));
        }
        TableWriter.write(heapTable, config, entries);
        try (final TableReader<Long, Long> reader = TableReader.open(heapTable, 10000000L)) {
            FileUtils.deleteDirectory(heapTable);
            for (long i = 0; i < 20; ++i) {
                assertEquals(new Long(i * i), reader.get(i));
            }
            assertEquals(null, reader.get(21L));
            final Set<Pair<Long, Long>> extracted = new HashSet<>();
            for (final Pair<Long, Long> e : reader) {
                extracted.add(e);
            }
            assertEquals(entries, extracted);
        }
    }

    @Test
    public void testMain() throws Exception {
        final File outputDir = new File(tmpDir, "main");
        TableWriter.main(
                new String[]{
                    "--valueSerializer", ".SmartListSerializer(.SmartIntegerSerializer, 5, \":\")",
                    outputDir.getPath(),
                    "src/test/input/nums.tsv"});
        try (final TableReader<String, List<Integer>> reader = TableReader.open(outputDir)) {
            assertEquals(ImmutableList.of(1, 2, 3), reader.get("foo"));
            assertEquals(ImmutableList.of(5, 7, 11), reader.get("bar"));
            assertEquals(ImmutableList.of(8, 8, 8, 8), reader.get("mumble"));
            assertEquals(null, reader.get("frotz"));
        }
    }

    @Ignore("requires >10G disk space and takes >10 minutes to run")
    @Test
    public void test4GigBarrier() throws Exception {
        final File table = new File(tmpDir, "4gigs");
        final TableConfig<Long, String> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartStringSerializer());
        // 200 million entries which generates a data.bin of 4.6G
        TableWriter.write(table, config, new LongToStringRangeIterable(0L, 200000000L, "number-"));
        try (final TableReader<Long, String> reader = TableReader.open(table)) {
            for (long i = 0; i < 2000; ++i) {
                assertEquals("number-" + i, reader.get(i));
            }
            assertEquals("number-199999998", reader.get(199999998L));
            assertEquals("number-199999999", reader.get(199999999L));
            assertEquals(null, reader.get(200000000L));
        }
    }

    @Ignore("requires >10G disk space and takes >10 minutes to run")
    @Test
    public void test4GigBarrierWithTempStorage() throws Exception {
        final File table = new File(tmpDir, "4gigstemp");
        final TableConfig<Long, String> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartStringSerializer());
        TableWriter.writeWithTempStorage(table, config, new LongToStringRangeIterator(0L, 200000000L, "number-"));
        try (final TableReader<Long, String> reader = TableReader.open(table)) {
            for (long i = 0; i < 2000; ++i) {
                assertEquals("number-" + i, reader.get(i));
            }
            assertEquals("number-199999998", reader.get(199999998L));
            assertEquals("number-199999999", reader.get(199999999L));
            assertEquals(null, reader.get(200000000L));
        }
    }

    @Ignore("requires more disk than god and infinite patience")
    @Test
    public void testIntMaxBarrier() throws Exception {
        final File table = new File(tmpDir, "intmax");
        final TableConfig<Long, Byte> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartByteSerializer())
            .withKeyStorage(TableConfig.KeyStorage.IMPLICIT)
            .withTempShardSize(5000000L);
        final Long end = Integer.valueOf(Integer.MAX_VALUE).longValue() + 2L;
        TableWriter.write(table, config, new LongToByteRangeIterable(0L, end));
        try (final TableReader<Long, Byte> reader = TableReader.open(table)) {
            for (Long i = 0L; i < 20000L; ++i) {
                assertEquals(i.byteValue(), (byte) reader.get(i));
            }
            assertEquals(Long.valueOf(end - 2L).byteValue(), (byte) reader.get(end - 2L));
            assertEquals(Long.valueOf(end - 1L).byteValue(), (byte) reader.get(end - 1L));
        }
    }

    public static class LongToStringRangeIterable implements Iterable<Pair<Long, String>> {
        private final long start;
        private final long end;
        private final String prefix;
        public LongToStringRangeIterable(final long start, final long end, final String prefix) {
            this.start = start;
            this.end = end;
            this.prefix = prefix;
        }
        @Override
        public Iterator<Pair<Long, String>> iterator() {
            return new LongToStringRangeIterator(start, end, prefix);
        }
    }

    public static class LongToStringRangeIterator implements Iterator<Pair<Long, String>> {
        private final String prefix;
        private final long end;
        private long i;
        public LongToStringRangeIterator(final long start, final long end, final String prefix) {
            this.i = start;
            this.end = end;
            this.prefix = prefix;
        }
        @Override
        public boolean hasNext() { return i < end; }
        @Override
        public Pair<Long, String> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final long key = i++;
            return new Pair<>(key, prefix + key);
        }
        @Override
        public void remove() { throw new UnsupportedOperationException(); }
    }

    public static class LongToByteRangeIterable implements Iterable<Pair<Long, Byte>> {
        private final long start;
        private final long end;
        public LongToByteRangeIterable(final long start, final long end) {
            this.start = start;
            this.end = end;
        }
        @Override
        public Iterator<Pair<Long, Byte>> iterator() {
            return new LongToByteRangeIterator(start, end);
        }
    }

    public static class LongToByteRangeIterator implements Iterator<Pair<Long, Byte>> {
        private final long end;
        private long i;
        public LongToByteRangeIterator(final long start, final long end) {
            this.i = start;
            this.end = end;
        }
        @Override
        public boolean hasNext() { return i < end; }
        @Override
        public Pair<Long, Byte> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final Long key = i++;
            return new Pair<>(key, key.byteValue());
        }
        @Override
        public void remove() { throw new UnsupportedOperationException(); }
    }
}
