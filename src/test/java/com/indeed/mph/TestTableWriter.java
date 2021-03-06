package com.indeed.mph;

import com.google.common.collect.ImmutableList;
import com.indeed.mph.serializers.AbstractSmartSerializer;
import com.indeed.mph.serializers.SmartBase32Serializer;
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

import java.io.DataInput;
import java.io.DataOutput;
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
    public void testWriteFixedImplicit() throws Exception {
        final File fixedTable = new File(tmpDir, "fixedimplicit");
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
    public void testWriteFixedImplicitWithRangeChecks() throws Exception {
        final File fixedTable = new File(tmpDir, "fixedrangechecks");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartLongSerializer())
            .withKeyStorage(TableConfig.KeyStorage.IMPLICIT)
            .withRangeChecking(TableConfig.RangeChecking.MIN_AND_MAX)
            .withSignatureWidth(3);
        final Set<Pair<Long, Long>> entries = new HashSet<>();
        for (long i = 0; i < 1000; i += 2) {  // evens
            entries.add(new Pair(i, i * i));
        }
        TableWriter.write(fixedTable, config, entries);
        try (final TableReader<Long, Long> reader = TableReader.open(fixedTable)) {
            for (long i = 0; i < 1000; i += 2) {  // evens
                assertEquals(new Long(i * i), reader.get(i));
            }
            int lowFalsePositives = 0;
            int midFalsePositives = 0;
            int highFalsePositives = 0;
            for (long i = -5001; i < 5000; i += 2) {  // odds
                if (reader.get(i) != null) {
                    if (i < 0) {
                        ++lowFalsePositives;
                    } else if (i > 1000) {
                        ++highFalsePositives;
                    } else {
                        ++midFalsePositives;
                    }
                }
            }
            assertEquals(0, lowFalsePositives);
            assertTrue(midFalsePositives > 0);
            assertEquals(0, highFalsePositives);
        }
    }

    @Test
    public void testWriteFixedImplicitWithRangeChecksUnserializable() throws Exception {
        final File fixedTable = new File(tmpDir, "fixedrangechecksunserializable");
        final TableConfig<Triple<Long, Long, Long>, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartTripleSerializer(new SmartLongSerializer(), new SmartLongSerializer(), new SmartLongSerializer()))
            .withValueSerializer(new SmartLongSerializer())
            .withKeyStorage(TableConfig.KeyStorage.IMPLICIT)
            .withRangeChecking(TableConfig.RangeChecking.MIN_AND_MAX)
            .withSignatureWidth(3);
        final Set<Pair<Triple<Long, Long, Long>, Long>> entries = new HashSet<>();
        for (Long i = 0L; i < 1000; i += 2) {  // evens
            entries.add(new Pair<>(new Triple<>(i - 1, i, i + 1), i * i));
        }
        TableWriter.write(fixedTable, config, entries);
        try (final TableReader<Triple<Long, Long, Long>, Long> reader = TableReader.open(fixedTable)) {
            for (Long i = 0L; i < 1000; i += 2) {  // evens
                assertEquals(new Long(i * i), reader.get(new Triple<>(i - 1, i, i + 1)));
            }
            int lowFalsePositives = 0;
            int midFalsePositives = 0;
            int highFalsePositives = 0;
            for (Long i = -5001L; i < 5000; i += 2) {  // odds
                if (reader.get(new Triple<>(i - 1, i, i + 1)) != null) {
                    if (i < 0) {
                        ++lowFalsePositives;
                    } else if (i > 1000) {
                        ++highFalsePositives;
                    } else {
                        ++midFalsePositives;
                    }
                }
            }
            assertEquals(0, lowFalsePositives);
            assertTrue(midFalsePositives > 0);
            assertEquals(0, highFalsePositives);
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
    public void testWriteFixedCtkList() throws Exception {
        final File table = new File(tmpDir, "fixedctklist");
        final TableConfig<Long, List<String>> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartListSerializer(new SmartBase32Serializer(80), 100));
        final Set<Pair<Long, List<String>>> entries = new HashSet<>();
        entries.add(new Pair<>(-8958556657105698816L, Arrays.<String>asList("eusb7o0sj89fs07o")));
        entries.add(new Pair<>(-1404006683118469120L, Arrays.<String>asList("3ks18crbfd8r5clc")));
        entries.add(new Pair<>(-2959110065296703488L, Arrays.<String>asList("6h8tipuj542rm0r3")));
        entries.add(new Pair<>(3828756529465982976L, Arrays.<String>asList("vqgchrof6cv200d8")));
        entries.add(new Pair<>(8843920576862552064L, Arrays.<String>asList("pmirui3u00dot0jl")));
        entries.add(new Pair<>(5682617893516935168L, Arrays.<String>asList("k2ufmik8mirl70lf")));
        entries.add(new Pair<>(5285114767967322112L, Arrays.<String>asList("gdloa6tbnrtug551")));
        entries.add(new Pair<>(849131654930694144L, Arrays.<String>asList("5vnsuao32btg29pq")));
        entries.add(new Pair<>(-8209908480332464128L, Arrays.<String>asList("66v2divp6ue581vj")));
        entries.add(new Pair<>(2199589795716595712L, Arrays.<String>asList("bbua0vrgjhn210jd")));
        TableWriter.write(table, config, entries);
        try (final TableReader<Long, List<String>> reader = TableReader.open(table)) {
            for (final Pair<Long, List<String>> e : entries) {
                assertEquals(e.getSecond(), reader.get(e.getFirst()));
            }
            final Set<Pair<Long, List<String>>> extracted = new HashSet<>();
            for (final Pair<Long, List<String>> e : reader) {
                extracted.add(e);
            }
            assertEquals(entries, extracted);
            assertEquals(null, reader.get(1L));
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

    public static class Triple<A, B, C> implements Comparable<Triple<A, B, C>> {
        public A a;
        public B b;
        public C c;
        public Triple(final A a, final B b, final C c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Triple) {
                final Triple t = (Triple)obj;
                final Object a = t.a;
                final Object b = t.b;
                final Object c = t.c;
                return (a == null ? this.a == null : a.equals(this.a))
                    && (b == null ? this.b == null : b.equals(this.b))
                    && (c == null ? this.c == null : c.equals(this.c));
            }
            return false;
        }
        @Override
        public int hashCode() {
            int result;
            result = (a != null ? a.hashCode() : 0);
            result = 31 * result + (b != null ? b.hashCode() : 0);
            result = 31 * result + (c != null ? c.hashCode() : 0);
            return result;
        }
        @Override
        public String toString() {
            return "("+a+", "+b+", "+c+")";
        }
        @Override
        public int compareTo(final Triple<A, B, C> o) {
            if (o == null) {
                return 1;
            }
            if (a != null) {
                if (a instanceof Comparable) {
                    final int acmp = ((Comparable) a).compareTo(o.a);
                    if (acmp != 0) return acmp;
                }
            } else if (o.a != null) {
                return -1;
            }
            if (b != null) {
                if (b instanceof Comparable) {
                    final int bcmp = ((Comparable) b).compareTo(o.b);
                    if (bcmp != 0) return bcmp;
                }
            } else if (o.b != null) {
                return -1;
            }
            if (c != null) {
                if (c instanceof Comparable) {
                    return ((Comparable) c).compareTo(o.c);
                }
            } else if (o.c != null) {
                return -1;
            }
            return 0;
        }
    }

    public static class SmartTripleSerializer<A, B, C> extends AbstractSmartSerializer<Triple<A, B, C>> {
        private final SmartSerializer<A> serializer1;
        private final SmartSerializer<B> serializer2;
        private final SmartSerializer<C> serializer3;
        public SmartTripleSerializer(final SmartSerializer<A> serializer1,
                                     final SmartSerializer<B> serializer2,
                                     final SmartSerializer<C> serializer3) {
            this.serializer1 = serializer1;
            this.serializer2 = serializer2;
            this.serializer3 = serializer3;
        }
        @Override
        public void write(final Triple<A, B, C> triple, final DataOutput out) throws IOException {
            serializer1.write(triple.a, out);
            serializer2.write(triple.b, out);
            serializer3.write(triple.c, out);
        }
        @Override
        public Triple<A, B, C> read(final DataInput in) throws IOException {
            final A a = serializer1.read(in);
            final B b = serializer2.read(in);
            final C c = serializer3.read(in);
            return new Triple<>(a, b, c);
        }
        @Override
        public LinearDiophantineEquation size() {
            return null;
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
