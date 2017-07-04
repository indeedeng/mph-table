package com.indeed.mph;

import com.indeed.mph.serializers.SmartLongSerializer;
import com.indeed.mph.serializers.SmartVLongSerializer;
import com.indeed.util.core.Pair;
import com.indeed.util.core.reference.SharedReference;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSharedTableReader {

    File tmpDir;
    File table;

    @Before
    public void setUp() throws Exception {
        tmpDir = File.createTempFile("tmptablewriter", "", new File("."));
        tmpDir.delete();
        tmpDir.mkdirs();
        table = new File(tmpDir, "table");
        final TableConfig<Long, Long> config =
            new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartVLongSerializer());
        final Set<Pair<Long, Long>> entries = new HashSet<>();
        for (long i = 0; i < 20; ++i) {
            entries.add(new Pair(i, i * i));
        }
        TableWriter.write(table, config, entries);
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(tmpDir);
    }

    @Test
    public void testBasic() throws Exception {
        try (final SharedTableReader<Long, Long> reader = SharedTableReader.open(table)) {
            for (long i = 0; i < 20; ++i) {
                assertEquals(new Long(i * i), reader.get(i));
            }
            assertEquals(null, reader.get(21L));
        }
    }

    @Test
    public void testReferences() throws Exception {
        try (final SharedTableReader<Long, Long> reader = SharedTableReader.open(table)) {
            try (final SharedReference<TableReader<Long, Long>> readerCopy = reader.getCopy()) {
                for (long i = 0; i < 20; ++i) {
                    assertEquals(new Long(i * i), readerCopy.get().get(i));
                }
                assertEquals(null, readerCopy.get().get(21L));
            }
            for (long i = 0; i < 20; ++i) {
                assertEquals(new Long(i * i), reader.get(i));
            }
            assertEquals(null, reader.get(21L));
        }
    }

    @Test
    public void testReadClosed() throws Exception {
        try (final SharedTableReader<Long, Long> reader = SharedTableReader.open(table)) {
            for (long i = 0; i < 20; ++i) {
                assertEquals(new Long(i * i), reader.get(i));
            }
            assertEquals(null, reader.get(21L));
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
}
