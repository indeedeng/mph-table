package com.indeed.mph;

import com.indeed.mph.serializers.SmartLongSerializer;
import com.indeed.mph.serializers.SmartVLongSerializer;
import com.indeed.util.core.Pair;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMphMap {

    @Test
    public void testBasic() throws Exception {
        final TableConfig<Long, Long> config = new TableConfig()
            .withKeySerializer(new SmartLongSerializer())
            .withValueSerializer(new SmartLongSerializer());
        final Map<Long, Long> entries = new HashMap<>();
        for (long i = 0; i < 20; ++i) {
            entries.put(i, i * i);
        }
        {
            final Map<Long, Long> table = MphMap.fromMap(config, entries);
            for (long i = 0; i < 20; ++i) {
                assertEquals(new Long(i * i), table.get(i));
            }
            assertEquals(null, table.get(21L));
        }
        {
            final Map<Long, Long> table = roundTrip(MphMap.fromMap(config, entries));
            for (long i = 0; i < 20; ++i) {
                assertEquals(new Long(i * i), table.get(i));
            }
            assertEquals(null, table.get(21L));
        }
        {
            final Map<Long, Long> table = new HashMap<>();
            table.putAll(roundTrip(MphMap.fromMap(config, entries)));
            assertEquals(entries, table);
        }
    }

    public static <T extends Serializable> T roundTrip(final T value) throws IOException, ClassNotFoundException {
        final ByteArrayOutputStream outBuf = new ByteArrayOutputStream();
        final ObjectOutputStream out = new ObjectOutputStream(outBuf);
        out.writeObject(value);
        final ByteArrayInputStream inBuf = new ByteArrayInputStream(outBuf.toByteArray());
        final ObjectInputStream in = new ObjectInputStream(inBuf);
        return (T) in.readObject();
    }
}
