package com.indeed.mph;

import com.indeed.mph.serializers.SmartLongSerializer;
import com.indeed.mph.serializers.SmartVLongSerializer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestTableConfig {
    private static final long ONE_MILLION = 1000000;
    private static final long ONE_MEGABYTE = 1024 * 1024;
    private static final long ONE_GIGABYTE = 1024 * 1024 * 1024;

    @Test
    public void testChooseBestOffsetStorage() throws Exception {
        assertEquals(TableConfig.OffsetStorage.AUTOMATIC,
                     new TableConfig().getOffsetStorage());
        assertEquals(TableConfig.OffsetStorage.FIXED,
                     new TableConfig()
                     .withKeySerializer(new SmartLongSerializer())
                     .chooseBestOffsetStorage(ONE_MILLION, 8 * ONE_MEGABYTE));
        assertEquals(TableConfig.OffsetStorage.SELECTED,
                     new TableConfig()
                     .withKeySerializer(new SmartVLongSerializer())
                     .chooseBestOffsetStorage(ONE_MILLION, 4 * ONE_MEGABYTE));
        assertEquals(TableConfig.OffsetStorage.INDEXED,
                     new TableConfig()
                     .withKeySerializer(new SmartVLongSerializer())
                     .chooseBestOffsetStorage(ONE_MILLION, ONE_GIGABYTE));
    }
}
