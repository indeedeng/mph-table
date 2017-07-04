package com.indeed.mph.serializers;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static com.indeed.mph.serializers.TestSmartDictionarySerializer.assertRoundTrip;

/**
 * @author xinjianz
 */
public class TestSmartSmallLongListSerializer {

    @Test
    public void testSmartSmallLongListSerializer() throws IOException {
        for (long offset = -100; offset <= 100; offset += 100) {
            final SmartSmallLongListSerializer serializer = new SmartSmallLongListSerializer(offset);
            // Test empty list.
            assertRoundTrip(serializer, new ArrayList<Long>());
            // Test singleton array of small number.
            for (long number = 0; number <= 128; ++number) {
                assertRoundTrip(serializer, Arrays.asList(number));
            }
            // Test singleton array of big number.
            assertRoundTrip(serializer, Arrays.asList(10000L));
            assertRoundTrip(serializer, Arrays.asList(Long.MAX_VALUE));
            // Test 2-elements array can be compressed
            assertRoundTrip(serializer, Arrays.asList(5L, 6L));
            // Test 2-elements array can't be compressed
            assertRoundTrip(serializer, Arrays.asList(5L, 100L));
            assertRoundTrip(serializer, Arrays.asList(1980L, 5L));
            // Test 3-elements array can be compressed.
            assertRoundTrip(serializer, Arrays.asList(1L, 2L, 3L));
            // Test 3-elements array can't be compressed.
            assertRoundTrip(serializer, Arrays.asList(0L, 5L, 3L));
            // Test size checking
            assertRoundTrip(serializer, Arrays.asList(1L, 2L, 3L, 1L));
            assertRoundTrip(serializer, Arrays.asList(1L, 2L, 3L, 1L, 2L));
            assertRoundTrip(serializer, Arrays.asList(4L, 4L, 6L));
            assertRoundTrip(serializer, Arrays.asList(4L, 4L, 6L, 6L));
            // Test big integer
            assertRoundTrip(serializer, Arrays.asList(10000000L));
            assertRoundTrip(serializer, Arrays.asList(100L, 2000L, 30000L));
            assertRoundTrip(serializer, Arrays.asList(1L, 2L, 2000L, 5L, 30000L));
            // Test negative integer
            assertRoundTrip(serializer, Arrays.asList(-1L));
            assertRoundTrip(serializer, Arrays.asList(-10000000L));
            assertRoundTrip(serializer, Arrays.asList(100L, -2000L, -30000L));
            // Test all together
            assertRoundTrip(serializer, Arrays.asList(1L, 2L, 3L, 100000L, 4L, 5L, -100L, -1000L, 6L, 7L));
        }
    }
}
