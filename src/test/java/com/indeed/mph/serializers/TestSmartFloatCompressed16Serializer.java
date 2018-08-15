package com.indeed.mph.serializers;

import com.indeed.mph.SmartSerializer;
import org.junit.Test;

import static com.indeed.mph.helpers.RoundTripHelpers.roundTrip;
import static org.junit.Assert.assertEquals;

public class TestSmartFloatCompressed16Serializer {
    private static final double EPSILON = 1e-5;

    @Test
    public void testRoundTrip() throws Exception {
        final SmartSerializer<Float> serializer = new SmartFloatCompressed16Serializer();
        assertEquals(0.0f, (double) roundTrip(serializer, Float.MIN_VALUE), EPSILON);
        assertEquals(-1.0f, (double) roundTrip(serializer, -1.0f), EPSILON);
        assertEquals(0.0f, (double) roundTrip(serializer, 0.0f), EPSILON);
        assertEquals(0.001f, (double) roundTrip(serializer, 0.001f), EPSILON);
        assertEquals(0.01f, (double) roundTrip(serializer, 0.01f), EPSILON);
        assertEquals(0.5f, (double) roundTrip(serializer, 0.5f), EPSILON);
        assertEquals(0.75f, (double) roundTrip(serializer, 0.75f), EPSILON);
        assertEquals(1.0f, (double) roundTrip(serializer, 1.0f), EPSILON);
        assertEquals(1.5f, (double) roundTrip(serializer, 1.5f), EPSILON);
        assertEquals(Float.POSITIVE_INFINITY, (double) roundTrip(serializer, Float.MAX_VALUE), EPSILON);
        assertEquals(Float.NEGATIVE_INFINITY, (double) roundTrip(serializer, -Float.MAX_VALUE), EPSILON);
    }
}
