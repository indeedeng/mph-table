package com.indeed.mph.serializers;

import com.indeed.mph.SmartSerializer;
import org.junit.Test;

import static com.indeed.mph.helpers.RoundTripHelpers.roundTrip;
import static org.junit.Assert.assertEquals;

public class TestSmartFloatFixed8Serializer {
    private static final double EPSILON = 1e-6;

    @Test
    public void testDefault() throws Exception {
        final SmartSerializer<Float> serializer = new SmartFloatFixed8Serializer();
        assertEquals(0.0, (double) roundTrip(serializer, Float.MIN_VALUE), EPSILON);
        assertEquals(0.0, (double) roundTrip(serializer, -1.0f), EPSILON);
        assertEquals(0.0, (double) roundTrip(serializer, 0.0f), EPSILON);
        assertEquals(0.0, (double) roundTrip(serializer, 0.001f), EPSILON);
        assertEquals(0.01171875, (double) roundTrip(serializer, 0.01f), EPSILON);
        assertEquals(0.5, (double) roundTrip(serializer, 0.5f), EPSILON);
        assertEquals(0.75, (double) roundTrip(serializer, 0.75f), EPSILON);
        assertEquals(0.99609375, (double) roundTrip(serializer, 1.0f), EPSILON);
        assertEquals(0.99609375, (double) roundTrip(serializer, 1.5f), EPSILON);
        assertEquals(0.99609375, (double) roundTrip(serializer, Float.MAX_VALUE), EPSILON);
    }

    @Test
    public void testCustom() throws Exception {
        final SmartSerializer<Float> serializer = new SmartFloatFixed8Serializer(-5.1f, 123000.0f);
        assertEquals(-5.1, (double) roundTrip(serializer, Float.MIN_VALUE), EPSILON);
        assertEquals(-5.1, (double) roundTrip(serializer, -5.1f), EPSILON);
        assertEquals(-5.1, (double) roundTrip(serializer, 0.0f), EPSILON);
        assertEquals(955.8773803710938, (double) roundTrip(serializer, 1000.0f), EPSILON);
        assertEquals(49965.72265625, (double) roundTrip(serializer, 50000.0f), EPSILON);
        assertEquals(122519.5078125, (double) roundTrip(serializer, 123000.0f), EPSILON);
        assertEquals(122519.5078125, (double) roundTrip(serializer, Float.MAX_VALUE), EPSILON);
    }
}
