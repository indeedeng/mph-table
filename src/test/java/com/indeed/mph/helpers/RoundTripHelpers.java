package com.indeed.mph.helpers;

import com.indeed.mph.SmartSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RoundTripHelpers {
    private RoundTripHelpers() {
        // Intentionally left blank
    }

    public static <T> T roundTrip(final SmartSerializer<T> serializer, final T value) throws IOException {
        final ByteArrayOutputStream outBuf = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(outBuf);
        serializer.write(value, out);
        final ByteArrayInputStream inBuf = new ByteArrayInputStream(outBuf.toByteArray());
        final DataInputStream in = new DataInputStream(inBuf);
        return serializer.read(in);
    }

    public static <T> void assertRoundTrip(final SmartSerializer<T> serializer, final T value) throws IOException {
        assertEquals(value, roundTrip(serializer, value));
    }

    public static <T> void assertParseRoundTrip(final SmartSerializer<T> serializer, final String s) throws IOException {
        assertEquals(s, serializer.printToString(serializer.parseFromString(s)));
    }

    public static <T> void assertSerializerError(final SmartSerializer<T> serializer, final T value) {
        boolean failed = false;
        try {
            roundTrip(serializer, value);
        } catch (final Exception e) {
            failed = true;
        }
        assertTrue(failed);
    }
}
