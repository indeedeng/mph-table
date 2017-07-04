package com.indeed.mph.serializers;

import com.indeed.mph.SmartSerializer;
import com.indeed.util.core.Pair;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestSmartDictionarySerializer {

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

    public static <T> void assertParseError(final SmartSerializer<T> serializer, final String s) {
        boolean failed = false;
        try {
            serializer.parseFromString(s);
        } catch (final Exception e) {
            failed = true;
        }
        assertTrue(failed);
    }

    @Test
    public void testRoundTrip() throws Exception {
        final SmartDictionarySerializer serializer = new SmartDictionarySerializer();
        final SmartDictionarySerializer valueSerializer = new SmartDictionarySerializer(true);
        for (final String word : new String[]{
                "cat", "dog", "elephant", "fox", "gorilla", "dog", "dog", "fox"}) {
            assertRoundTrip(serializer, word);
            assertRoundTrip(valueSerializer, word);
        }
    }
}
