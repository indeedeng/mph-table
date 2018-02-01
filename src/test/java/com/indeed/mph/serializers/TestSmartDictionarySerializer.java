package com.indeed.mph.serializers;

import org.junit.Test;

import static com.indeed.mph.helpers.RoundTripHelpers.assertRoundTrip;


public class TestSmartDictionarySerializer {
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
