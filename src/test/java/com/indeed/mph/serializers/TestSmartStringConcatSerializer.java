package com.indeed.mph.serializers;

import org.junit.Test;

import java.io.IOException;

import static com.indeed.mph.serializers.TestSmartDictionarySerializer.assertRoundTrip;
import static com.indeed.mph.serializers.TestSmartDictionarySerializer.assertSerializerError;

/**
 * @author xinjianz
 */
public class TestSmartStringConcatSerializer {
    @Test
    public void testDelimiter() throws IOException {
        final SmartStringConcatSerializer serializer = new SmartStringConcatSerializer(new SmartStringSerializer(),
                new SmartDictionarySerializer(), " ");
        assertRoundTrip(serializer, "gfkv teacher");
        assertRoundTrip(serializer, "gbsu software engineer");
        assertSerializerError(serializer, "gbsusoftwareengineer");
    }

    @Test
    public void testSplitIndex() throws IOException {
        final SmartStringConcatSerializer serializer = new SmartStringConcatSerializer(new SmartStringSerializer(),
                new SmartStringSerializer(), 4);
        assertRoundTrip(serializer, "gfkv teacher");
        assertRoundTrip(serializer, "gbsu software engineer");
        assertRoundTrip(serializer, "gbsu");
        assertSerializerError(serializer, "gbs");
    }

}
