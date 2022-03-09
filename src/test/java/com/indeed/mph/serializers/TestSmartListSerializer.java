package com.indeed.mph.serializers;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.runner.RunWith;
import com.indeed.mph.generators.IntListGenerator;
import java.io.IOException;
import java.util.List;

import static com.indeed.mph.helpers.RoundTripHelpers.assertRoundTrip;

@RunWith(JUnitQuickcheck.class)
public class TestSmartListSerializer {
    @Property
    public void canRoundTripSerializableLists(
            final List<Integer> intTarget,
            final List<Byte> byteTarget,
            final List<String> stringTarget
    ) throws IOException {
        final SmartListSerializer<Integer> intsSerializer = new SmartListSerializer<>(new SmartIntegerSerializer());
        assertRoundTrip(intsSerializer, intTarget);

        final SmartListSerializer<Byte> bytesSerializer = new SmartListSerializer<>(new SmartByteSerializer());
        assertRoundTrip(bytesSerializer, byteTarget);

        final SmartListSerializer<String> stringsSerializer = new SmartListSerializer<>(new SmartStringSerializer());
        assertRoundTrip(stringsSerializer, stringTarget);
    }
    @Property
    public void canRoundTripSerializableListsWithGenerator(
            @From(IntListGenerator.class) final List<Integer> intTarget,
            final List<Byte> byteTarget,
            final List<String> stringTarget
    ) throws IOException {
        final SmartListSerializer<Integer> intsSerializer = new SmartListSerializer<>(new SmartIntegerSerializer());
        assertRoundTrip(intsSerializer, intTarget);

        final SmartListSerializer<Byte> bytesSerializer = new SmartListSerializer<>(new SmartByteSerializer());
        assertRoundTrip(bytesSerializer, byteTarget);

        final SmartListSerializer<String> stringsSerializer = new SmartListSerializer<>(new SmartStringSerializer());
        assertRoundTrip(stringsSerializer, stringTarget);
    }
}

