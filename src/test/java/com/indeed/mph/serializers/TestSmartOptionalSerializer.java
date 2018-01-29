package com.indeed.mph.serializers;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Optional;

import static com.indeed.mph.helpers.RoundTripHelpers.assertRoundTrip;

@RunWith(JUnitQuickcheck.class)
public class TestSmartOptionalSerializer {
    @Test
    public void canRoundTripEmptyOptionals() throws IOException {
        final SmartOptionalSerializer<Long> serializer = new SmartOptionalSerializer<>(new SmartLongSerializer());
        assertRoundTrip(serializer, Optional.empty());
    }

    @Property
    public void canRoundTripPresentOptionals(final long target) throws IOException {
        final SmartOptionalSerializer<Long> serializer = new SmartOptionalSerializer<>(new SmartLongSerializer());
        assertRoundTrip(serializer, Optional.of(target));
    }
}
