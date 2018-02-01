package com.indeed.mph.serializers;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.indeed.mph.helpers.RoundTripHelpers.assertRoundTrip;

@RunWith(JUnitQuickcheck.class)
public class TestSmartIntegerSerializer {
    @Property
    public void canRoundTripIntegers(final int target) throws IOException {
        final SmartIntegerSerializer serializer = new SmartIntegerSerializer();
        assertRoundTrip(serializer, target);
    }
}
