package com.indeed.mph.serializers;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.indeed.mph.helpers.RoundTripHelpers.assertRoundTrip;

@RunWith(JUnitQuickcheck.class)
public class TestSmartByteSerializer {
    @Property
    public void canRoundTripBytes(final byte target) throws IOException {
        final SmartByteSerializer serializer = new SmartByteSerializer();
        assertRoundTrip(serializer, target);
    }
}
