package com.indeed.mph.serializers;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.indeed.mph.helpers.RoundTripHelpers.assertRoundTrip;

@RunWith(JUnitQuickcheck.class)
public class TestSmartLongSerializer {
    @Property
    public void canRoundTripLongs(final long target) throws IOException {
        final SmartLongSerializer serializer = new SmartLongSerializer();
        assertRoundTrip(serializer, target);
    }
}
