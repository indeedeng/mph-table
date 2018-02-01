package com.indeed.mph.serializers;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.indeed.mph.helpers.RoundTripHelpers.assertRoundTrip;

@RunWith(JUnitQuickcheck.class)
public class TestSmartStringSerializer {
    @Property
    public void canRoundTripStrings(final String target) throws IOException {
        SmartStringSerializer serializer = new SmartStringSerializer();
        assertRoundTrip(serializer, target);
    }
}
