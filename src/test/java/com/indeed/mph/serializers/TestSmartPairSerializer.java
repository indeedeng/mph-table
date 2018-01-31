package com.indeed.mph.serializers;

import com.indeed.mph.generators.PairGenerator;
import com.indeed.util.core.Pair;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.indeed.mph.helpers.RoundTripHelpers.assertRoundTrip;

@RunWith(JUnitQuickcheck.class)
public class TestSmartPairSerializer {

    @Property
    public void canRoundTripPairs(@From(PairGenerator.class)final Pair<String, Long> target) throws IOException {
        SmartPairSerializer<String, Long> serializer = new SmartPairSerializer<>(
                new SmartStringSerializer(),
                new SmartLongSerializer()
        );
        assertRoundTrip(serializer, target);
    }
}
