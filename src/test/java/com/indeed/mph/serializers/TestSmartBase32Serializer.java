package com.indeed.mph.serializers;

import org.junit.Test;

import static com.indeed.mph.helpers.RoundTripHelpers.assertParseRoundTrip;
import static com.indeed.mph.helpers.RoundTripHelpers.assertRoundTrip;
import static com.indeed.mph.helpers.RoundTripHelpers.assertSerializerError;

public class TestSmartBase32Serializer {

    @Test
    public void testStatic() throws Exception {
        final SmartBase32Serializer serializer = new SmartBase32Serializer(80);
        // [junit]  write: 14tal9hke04ig4e1
        // hi: 00001001 00111010 10101010 10100110 00110100 01110000 00001001 00101000
        // lo: 00010001 11000001
        //     00001001 00111010 10101010 10100110 00110100 01110000 00001001 00101000
        //     00010001 11000001
        // write: i: 1 ch: 4 bits: 10 acc: 100100 byte: 1001
        // write: i: 3 ch: a bits: 12 acc: 1110101010 byte: 111010
        // write: i: 4 ch: l bits: 9 acc: 101010101 byte: 10101010
        // write: i: 6 ch: h bits: 11 acc: 10100110001 byte: 10100110
        // write: i: 7 ch: k bits: 8 acc: 110100 byte: 110100
        // write: i: 9 ch: 0 bits: 10 acc: 111000000 byte: 1110000
        // write: i: 11 ch: i bits: 12 acc: 10010010 byte: 1001
        // write: i: 12 ch: g bits: 9 acc: 1010000 byte: 101000
        // write: i: 14 ch: e bits: 11 acc: 10001110 byte: 10001
        // write: i: 15 ch: 1 bits: 8 acc: 11000001 byte: 11000001
        assertRoundTrip(serializer, "14tal9hke04ig4e1");
        assertRoundTrip(serializer, "16kkn19st0k71324");
        assertRoundTrip(serializer, "19v1aknlabqf7cm7");
        assertRoundTrip(serializer, "1ak04h8jobsplbs7");
        assertSerializerError(serializer, null);
        assertSerializerError(serializer, "");
        assertSerializerError(serializer, "abc");
        assertSerializerError(serializer, "1ak04h8j_bsplbs7");
        assertSerializerError(serializer, "1ak04h8jobsplbs78");
        assertParseRoundTrip(serializer, "1ak04h8jobsplbs7");
    }
}
