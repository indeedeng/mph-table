package com.indeed.mph.serializers;

import com.indeed.mph.LinearDiophantineEquation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A float compressed lossily into a single byte using an IEEE-754
 * 8-bit floating point representation with 1 sign bit, 5 exponent
 * bits and 2 significand bits (plus a hidden bit).  There is no
 * representation for NaNs or infinities - the largest values are
 * +/-458752.0f.
 *
 * @author alexs
 */
public class SmartFloatCompressed8Serializer extends AbstractSmartFloatSerializer {
    private static final LinearDiophantineEquation ONE = LinearDiophantineEquation.constantValue(1L);
    private static final long serialVersionUID = -2715255390933029204L;

    @Override
    public void write(final Float n, final DataOutput out) throws IOException {
        out.writeByte(floatToByte(n));
    }

    @Override
    public Float read(final DataInput in) throws IOException {
        return byteToFloat(in.readByte());
    }

    @Override
    public long sizeOf(final Float n) throws IOException {
        return 1L;
    }

    @Override
    public LinearDiophantineEquation size() {
        return ONE;
    }

    private static float computeByteToFloat(final byte n) {
        final int sign = (n < 0) ? -1 : 1;
        final int exponent = (n >>> 2) & 31;
        final int m = n & 3;
        // mantissa has implicit most significant hidden bit when exponent is non-zero
        final float v = (float) Math.pow(2.0f, (float) exponent - 15) * (float) ((exponent == 0) ? m : (m + 4));
        return sign * v;
    }

    public static float byteToFloat(final byte s) {
        return FLOAT_ENCODING_MAPPING.encodingToFloat(s);
    }

    public static byte floatToByte(final float v) {
        return (byte) FLOAT_ENCODING_MAPPING.findClosestEncoding(v);
    }

    private static final FloatEncodingMapping FLOAT_ENCODING_MAPPING;

    static {
        final float[] computedFloats = new float[128];
        for (int b = 0; b < computedFloats.length; ++b) {
            computedFloats[b] = computeByteToFloat((byte) b);
        }
        FLOAT_ENCODING_MAPPING = new FloatEncodingMapping(computedFloats);
    }
}
