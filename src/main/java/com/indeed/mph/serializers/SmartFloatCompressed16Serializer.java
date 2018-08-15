package com.indeed.mph.serializers;

import com.indeed.mph.LinearDiophantineEquation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A float compressed lossily into two bytes using IEEE half precision.
 *
 * @author alexs
 */
public class SmartFloatCompressed16Serializer extends AbstractSmartFloatSerializer {
    private static final LinearDiophantineEquation TWO = LinearDiophantineEquation.constantValue(2L);
    private static final long serialVersionUID = -299793944804375541L;
    private static final float MIN_VALUE = (float) Math.pow(2, -24);
    private static final float MAX_VALUE = (float) Math.pow(2, 5) * (1024 + 1023);
    private static final short ENCODING_INFINITY = 31744;

    @Override
    public void write(final Float n, final DataOutput out) throws IOException {
        out.writeShort(floatToShort(n));
    }

    @Override
    public Float read(final DataInput in) throws IOException {
        return shortToFloat(in.readShort());
    }

    @Override
    public long sizeOf(final Float n) throws IOException {
        return 2L;
    }

    @Override
    public LinearDiophantineEquation size() {
        return TWO;
    }

    private static float computeShortToFloat(final short value) {
        final int sign = ((value >>> 15) == 0) ? 1 : -1;
        final int mantissa = value & 1023;
        final int exponent = (value >>> 10) & 31;
        if (exponent == 0) {
            // subnormal values for when exponent is zero
            return sign * MIN_VALUE * mantissa;
        } else if (exponent < 31) {
            // mantissa has implicit most significant hidden bit when exponent is non-zero
            return sign * ((float) Math.pow(2, exponent - 25) * (1024 + mantissa));
        } else if (mantissa == 0) {
            return sign * Float.POSITIVE_INFINITY;
        } else {
            return Float.NaN;
        }
    }

    public static float shortToFloat(final short s) {
        return FLOAT_ENCODING_MAPPING.encodingToFloat(s);
    }

    public static short floatToShort(final float v) {
        if (Float.isNaN(v)) {
            return 32767;
        } else if (v > MAX_VALUE) {
            return ENCODING_INFINITY;
        } else if (v < -MAX_VALUE) {
            return ENCODING_INFINITY - 32768;
        } else {
            return (short) FLOAT_ENCODING_MAPPING.findClosestEncoding(v);
        }
    }

    private static final FloatEncodingMapping FLOAT_ENCODING_MAPPING;

    static {
        final float[] computedFloats = new float[32768];
        for (int s = 0; s < computedFloats.length; ++s) {
            computedFloats[s] = computeShortToFloat((short) s);
        }
        FLOAT_ENCODING_MAPPING = new FloatEncodingMapping(computedFloats);
    }
}
