package com.indeed.mph.serializers;

import com.indeed.mph.LinearDiophantineEquation;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** A compressed float in one byte, clamping values to a range and
 * dividing into 256 equal sized buckets within that range.  Default
 * range is [0, 1].
 *
 * @author alexs
 */
public class SmartFloatFixed8Serializer extends AbstractSmartFloatSerializer {
    private static final LinearDiophantineEquation ONE = LinearDiophantineEquation.constantValue(1L);
    private static final long serialVersionUID = -1199488992867983413L;
    private final float minValue;
    private final float maxValue;
    private final float bucketSize;

    public SmartFloatFixed8Serializer(final float minValue, final float maxValue) {
        if (minValue >= maxValue) {
            throw new IllegalArgumentException("range must be non-empty: " + minValue + " >= " + maxValue);
        }
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.bucketSize = (maxValue - minValue) / 256.0f;
    }

    public SmartFloatFixed8Serializer() {
        this(0.0f, 1.0f);
    }

    @Override
    public void write(@Nonnull final Float v, final DataOutput out) throws IOException {
        out.writeByte(floatToByte(v));
    }

    @Override
    public Float read(final DataInput in) throws IOException {
        return byteToFloat(in.readByte());
    }

    public byte floatToByte(final float v) {
        //final float u = v < minValue ? minValue : v > maxValue ? maxValue : v;
        final float u = v;
        final int n = (int) Math.round((u - minValue) / bucketSize) - 128;
        final int m = n < -128 ? -128 : n > 127 ? 127 : n;
        return (byte) m;
    }

    public float byteToFloat(final byte b) {
        float u = ((float) b) + 128.0f;
        return u * bucketSize + minValue;
    }

    @Override
    public long sizeOf(final Float n) throws IOException {
        return 1L;
    }

    @Override
    public LinearDiophantineEquation size() {
        return ONE;
    }
}
