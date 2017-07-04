package com.indeed.mph.serializers;

import com.indeed.mph.LinearDiophantineEquation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author alexs
 */
public class SmartIntegerSerializer extends AbstractSmartSerializer<Integer> {
    private static final LinearDiophantineEquation FOUR = LinearDiophantineEquation.constantValue(4L);
    private static final long serialVersionUID = 1325753019;

    @Override
    public Integer parseFromString(final String s) throws IOException {
        return Integer.parseInt(s);
    }

    @Override
    public void write(final Integer n, final DataOutput out) throws IOException {
        out.writeInt(n);
    }

    @Override
    public Integer read(final DataInput in) throws IOException {
        return in.readInt();
    }

    @Override
    public long sizeOf(final Integer n) throws IOException {
        return 4;
    }

    @Override
    public LinearDiophantineEquation size() {
        return FOUR;
    }
}
