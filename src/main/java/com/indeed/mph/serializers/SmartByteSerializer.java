package com.indeed.mph.serializers;

import com.indeed.mph.LinearDiophantineEquation;
import com.indeed.mph.SmartSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author alexs
 */
public class SmartByteSerializer extends AbstractSmartSerializer<Byte> {
    private static final LinearDiophantineEquation ONE = LinearDiophantineEquation.constantValue(1L);
    private static final long serialVersionUID = -3054402960129173227L;

    @Override
    public Byte parseFromString(final String s) throws IOException {
        return Byte.parseByte(s);
    }

    @Override
    public void write(final Byte b, final DataOutput out) throws IOException {
        out.writeByte(b);
    }

    @Override
    public Byte read(final DataInput in) throws IOException {
        return in.readByte();
    }

    @Override
    public long sizeOf(final Byte b) throws IOException {
        return 1;
    }

    @Override
    public LinearDiophantineEquation size() {
        return ONE;
    }
}
