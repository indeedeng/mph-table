package com.indeed.mph.serializers;

import com.indeed.mph.LinearDiophantineEquation;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author alexs
 */
public class SmartLongSerializer extends AbstractSmartLongSerializer {
    private static final LinearDiophantineEquation EIGHT = LinearDiophantineEquation.constantValue(8L);
    private static final long serialVersionUID = 1325753019;

    @Override
    public void write(@Nonnull final Long n, final DataOutput out) throws IOException {
        out.writeLong(n);
    }

    @Override
    public Long read(final DataInput in) throws IOException {
        return in.readLong();
    }

    @Override
    public long sizeOf(final Long n) throws IOException {
        return 8;
    }

    @Override
    public LinearDiophantineEquation size() {
        return EIGHT;
    }
}
