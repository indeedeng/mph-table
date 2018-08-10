package com.indeed.mph.serializers;

import com.indeed.mph.LinearDiophantineEquation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** A 64-bit double serialized in 8 bytes with DataOutput.writeDouble/DataInput.readDouble.
 *
 * @author alexs
 */
public class SmartDoubleSerializer extends AbstractSmartDoubleSerializer {
    private static final LinearDiophantineEquation EIGHT = LinearDiophantineEquation.constantValue(8L);
    private static final long serialVersionUID = -4089805788174202404L;

    public SmartDoubleSerializer() {
    }

    @Override
    public void write(final Double v, final DataOutput out) throws IOException {
        out.writeDouble(v);
    }

    @Override
    public Double read(final DataInput in) throws IOException {
        return in.readDouble();
    }

    @Override
    public long sizeOf(final Double n) throws IOException {
        return 8L;
    }

    @Override
    public LinearDiophantineEquation size() {
        return EIGHT;
    }
}
