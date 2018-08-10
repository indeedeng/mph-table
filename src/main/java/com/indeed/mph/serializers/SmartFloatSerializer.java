package com.indeed.mph.serializers;

import com.indeed.mph.LinearDiophantineEquation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** A 32-bit float serialized in 4 bytes with DataOutput.writeFloat/DataInput.readFloat.
 *
 * @author alexs
 */
public class SmartFloatSerializer extends AbstractSmartFloatSerializer {
    private static final LinearDiophantineEquation FOUR = LinearDiophantineEquation.constantValue(4L);
    private static final long serialVersionUID = -2406663543198395447L;

    public SmartFloatSerializer() {
    }

    @Override
    public void write(final Float v, final DataOutput out) throws IOException {
        out.writeFloat(v);
    }

    @Override
    public Float read(final DataInput in) throws IOException {
        return in.readFloat();
    }

    @Override
    public long sizeOf(final Float n) throws IOException {
        return 4L;
    }

    @Override
    public LinearDiophantineEquation size() {
        return FOUR;
    }
}
