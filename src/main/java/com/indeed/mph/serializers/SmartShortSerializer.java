package com.indeed.mph.serializers;

import com.indeed.mph.LinearDiophantineEquation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author maxtan
 */
public class SmartShortSerializer extends AbstractSmartSerializer<Short> {
    
    private static final LinearDiophantineEquation TWO = LinearDiophantineEquation.constantValue(2L);
    private static final long serialVersionUID = 9182692712983932531L;

    @Override
    public Short parseFromString(final String s) throws IOException {
        return Short.parseShort(s);
    }

    @Override
    public void write(final Short s, final DataOutput out) throws IOException {
        out.writeShort(s);
    }

    @Override
    public Short read(final DataInput in) throws IOException {
        return in.readShort();
    }

    @Override
    public long sizeOf(final Short s) throws IOException {
        return 2;
    }

    @Override
    public LinearDiophantineEquation size() {
        return TWO;
    }
}
