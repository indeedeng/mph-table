package com.indeed.mph.serializers;

import com.google.common.base.Charsets;
import com.indeed.mph.LinearDiophantineEquation;
import com.indeed.mph.SmartSerializer;
import com.indeed.util.serialization.LengthVIntSerializer;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author alexs
 */
public class SmartStringSerializer extends AbstractSmartSerializer<String> {
    private static final LengthVIntSerializer lengthSerializer = new LengthVIntSerializer();
    private static final LinearDiophantineEquation ONE_PLUS = LinearDiophantineEquation.slopeIntercept(1L, 1L);
    private static final long serialVersionUID = 1080137424;

    @Override
    public String parseFromString(final String s) throws IOException {
        return s;
    }

    @Override
    public void write(@Nonnull final String s, final DataOutput out) throws IOException {
        final byte[] bytes = s.getBytes(Charsets.UTF_8);
        lengthSerializer.write(bytes.length, out);
        out.write(bytes);
    }

    @Override
    public String read(final DataInput in) throws IOException {
        final int len = lengthSerializer.read(in);
        final byte[] bytes = new byte[len];
        in.readFully(bytes);
        return new String(bytes, Charsets.UTF_8);
    }

    @Override
    public long sizeOf(final String s) throws IOException {
        final int len = s.getBytes(Charsets.UTF_8).length;
        return len < 0xFF ? len + 1 : len + 5;
    }

    @Override
    public LinearDiophantineEquation size() {
        return ONE_PLUS;
    }
}
