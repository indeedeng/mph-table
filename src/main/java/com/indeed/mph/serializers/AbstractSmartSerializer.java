package com.indeed.mph.serializers;

import com.indeed.mph.SmartSerializer;
import com.indeed.mph.LinearDiophantineEquation;
import com.indeed.mph.NullOutputStream;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author alexs
 */
public abstract class AbstractSmartSerializer<T> extends AbstractParseable<T> implements SmartSerializer<T> {
    private static final long serialVersionUID = 5010196727600564999L;

    public void skip(final DataInput in, final int n) throws IOException {
        for (int i = 0; i < n; ++i) {
            read(in);
        }
    }

    public long sizeOf(final T t) throws IOException {
        final LinearDiophantineEquation eq = size();
        if (eq != null && eq.isConstant()) {
            return eq.apply(0);
        }
        final NullOutputStream nullOut = new NullOutputStream();
        final DataOutputStream dataOut = new DataOutputStream(nullOut);
        write(t, dataOut);
        return nullOut.getCount();
    }

    public LinearDiophantineEquation size() {
        return null;
    }
}
