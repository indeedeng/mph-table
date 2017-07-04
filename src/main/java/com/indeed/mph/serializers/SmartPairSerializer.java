package com.indeed.mph.serializers;

import com.indeed.util.core.Pair;
import com.indeed.mph.LinearDiophantineEquation;
import com.indeed.mph.SmartSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *　@author xinjianz
 */
public class SmartPairSerializer<U, V> extends AbstractSmartSerializer<Pair<U, V>> {

    private static final long serialVersionUID = 1325745619L;

    private final SmartSerializer<U> serializer1;
    private final SmartSerializer<V> serializer2;

    public SmartPairSerializer(final SmartSerializer<U> serializer1,
                               final SmartSerializer<V> serializer2) {
        this.serializer1 = serializer1;
        this.serializer2 = serializer2;
    }

    @Override
    public void write(final Pair<U, V> pair, final DataOutput out) throws IOException {
        serializer1.write(pair.getFirst(), out);
        serializer2.write(pair.getSecond(), out);
    }

    @Override
    public Pair<U, V> read(final DataInput in) throws IOException {
        return new Pair<>(serializer1.read(in), serializer2.read(in));
    }

    @Override
    public LinearDiophantineEquation size() {
        if (serializer1.size() == null) {
            return null;
        } else {
            return serializer1.size().add(serializer2.size());
        }
    }
}
