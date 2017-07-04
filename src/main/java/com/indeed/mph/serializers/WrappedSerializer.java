package com.indeed.mph.serializers;

import com.indeed.mph.SmartSerializer;
import com.indeed.util.serialization.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Trivial wrapper around a Serializer with a default constructor to
 * make it Serializable.
 *
 * @author alexs
 */
public class WrappedSerializer<T> extends AbstractSmartSerializer<T> {
    private Serializer<T> serializer;

    public WrappedSerializer(final Serializer<T> serializer) {
        this.serializer = serializer;
    }

    @Override
    public void write(final T t, final DataOutput out) throws IOException {
        serializer.write(t, out);
    }

    @Override
    public T read(final DataInput in) throws IOException {
        return serializer.read(in);
    }

    private void writeObject(final ObjectOutputStream out) throws IOException {
        out.writeObject(serializer.getClass().getName());
    }

    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
        final String klassName = (String) in.readObject();
        final Class klass = Class.forName(klassName);
        try {
            final Constructor<?> konstruct = klass.getConstructor();
            serializer = (Serializer<T>) konstruct.newInstance();
        } catch (final NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IOException("can't construct wrapped serializer: " + klassName, e);
        }
    }
}
