package com.indeed.mph.serializers;

import com.indeed.mph.SmartSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

/**
 * Generic serializer for an Optional object using another serializer.
 * In general, it is recommended not to serialize absent values,
 * if possible.
 *
 * Please note that in the case where an absent value is meaningful,
 * {@link com.indeed.mph.serializers.SmartOptionalSerializer#write(Optional, DataOutput)}
 * will add one byte to the serialized value when present and
 * encode an absent value as a single byte.
 * @author dpassen
 */
public class SmartOptionalSerializer<T> extends AbstractSmartSerializer<Optional<T>> {

    private static final byte PRESENCE = 0x1;
    private static final byte ABSENCE = 0x0;

    private final SmartSerializer<T> serializer;

    public SmartOptionalSerializer(final SmartSerializer<T> serializer) {
        this.serializer = serializer;
    }

    @Override
    public void write(final Optional<T> o, final DataOutput out) throws IOException {
        if (o.isPresent()) {
            out.write(PRESENCE);
            serializer.write(o.get(), out);
        } else {
            out.write(ABSENCE);
        }
    }

    @Override
    public Optional<T> read(final DataInput in) throws IOException {
        return PRESENCE == in.readByte()
                ? Optional.of(serializer.read(in))
                : Optional.empty();
    }
}
