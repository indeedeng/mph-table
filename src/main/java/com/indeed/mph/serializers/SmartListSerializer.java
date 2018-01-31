package com.indeed.mph.serializers;

import com.indeed.mph.LinearDiophantineEquation;
import com.indeed.mph.SmartSerializer;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static com.indeed.mph.serializers.SmartVLongSerializer.readVLong;
import static com.indeed.mph.serializers.SmartVLongSerializer.writeVLong;

/**
 * Generic serializer for a list of objects using another serializer.
 * Takes an optional limit on the length of the list, above which
 * longer lists are rejected.  If the limit fits in 1 byte and the
 * element serializer is constant-sized, the size() will be optimized.
 *
 * @author alexs
 */
public class SmartListSerializer<T> extends AbstractSmartSerializer<List<T>> {
    private static final LinearDiophantineEquation ONE_PLUS =
        LinearDiophantineEquation.slopeIntercept(1L, 1L);
    private static final long serialVersionUID = 1846053469;
    private static final Pattern COMMA = Pattern.compile("\\s*,\\s*");
    private final Pattern separator;
    private final long limit;
    private final SmartSerializer<T> serializer;
    private final LinearDiophantineEquation sizeEq;
    private final boolean ignoreErrors;

    public SmartListSerializer(final SmartSerializer<T> serializer, final long limit, final Pattern separator, final boolean ignoreErrors) {
        this.serializer = serializer;
        this.limit = limit;
        this.separator = separator;
        this.ignoreErrors = ignoreErrors;
        this.sizeEq = limit > 0 && limit < 256 && serializer.size().isConstant() ?
            LinearDiophantineEquation.slopeIntercept(serializer.size().getConstant(), 1L) :
            ONE_PLUS;
    }

    public SmartListSerializer(final SmartSerializer<T> serializer, final long limit, final Pattern separator) {
        this(serializer, limit, separator, false);
    }

    public SmartListSerializer(final SmartSerializer<T> serializer, final long limit, final String separator, final boolean ignoreErrors) {
        this(serializer, limit, Pattern.compile(separator), ignoreErrors);
    }

    public SmartListSerializer(final SmartSerializer<T> serializer, final long limit, final String separator) {
        this(serializer, limit, separator, false);
    }

    public SmartListSerializer(final SmartSerializer<T> serializer, final long limit) {
        this(serializer, limit, COMMA);
    }

    public SmartListSerializer(final SmartSerializer<T> serializer) {
        this(serializer, 0L);
    }

    @Override
    public List<T> parseFromString(final String s) throws IOException {
        final String[] objectStrings = separator.split(s);
        final List<T> result = new ArrayList<>(objectStrings.length);
        if (ignoreErrors) {
            for (final String objStr : objectStrings) {
                try {
                    final T obj = serializer.parseFromString(objStr);
                    result.add(obj);
                } catch (final Exception e) { // ignore
                }
            }
        } else {
            for (final String objStr : objectStrings) {
                final T obj = serializer.parseFromString(objStr);
                result.add(obj);
            }
        }
        if (limit > 0 && result.size() > limit) {
            throw new IOException("exceeded list limit " + limit + ": " + s);
        }
        return result;
    }

    @Override
    public void write(@Nonnull final List<T> objs, final DataOutput out) throws IOException {
        if (limit > 0 && objs.size() > limit) {
            throw new IllegalArgumentException("exceeded list limit " + limit + ": " + objs);
        }
        if (limit > 0 && limit < 256) {
            out.writeByte((byte) objs.size());
        } else {
            writeVLong(out, objs.size());
        }
        for (final T obj : objs) {
            serializer.write(obj, out);
        }
    }

    @Override
    public List<T> read(final DataInput in) throws IOException {
        final int length = (limit > 0 && limit < 256) ? (in.readByte() & 0xFF) : (int) readVLong(in);
        final List<T> result = new ArrayList<>(length);
        for (int i = 0; i < length; ++i) {
            result.add(serializer.read(in));
        }
        return result;
    }

    @Override
    public long sizeOf(final List<T> list) throws IOException {
        final int n = list.size();
        if (serializer.size().isConstant() && ((limit > 0 && limit < 256) || ((n < 128) && (n >= -32)))) {
            return 1 + n * serializer.size().getConstant();
        }
        return super.sizeOf(list);
    }

    @Override
    public LinearDiophantineEquation size() {
        return sizeEq;
    }
}
