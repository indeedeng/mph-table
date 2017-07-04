package com.indeed.mph.serializers;

import com.indeed.mph.LinearDiophantineEquation;
import com.indeed.mph.SmartSerializer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;

/**
 * @author xinjianz
 */
public class SmartStringConcatSerializer extends AbstractSmartSerializer<String> {

    private static final long serialVersionUID = 69167219L;

    private final SmartSerializer<String> serializer1;
    private final SmartSerializer<String> serializer2;
    private final String delimiter;
    private final Pattern delimiterPattern;
    private final int splitIndex;
    private final int version;

    public SmartStringConcatSerializer(@Nonnull final SmartSerializer<String> serializer1,
                                       @Nonnull final SmartSerializer<String> serializer2,
                                       @Nonnegative final int splitIndex) {
        this(serializer1, serializer2, null, splitIndex);
    }

    public SmartStringConcatSerializer(@Nonnull final SmartSerializer<String> serializer1,
                                       @Nonnull final SmartSerializer<String> serializer2,
                                       @Nonnull final String delimiter) {
        this(serializer1, serializer2, delimiter, -1);
    }

    private SmartStringConcatSerializer(final SmartSerializer<String> serializer1,
                                        final SmartSerializer<String> serializer2,
                                        final String delimiter,
                                        final int splitIndex) {
        this.serializer1 = serializer1;
        this.serializer2 = serializer2;
        this.delimiter = delimiter;
        delimiterPattern = delimiter == null ? null : Pattern.compile(delimiter);
        this.splitIndex = splitIndex;
        version = 0;
    }

    @Override
    public String parseFromString(final String s) throws IOException {
        return s;
    }

    @Override
    public void write(final String s, final DataOutput out) throws IOException {
        final String s1;
        final String s2;
        if (delimiterPattern != null) {
            final String[] values = delimiterPattern.split(s, 2);
            if (values.length != 2) {
                throw new IllegalArgumentException("Unformatted string: " + s);
            }
            s1 = values[0];
            s2 = values[1];
        } else {
            if (s.length() < splitIndex) {
                throw new IllegalArgumentException("Unformatted string: " + s);
            }
            s1 = s.substring(0, splitIndex);
            s2 = s.substring(splitIndex);
        }
        serializer1.write(s1, out);
        serializer2.write(s2, out);
    }

    @Override
    public String read(final DataInput in) throws IOException {
        if (delimiter != null) {
            return serializer1.read(in) + delimiter + serializer2.read(in);
        } else {
            return serializer1.read(in) + serializer2.read(in);
        }
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
