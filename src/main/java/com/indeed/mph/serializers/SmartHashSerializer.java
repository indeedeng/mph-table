package com.indeed.mph.serializers;

import com.indeed.mph.LinearDiophantineEquation;
import com.indeed.mph.Parseable;
import com.indeed.mph.TableMeta;
import it.unimi.dsi.sux4j.mph.AbstractHashFunction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

/**
 * An Integer serializer which compresses its values to a smaller
 * range using a pre-existing minimal perfect hash.  Conversion occurs
 * only on parsing, as a convenience for translating mapreduce output.
 *
 * @author alexs
 */
public class SmartHashSerializer extends SmartIntegerSerializer {
    private static final LinearDiophantineEquation FOUR =
        LinearDiophantineEquation.constantValue(4L);
    private static final long serialVersionUID = 2009124557;
    private final AbstractHashFunction<Long> mph;
    private final Parseable<Long> parser;
    private final boolean ignoreErrors;

    public SmartHashSerializer(final AbstractHashFunction<Long> mph, final Parseable<Long> parser, final boolean ignoreErrors) {
        this.mph = mph;
        this.parser = parser;
        this.ignoreErrors = ignoreErrors;
    }

    public SmartHashSerializer(final AbstractHashFunction<Long> mph, final Parseable<Long> parser) {
        this(mph, parser, false);
    }

    public SmartHashSerializer(final AbstractHashFunction<Long> mph) {
        this(mph, null);
    }

    public SmartHashSerializer(final String metaPath, final Parseable<Long> parser, final boolean ignoreErrors) throws IOException {
        this(TableMeta.load(new File(metaPath)).getMph(), parser, ignoreErrors);
    }

    public SmartHashSerializer(final String metaPath, final Parseable<Long> parser) throws IOException {
        this(metaPath, parser, false);
    }

    public SmartHashSerializer(final String metaPath) throws IOException {
        this(metaPath, null);
    }

    @Override
    public Integer parseFromString(final String s) throws IOException {
        final Long value = parser == null ? Long.parseLong(s) : parser.parseFromString(s);
        final Long hash = value == null ? null : mph.get(value);
        if (!ignoreErrors && (hash == null || hash < 0)) {
            throw new IOException("bad key: " + s);
        }
        return hash == null ? null : hash.intValue();
    }
}
