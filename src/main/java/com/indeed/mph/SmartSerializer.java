package com.indeed.mph;

import com.indeed.util.serialization.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * A Serializer which is itself serializable, including additional
 * (optional) utilities for convenience and performance.
 *
 * See {@link AbstractSmartSerializer} for default methods and
 * {@link WrappedSerializer} to wrap any existing Serializer which
 * has a default constructor.
 *
 * @author alexs
 */
public interface SmartSerializer<T> extends Serializable, Serializer<T>, Parseable<T> {
    // Skips n records from in, possibly by just repeatedly calling
    // read and discarding the result.
    void skip(DataInput in, int n) throws IOException;

    // Returns the serialized size for a specific object.  Must always
    // return the same value for the same object.
    long sizeOf(T t) throws IOException;

    // If non-null, indicates that the serialized form of T always has
    // a size determined by the given linear equation in one unknown
    // aspect of T.
    LinearDiophantineEquation size();
}
