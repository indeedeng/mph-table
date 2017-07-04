package com.indeed.mph.serializers;

import com.indeed.mph.Parseable;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author alexs
 */
public abstract class AbstractParseable<T> implements Parseable<T> {
    private static final long serialVersionUID = 1370948847;

    public T parseFromString(final String s) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void print(final T t, final OutputStream out) throws IOException {
        out.write(printToString(t).getBytes("UTF-8"));
    }

    public String printToString(final T t) {
        return String.valueOf(t);
    }
}
