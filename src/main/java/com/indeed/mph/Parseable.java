package com.indeed.mph;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A class with a parseable, human-readable representation.
 *
 * @author alexs
 */
public interface Parseable<T> {
    // Parse a textual representation (not necessarily the same as the
    // serialized representation) of a T from the string.
    // This method is optional, but can be useful for debug utilities
    // to lookup a value by human-readable key.
    T parseFromString(String s) throws IOException;

    // Print a textual representation of T to out.  This should be the
    // complement of parse if parse is supported, otherwise it may
    // just use T.toString().
    void print(T t, OutputStream out) throws IOException;

    // As print, but return the results as a String.
    String printToString(T t);
}
