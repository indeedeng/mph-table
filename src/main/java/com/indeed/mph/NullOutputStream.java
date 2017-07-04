package com.indeed.mph;

import java.io.IOException;
import java.io.OutputStream;

public class NullOutputStream extends OutputStream {
    private long count;

    public long getCount() {
        return count;
    }

    @Override
    public void write(final int b) throws IOException {
        ++count;
    }

    @Override
    public void write(final byte[] b) throws IOException {
        count += b.length;
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        count += len;
    }
}
