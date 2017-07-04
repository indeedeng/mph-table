package com.indeed.mph.serializers;

import com.indeed.mph.SmartSerializer;

import java.io.IOException;

/**
 * @author alexs
 */
public abstract class AbstractSmartLongSerializer extends AbstractSmartSerializer<Long> {
    private static final long serialVersionUID = 7779405971299471697L;

    @Override
    public Long parseFromString(final String s) throws IOException {
        return Long.parseLong(s);
    }
}
