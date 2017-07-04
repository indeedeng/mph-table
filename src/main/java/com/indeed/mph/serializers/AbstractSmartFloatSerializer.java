package com.indeed.mph.serializers;

import com.indeed.mph.SmartSerializer;

import java.io.IOException;

/**
 * @author alexs
 */
public abstract class AbstractSmartFloatSerializer extends AbstractSmartSerializer<Float> {
    private static final long serialVersionUID = -1167571588287234119L;

    @Override
    public Float parseFromString(final String s) throws IOException {
        return Float.parseFloat(s);
    }
}
