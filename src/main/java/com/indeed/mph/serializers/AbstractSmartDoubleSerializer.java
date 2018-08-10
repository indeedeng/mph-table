package com.indeed.mph.serializers;

import com.indeed.mph.SmartSerializer;

import java.io.IOException;

/**
 * @author alexs
 */
public abstract class AbstractSmartDoubleSerializer extends AbstractSmartSerializer<Double> {
    private static final long serialVersionUID = -2643924535236636206L;

    @Override
    public Double parseFromString(final String s) throws IOException {
        return Double.parseDouble(s);
    }
}
