package com.indeed.mph;

/**
 * @author alexs
 */
public class EqualKeyValidator<K, V> implements KeyValidator<K, V> {
    private static final long serialVersionUID = 770186872;

    public V validate(final K inputKey, final K extractedKey, final V extractedValue) {
        // allow null for implicit keys
        return extractedKey == null || inputKey.equals(extractedKey) ? extractedValue : null;
    }
}
