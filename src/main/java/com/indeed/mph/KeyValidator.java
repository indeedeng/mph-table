package com.indeed.mph;

import java.io.Serializable;

/**
 * A validator used when looking up entries.  Given the inputKey,
 * assuming it has a known hash value we will extract an optional key
 * and value, and it's the job of the validator to return the correct
 * value, or null if this was not a known key.
 *
 * For example, the default {@link EqualKeyValidator} used by
 * {@link TableWriter} just checks that the keys are equal and
 * if so returns the extractedValue.
 *
 * @author alexs
 */
public interface KeyValidator<K, V> extends Serializable {
    V validate(K inputKey, K extractedKey, V extractedValue);
}
