package com.indeed.mph.serializers;

/**
 * Given a mapping of encoded value => encoded float for sorted non-zero values
 * perform the conversion of encoding to value, or value to encoding.
 * When given a value, it will find the encoding that is the closest approximation of the value.
 * This does not handle NaN, infinity, and has 2 zero values (positive/negative).
 * Here are some example encoding mappings for 8-bit floats (q2.5)
 * <ul>
 *     <li>0 => 0</li>
 *     <li>1 => pow(2, -16) * 1</li>
 *     <li>2 => pow(2, -16) * 2</li>
 *     <li>4 => pow(2, -15) * 4</li>
 *     <li>-128 => -0</li>
 *     <li>-127 => -pow(2, -16) * 1</li>
 *     <li>-126 => -pow(2, -16) * 2</li>
 * </ul>
 * @author kenh
 */
class FloatEncodingMapping {
    private final float[] values;

    FloatEncodingMapping(final float[] values) {
        this.values = values;
    }

    float encodingToFloat(final int enc) {
        if (enc < 0) {
            return -values[enc + values.length];
        } else {
            return values[enc];
        }
    }

    int findClosestEncoding(final float v) {
        final int maxEnc = values.length - 1;
        if (v == 0.0) {
            return 0;
        } else if (v >= values[maxEnc]) {
            return maxEnc;
        } else if (-v >= values[maxEnc]) {
            return -1;
        }

        final float absv = Math.abs(v);
        int lo = 0;
        int hi = maxEnc;

        while (lo < hi) {
            final int mid = (lo + hi) / 2;
            final float midValue = values[mid];
            if (absv < midValue) {
                hi = mid;
            } else if (absv > midValue) {
                if (lo == mid) {
                    break;
                }
                lo = mid;
            } else {
                return (v < 0.0f) ? (mid - values.length) : mid;
            }
        }
        final int prevHi = (lo == hi) ? (lo + 1) : hi;
        final int closest =
                (Math.abs(absv - values[lo]) <= Math.abs(absv - values[prevHi])) ? lo : prevHi;
        return (v < 0.0f) ? (closest - values.length) : closest;
    }
}
