package com.indeed.mph.serializers;

import com.indeed.mph.LinearDiophantineEquation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Serializer for arbitrary precision integers, represented as base-32 in strings
 * but using raw 8-bit bytes for serialization.
 *
 * @author alexs
 */
public class SmartBase32Serializer extends AbstractSmartSerializer<String> {
    private static final Pattern VALID_BASE32_PAT = Pattern.compile("^[0-9a-v]+$", Pattern.CASE_INSENSITIVE);
    private static final long serialVersionUID = -3895520373164636225L;
    private final int version = 1;
    private final int numRawBits;
    private final int numRawBytes;
    private final int numBase32Bytes;
    private final LinearDiophantineEquation sizeEq;

    public SmartBase32Serializer(final int numRawBits) {
        this.numRawBits = numRawBits;
        this.numRawBytes = (numRawBits + 7) / 8;
        this.numBase32Bytes = (numRawBits + 4) / 5;
        this.sizeEq = LinearDiophantineEquation.constantValue(numRawBytes);
    }

    @Override
    public String parseFromString(final String s) throws IOException {
        if (s.length() == numBase32Bytes && VALID_BASE32_PAT.matcher(s).matches()) {
            return s;
        } else {
            throw new IOException("not a valid base32 string: " + s);
        }
    }

    public static int decodeBase32(char ch) {
        int ret = -1;
        if (Character.isDigit(ch)) {
            ret = ch - '0';
        } else if (Character.isUpperCase(ch)) {
            ret = ch - 'A' + 10;
        } else if (Character.isLowerCase(ch)) {
            ret = ch - 'a' + 10;
        }
        if (ret < 0 || ret > 31) {
            throw new IllegalArgumentException("invalid base 32 character");
        }
        return ret;
    }

    @Override
    public void write(final String base32, final DataOutput out) throws IOException {
        if (base32.length() != numBase32Bytes) {
            throw new IOException("not a valid base32 string: " + base32);
        }
        int acc = 0;
        int bits = 0;
        for (int i = 0; i < base32.length(); ++i) {
            final int value = decodeBase32(base32.charAt(i));
            acc = (acc << 5) + value;
            bits += 5;
            if (bits >= 8) {
                out.writeByte(bits == 8 ? acc : acc >> (bits % 8));
                acc = (bits == 8 ? 0 : acc & ((1 << (bits % 8)) - 1));
                bits -= 8;
            }
        }
        if (bits > 0) {
            out.writeByte(acc);
        }
    }

    public static char encodeBase32(int b) {
        return (char) (b < 10 ? b + '0' : b + 'a' - 10);
    }

    @Override
    public String read(final DataInput in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int acc = 0;
        int bits = 0;
        for (int i = 0; i < numRawBytes; ++i) {
            final int b = (int) in.readByte() & 0xFF;
            acc = (acc << 8) + b;
            bits += 8;
            if (bits >= 10) {
                sb.append(encodeBase32(acc >> (bits == 10 ? 5 : (5 + (bits % 5)))));
                acc = acc & ((1 << (bits == 10 ? 5 : (5 + (bits % 5)))) - 1);
                bits -= 5;
            }
            if (bits >= 5) {
                sb.append(encodeBase32(bits == 5 ? acc : acc >> (bits % 5)));
                acc = (bits == 5 ? 0 : acc & ((1 << (bits % 5)) - 1));
                bits -= 5;
            }
        }
        if (bits > 0) {
            sb.append(encodeBase32(acc));
        }
        return sb.toString();
    }

    @Override
    public long sizeOf(final String base32) {
        return numRawBytes;
    }

    @Override
    public LinearDiophantineEquation size() {
        return sizeEq;
    }
}
