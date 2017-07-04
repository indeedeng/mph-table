package com.indeed.mph.serializers;

import com.indeed.mph.LinearDiophantineEquation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author alexs
 */
public class SmartVLongSerializer extends AbstractSmartLongSerializer {
    private static final LinearDiophantineEquation ONE_PLUS = LinearDiophantineEquation.slopeIntercept(1L, 1L);
    private static final long serialVersionUID = 2147053399;

    @Override
    public void write(final Long n, final DataOutput out) throws IOException {
        writeVLong(out, n);
    }

    @Override
    public Long read(final DataInput in) throws IOException {
        return readVLong(in);
    }

    @Override
    public long sizeOf(final Long n) throws IOException {
        if ((n < 128) && (n >= -32)) {
            return 1L;
        }
        return super.sizeOf(n);
    }

    @Override
    public LinearDiophantineEquation size() {
        return ONE_PLUS;
    }

    /**
     * Borrowed from  Hadoop org.apache.hadoop.io.file.tfile.Utils
     */

    /**
     * Encoding a Long integer into a variable-length encoding format.
     * <ul>
     * <li>if n in [-32, 127): encode in one byte with the actual value.
     * Otherwise,
     * <li>if n in [-20*2^8, 20*2^8): encode in two bytes: byte[0] = n/256 - 52;
     * byte[1]=n&0xff. Otherwise,
     * <li>if n IN [-16*2^16, 16*2^16): encode in three bytes: byte[0]=n/2^16 -
     * 88; byte[1]=(n>>8)&0xff; byte[2]=n&0xff. Otherwise,
     * <li>if n in [-8*2^24, 8*2^24): encode in four bytes: byte[0]=n/2^24 - 112;
     * byte[1] = (n>>16)&0xff; byte[2] = (n>>8)&0xff; byte[3]=n&0xff. Otherwise:
     * <li>if n in [-2^31, 2^31): encode in five bytes: byte[0]=-125; byte[1] =
     * (n>>24)&0xff; byte[2]=(n>>16)&0xff; byte[3]=(n>>8)&0xff; byte[4]=n&0xff;
     * <li>if n in [-2^39, 2^39): encode in six bytes: byte[0]=-124; byte[1] =
     * (n>>32)&0xff; byte[2]=(n>>24)&0xff; byte[3]=(n>>16)&0xff;
     * byte[4]=(n>>8)&0xff; byte[5]=n&0xff
     * <li>if n in [-2^47, 2^47): encode in seven bytes: byte[0]=-123; byte[1] =
     * (n>>40)&0xff; byte[2]=(n>>32)&0xff; byte[3]=(n>>24)&0xff;
     * byte[4]=(n>>16)&0xff; byte[5]=(n>>8)&0xff; byte[6]=n&0xff;
     * <li>if n in [-2^55, 2^55): encode in eight bytes: byte[0]=-122; byte[1] =
     * (n>>48)&0xff; byte[2] = (n>>40)&0xff; byte[3]=(n>>32)&0xff;
     * byte[4]=(n>>24)&0xff; byte[5]=(n>>16)&0xff; byte[6]=(n>>8)&0xff;
     * byte[7]=n&0xff;
     * <li>if n in [-2^63, 2^63): encode in nine bytes: byte[0]=-121; byte[1] =
     * (n>>54)&0xff; byte[2] = (n>>48)&0xff; byte[3] = (n>>40)&0xff;
     * byte[4]=(n>>32)&0xff; byte[5]=(n>>24)&0xff; byte[6]=(n>>16)&0xff;
     * byte[7]=(n>>8)&0xff; byte[8]=n&0xff;
     * </ul>
     *
     * @param out output stream
     * @param n   the integer number
     * @throws java.io.IOException
     */
    @SuppressWarnings("fallthrough")
    public static void writeVLong(final DataOutput out, final long n) throws IOException {
        if ((n < 128) && (n >= -32)) {
            out.writeByte((int) n);
            return;
        }

        final long un = (n < 0) ? ~n : n;
        // how many bytes do we need to represent the number with sign bit?
        final int len = ((Long.SIZE - Long.numberOfLeadingZeros(un)) / 8) + 1;
        int firstByte = (int) (n >> ((len - 1) * 8));
        switch (len) {
            case 1:
                // fall it through to firstByte==-1, len=2.
                firstByte >>= 8;
            case 2:
                if ((firstByte < 20) && (firstByte >= -20)) {
                    out.writeByte(firstByte - 52);
                    out.writeByte((int) n);
                    return;
                }
                // fall it through to firstByte==0/-1, len=3.
                firstByte >>= 8;
            case 3:
                if ((firstByte < 16) && (firstByte >= -16)) {
                    out.writeByte(firstByte - 88);
                    out.writeShort((int) n);
                    return;
                }
                // fall it through to firstByte==0/-1, len=4.
                firstByte >>= 8;
            case 4:
                if ((firstByte < 8) && (firstByte >= -8)) {
                    out.writeByte(firstByte - 112);
                    out.writeShort(((int) n) >>> 8);
                    out.writeByte((int) n);
                    return;
                }
                out.writeByte(len - 129);
                out.writeInt((int) n);
                return;
            case 5:
                out.writeByte(len - 129);
                out.writeInt((int) (n >>> 8));
                out.writeByte((int) n);
                return;
            case 6:
                out.writeByte(len - 129);
                out.writeInt((int) (n >>> 16));
                out.writeShort((int) n);
                return;
            case 7:
                out.writeByte(len - 129);
                out.writeInt((int) (n >>> 24));
                out.writeShort((int) (n >>> 8));
                out.writeByte((int) n);
                return;
            case 8:
                out.writeByte(len - 129);
                out.writeLong(n);
                return;
            default:
                throw new RuntimeException("Internal error");
        }
    }

    /**
     * Decoding the variable-length integer. Suppose the value of the first byte
     * is FB, and the following bytes are NB[*].
     * <ul>
     * <li>if (FB >= -32), return (long)FB;
     * <li>if (FB in [-72, -33]), return (FB+52)<<8 + NB[0]&0xff;
     * <li>if (FB in [-104, -73]), return (FB+88)<<16 + (NB[0]&0xff)<<8 +
     * NB[1]&0xff;
     * <li>if (FB in [-120, -105]), return (FB+112)<<24 + (NB[0]&0xff)<<16 +
     * (NB[1]&0xff)<<8 + NB[2]&0xff;
     * <li>if (FB in [-128, -121]), return interpret NB[FB+129] as a signed
     * big-endian integer.
     *
     * @param in input stream
     * @return the decoded long integer.
     * @throws java.io.IOException
     */

    public static long readVLong(final DataInput in) throws IOException {
        final int firstByte = in.readByte();
        if (firstByte >= -32) {
            return firstByte;
        }

        switch ((firstByte + 128) / 8) {
            case 11:
            case 10:
            case 9:
            case 8:
            case 7:
                return ((firstByte + 52) << 8) | in.readUnsignedByte();
            case 6:
            case 5:
            case 4:
            case 3:
                return ((firstByte + 88) << 16) | in.readUnsignedShort();
            case 2:
            case 1:
                return ((firstByte + 112) << 24) | (in.readUnsignedShort() << 8)
                        | in.readUnsignedByte();
            case 0:
                final int len = firstByte + 129;
                switch (len) {
                    case 4:
                        return in.readInt();
                    case 5:
                        return (((long) in.readInt()) << 8) | in.readUnsignedByte();
                    case 6:
                        return (((long) in.readInt()) << 16) | in.readUnsignedShort();
                    case 7:
                        return (((long) in.readInt()) << 24) | (in.readUnsignedShort() << 8)
                                | in.readUnsignedByte();
                    case 8:
                        return in.readLong();
                    default:
                        throw new IOException("Corrupted VLong encoding");
                }
            default:
                throw new RuntimeException("Internal error");
        }
    }
}
