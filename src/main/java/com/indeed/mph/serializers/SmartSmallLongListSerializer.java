package com.indeed.mph.serializers;

import com.google.common.primitives.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This serializer is used to serialize a list of long numbers that contains many small non-negative numbers.
 * It uses four types of byte to encode numbers:
 * 00 XX XX XX is used to encode three numbers in the range [0, 4).
 * 01 XXX XXX is used to encode two numbers in the range [0, 8).
 * 10 XXXXXXX, 11 XXXXXX are used to encode the other numbers. 11 is used at the head of leading bytes and 10 is used
 * at the head of final byte.
 * This serializer also supports setting offset. Minus offset from all the number when serializing and plus back when
 * de-serializing.
 *
 * @author xinjianz
 */
public class SmartSmallLongListSerializer extends AbstractSmartSerializer<List<Long>>  {

    private static final byte MASK1 = 0X00; // 00000000
    private static final byte MASK2 = 0X40; // 01000000
    private static final byte MASK3 = (byte)0X80; // 10000000
    private static final byte MASK4 = (byte)0XC0; // 11000000
    private static final byte TYPE_MASK = (byte)0XC0; // 11000000
    private static final long DATA_MASK = 0X3F; // 00111111

    private static final long serialVersionUID = 7516104793L;

    private final SmartVLongSerializer lengthSerializer = new SmartVLongSerializer();

    private final long offset;

    public SmartSmallLongListSerializer() {
        offset = 0;
    }

    public SmartSmallLongListSerializer(final long offset) {
        this.offset = offset;
    }

    /**
     * Only write data of the list, not include the length of the list.
     * @param data the input list.
     * @param out DataOutput.
     * @throws IOException if unable to write to out
     */
    public void writeDataOnly(final List<Long> data, final DataOutput out) throws IOException {
        int index = 0;
        while (index < data.size()) {
            final long number0 = data.get(index) - offset;
            final long number1 = index + 1 < data.size() ? data.get(index + 1) - offset : 0;
            final long number2 = index + 2 < data.size() ? data.get(index + 2) - offset : 0;
            if (index + 2 < data.size() &&
                number0 >= 0 && number0 < 4 &&
                number1 >= 0 && number1 < 4 &&
                number2 >= 0 && number2 < 4) {
                // Three consecutive numbers in the range [0, 4).
                out.write((byte)(MASK1 | (number0 << 4) | (number1 << 2) | number2));
                index += 3;
            } else if (index + 1 < data.size() &&
                       number0 >= 0 && number0 < 8 &&
                       number1 >= 0 && number1 < 8) {
                // Two consecutive numbers in the range [0, 8)
                out.write((byte)(MASK2 | (number0 << 3) | number1));
                index += 2;
            } else {
                long number = number0;
                while ((number & ~DATA_MASK) != 0) {
                    out.write((byte)(MASK4 | (number & DATA_MASK)));
                    number >>>= 6;
                }
                out.write((byte)(MASK3 | (number & DATA_MASK)));
                ++index;
            }
        }
    }

    @Override
    public void write(final List<Long> data, final DataOutput out) throws IOException {
        lengthSerializer.write((long)data.size(), out);
        writeDataOnly(data, out);
    }

    /**
     * Only read the data of a list, the length of the list is given as an input parameter.
     * @param length the length of the array to read.
     * @param in the input data.
     * @return the deserialized list.
     * @throws IOException if unable to write to out
     */
    public List<Long> readDataOnly(final int length, final DataInput in) throws IOException {
        final List<Long> numbers = new ArrayList<>(length);
        while (numbers.size() < length) {
            byte value = in.readByte();
            if ((value & TYPE_MASK) == MASK1) {
                numbers.add(((value >> 4) & 0X03L) + offset);
                numbers.add(((value >> 2) & 0X03L) + offset);
                numbers.add((value & 0X03L) + offset);
            } else if ((value & TYPE_MASK) == MASK2) {
                numbers.add(((value >> 3) & 0X07L) + offset);
                numbers.add((value & 0X07L) + offset);
            } else {
                int shift = 0;
                long decodedNumber = 0;
                while ((value & TYPE_MASK) == MASK4) {
                    decodedNumber += (value & DATA_MASK) << shift;
                    shift += 6;
                    value = in.readByte();
                }
                decodedNumber += (value & DATA_MASK) << shift;
                numbers.add(decodedNumber + offset);
            }
        }
        return numbers;
    }

    @Override
    public List<Long> read(final DataInput in) throws IOException {
        final int length = lengthSerializer.read(in).intValue();
        return readDataOnly(length, in);
    }
}
