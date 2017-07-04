package com.indeed.mph;

import com.google.common.io.LittleEndianDataOutputStream;
import com.indeed.util.io.Files;
import com.indeed.util.mmap.MMapBuffer;
import it.unimi.dsi.bits.AbstractBitVector;
import it.unimi.dsi.bits.BitVector;
import it.unimi.dsi.sux4j.mph.GOV4Function;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * An mmap-backed GOV4Function, supporting serialization to and from a file.
 *
 * @author xinjianz
 */
public class MMapGOV4Function<T> {

    private final GOV4Function<T> gov4Function;
    private final MMapBuffer mmapBuffer;
    private final int signatureWidth;

    public MMapGOV4Function(final GOV4Function<T> gov4Function, final MMapBuffer mmapBuffer, final int signatureWidth) {
        this.gov4Function = gov4Function;
        this.mmapBuffer = mmapBuffer;
        this.signatureWidth = signatureWidth;
    }

    /*
     * Write an existing GOV4Function to path.
     */
    public static <T> void writeTo(final GOV4Function<T> gov4Function, final String path) throws NoSuchFieldException,
            IllegalAccessException, IOException {
        final Field dataField = gov4Function.getClass().getDeclaredField("data");
        dataField.setAccessible(true);
        final AbstractBitVector.LongBigListView signaturesData = (AbstractBitVector.LongBigListView) dataField.get(gov4Function);
        final Field bitVectorField = signaturesData.getClass().getDeclaredField("bitVector");
        bitVectorField.setAccessible(true);
        final BitVector bitVector = (BitVector)bitVectorField.get(signaturesData);
        try (final LittleEndianDataOutputStream outputStream = new LittleEndianDataOutputStream(
                new BufferedOutputStream(new FileOutputStream(Files.buildPath(path, "signatures.bin"))))) {
            for (final long value : bitVector.bits()) {
                outputStream.writeLong(value);
            }
        }
        dataField.set(gov4Function, null);
        try (final OutputStream outputStream = new FileOutputStream(Files.buildPath(path, "GOV4Function.bin"));
             final ObjectOutput objectOutput = new ObjectOutputStream(outputStream)) {
            objectOutput.writeObject(gov4Function);
        }
    }

    /*
     * Read an mmap-backed GOV4Function from path.
     */
    public static <T> MMapGOV4Function<T> readFrom(final String path) throws IOException, NoSuchFieldException,
            IllegalAccessException, ClassNotFoundException {
        final GOV4Function<T> gov4Function;
        try (final InputStream file = new FileInputStream(Files.buildPath(path, "GOV4Function.bin"));
             final ObjectInput input = new ObjectInputStream(file)) {
            gov4Function = (GOV4Function<T>)(input.readObject());
        }
        final Field widthField = gov4Function.getClass().getDeclaredField("width");
        widthField.setAccessible(true);
        final int width = (Integer) widthField.get(gov4Function);
        final MMapBuffer buffer = new MMapBuffer(new File(Files.buildPath(path, "signatures.bin")),
                                                 FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN);
        final MMapLongBigList signaturesData = new MMapLongBigList(buffer.memory().longArray(0, buffer.memory().length() / 8), width);
        final Field dataField = gov4Function.getClass().getDeclaredField("data");
        dataField.setAccessible(true);
        dataField.set(gov4Function, signaturesData);
        return new MMapGOV4Function<>(gov4Function, buffer, width);
    }

    public GOV4Function<T> getGov4Function() {
        return gov4Function;
    }

    public MMapBuffer getMMapBuffer() {
        return mmapBuffer;
    }

    public int getSignatureWidth() {
        return signatureWidth;
    }
}
