package com.indeed.mph;

import com.indeed.util.mmap.Memory;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.sux4j.bits.Select;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * All meta-information for a table, including configuration, paths,
 * optional offsets, and the hash function itself, but not the raw
 * data.  Generally you don't need to access this directly.
 *
 * @param <K> key type
 * @param <V> value type
 *
 * @author alexs
 */
public class TableMeta<K, V> implements Serializable {
    private static final long serialVersionUID = 1288403300;
    public static final String DEFAULT_META_PATH = "meta.bin";
    public static final String DEFAULT_OFFSETS_PATH = "offsets.bin";
    public static final String DEFAULT_DATA_PATH = "data.bin";
    private final TableConfig<K, V> config;
    private final GOVMinimalPerfectHashFunction<K> mph;
    private final Select selectOffsets;
    private final long dataSize;
    private final int bytesPerOffset;
    private final long timestamp;
    private final int version = 1;
    private File metaPath;
    private File offsetsPath;
    private File dataPath;

    public TableMeta(@Nonnull final TableConfig<K, V> config,
                     @Nonnull final GOVMinimalPerfectHashFunction<K> mph,
                     @Nullable final Select selectOffsets,
                     final long dataSize) {
        this.config = config;
        this.mph = mph;
        this.selectOffsets = selectOffsets;
        this.dataSize = dataSize;
        this.bytesPerOffset = config.bytesPerOffset(numEntries(), dataSize);
        this.timestamp = System.currentTimeMillis();
    }

    public static TableMeta load(@Nonnull final File input, @Nullable final File offsetsPath, @Nullable final File dataPath) throws IOException {
        final File metaPath = input.isDirectory() ? new File(input, DEFAULT_META_PATH) : input;
        try (final ObjectInputStream in = new ObjectInputStream(new FileInputStream(metaPath))) {
            final TableMeta result = (TableMeta) in.readObject();
            result.metaPath = metaPath;
            if (offsetsPath != null) {
                result.offsetsPath = offsetsPath;
            }
            if (dataPath != null) {
                result.dataPath = dataPath;
            }
            return result;
        } catch (final ClassNotFoundException e) {
            throw new IOException("couldn't read TableMeta", e);
        }
    }

    public static TableMeta load(@Nonnull final File metaPath) throws IOException {
        return load(metaPath, null, null);
    }

    public void store(@Nonnull final File path) throws IOException {
        try (final ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(path))) {
            out.writeObject(this);
        }
        path.setReadOnly();
    }

    public File getMetaPath() {
        return metaPath;
    }

    public File getOffsetsPath() {
        return offsetsPath == null ? new File(metaPath.getParentFile(), DEFAULT_OFFSETS_PATH) : offsetsPath;
    }

    public File getDataPath() {
        return dataPath == null ? new File(metaPath.getParentFile(), DEFAULT_DATA_PATH) : dataPath;
    }

    public long numEntries() {
        return mph.size64();
    }

    public long getSizeInBytes() {
        switch (config.getOffsetStorage()) {
        case INDEXED:
            return dataSize + config.getIndexedOffsetSize(numEntries(), dataSize);
        case SELECTED:
            return dataSize + config.getSelectedOffsetSize(numEntries(), dataSize);
        default:
            return dataSize;
        }
    }

    public int getVersion() {
        return version;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Nonnull
    public GOVMinimalPerfectHashFunction<K> getMph() {
        return mph;
    }

    @Nonnull
    public TableConfig<K, V> getConfig() {
        return config;
    }

    @Nullable
    public Select getSelectOffsets() {
        return selectOffsets;
    }

    public long getHash(@Nonnull final K key) {
        return mph.getLong(key);
    }

    public long getOffset(@Nonnull final K key, @Nullable final Memory offsets, @Nullable final Select select) {
        final long hash = getHash(key);
        if (hash < 0) {
            return hash;
        }
        return getHashOffset(hash, offsets, select);
    }

    public long getHashOffset(final long hash, @Nullable final Memory offsets, @Nullable final Select select) {
        switch (config.getOffsetStorage()) {
        case INDEXED:
            if (offsets == null) {
                throw new IllegalArgumentException("indexed offsets with null memory");
            }
            final long offset;
            if (bytesPerOffset == 2) {
                offset = offsets.getShort(hash * 2L);
            } else if (bytesPerOffset == 4) {
                offset = offsets.getInt(hash * 4L);
            } else {
                offset = offsets.getLong(hash * 8L);
            }
            return offset;
        case SELECTED:
            final long rawSelected = select == null ? selectOffsets.select(hash) : select.select(hash);
            final long selected = rawSelected < 0 ? 0 : rawSelected;
            return config.decompressOffset(selected, hash);
        case FIXED:
            return config.decompressOffset(0L /* unused */, hash);
        default:
            throw new IllegalArgumentException("unknown offset storage type: " + config.getOffsetStorage());
        }
    }

    public String toString() {
        return "[TableMeta version: " + version + " timestamp: " + timestamp + " config: " + config +
            " mph: " + mph + " (" + mph.size() + " entries) " + " selectOffsets: " + selectOffsets +
            " dataSize: " + dataSize + " metaPath: " + metaPath + " offsetsPath: " + offsetsPath +
            " dataPath: " + dataPath + "]";
    }
}
