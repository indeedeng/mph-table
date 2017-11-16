package com.indeed.mph;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Configuration for TableWriter (serialized and loaded automatically for TableReader).
 *
 * Fluent API constructor allows you to set the following fields:
 *
 *   keySerializer: A SmartSerializer applied to the keys (required).
 *
 *   valueSerializer: A SmartSerializer applied to the values
 *     (optional, without which values are not stored).
 *
 *   offsetStorage: The hash function maps keys to hash buckets, which
 *     we then need to map to offsets in the data file.  By default,
 *     the most compact storage representation is chosen
 *     automatically, but you can manually override:
 *       - FIXED: if the entries are all a fixed size, we don't need to store offsets
 *       - INDEXED: offsets are just a flat table indexed by hash bucket
 *       - SELECTED: offsets are represented as a bit-vector of all bytes in the data
 *           file, and we use a Rank/Select algorithm to quickly map from hash bucket
 *           to corresponding starting offset
 *     In general, if you have many small entries SELECTED will be
 *     better, but INDEXED is better if individual entries are large.
 *
 *   keyStorage: EXPLICIT by default, but can be set to IMPLICIT
 *     (along with specifying a signatureWidth) to omit the keys from
 *     table at the expense of allowing false positives.  In many
 *     cases, you know you will only be querying existing keys so
 *     there's no reason to store them.
 *
 *   rangeChecking: if not NONE, keeps track of the minimum and
 *     maximum keys in the table to shortcut lookups and further
 *     reduce false positives when using IMPLICIT keyStorage.
 *
 *   signatureWidth: The number of bits per key to use in a bloom
 *     filter (required for IMPLICIT keyStorage).
 *
 *   maxHeapUsage: If positive, the limit beyond which offsets are
 *     mmapped instead of being stored directly in the heap.  By
 *     default we always store offsets in the heap.
 *
 *   maxDataHeapUsage: If positive, the limit beyond which data is
 *     mmapped instead of being stored directly in the heap.  By
 *     default we never store data in the heap.
 *
 *   debugDuplicateKeys: If true, asks TableWriter to attempt to
 *     determine which keys were duplicated on failure to build the
 *     hash function.
 *
 * @param <K> key type
 * @param <V> value type
 *
 * @author alexs
 */
public class TableConfig<K, V> implements Serializable {
    public static final long DEFAULT_SHARD_SIZE = 64 * 1024 * 1024;
    private static final long serialVersionUID = 927763169;
    private final SmartSerializer<? super K> keySerializer;
    private final SmartSerializer<? super V> valueSerializer;
    private final LinearDiophantineEquation entrySizeEq;
    private final KeyValidator<K, V> keyValidator;
    private final KeyStorage keyStorage;
    private final OffsetStorage offsetStorage;
    private final RangeChecking rangeChecking;
    private final int signatureWidth;
    private final long maxHeapUsage;
    private final long maxDataHeapUsage;
    private final long tempShardSize;
    private final boolean debugDuplicateKeys;

    TableConfig(@Nullable final SmartSerializer<? super K> keySerializer,
                @Nullable final SmartSerializer<? super V> valueSerializer,
                @Nullable final KeyValidator<K, V> keyValidator,
                final KeyStorage keyStorage,
                final OffsetStorage offsetStorage,
                final RangeChecking rangeChecking,
                final int signatureWidth,
                final long maxHeapUsage,
                final long maxDataHeapUsage,
                final long tempShardSize,
                final boolean debugDuplicateKeys) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyValidator = keyValidator;
        this.keyStorage = keyStorage;
        this.offsetStorage = offsetStorage;
        this.rangeChecking = rangeChecking;
        this.signatureWidth = signatureWidth;
        this.maxHeapUsage = maxHeapUsage;
        this.maxDataHeapUsage = maxDataHeapUsage;
        this.tempShardSize = tempShardSize;
        this.debugDuplicateKeys = debugDuplicateKeys;
        final LinearDiophantineEquation valueSizeEq = valueSerializer == null ?
            LinearDiophantineEquation.constantValue(0L) :
            valueSerializer.size() == null ? LinearDiophantineEquation.multipleOf(1L) : valueSerializer.size();
        final LinearDiophantineEquation keySizeEq =
            (KeyStorage.IMPLICIT.equals(keyStorage) || keySerializer == null) ?
            LinearDiophantineEquation.constantValue(0L) : keySerializer.size();
        this.entrySizeEq =
            (keySizeEq == null ? LinearDiophantineEquation.multipleOf(1L) : keySizeEq).add(valueSizeEq);
    }

    public TableConfig() {
        this(null, null, new EqualKeyValidator(), KeyStorage.EXPLICIT, OffsetStorage.AUTOMATIC, RangeChecking.NONE, 0, 0, 0, DEFAULT_SHARD_SIZE, false);
    }

    public SmartSerializer<? super K> getKeySerializer() {
        return keySerializer;
    }

    public SmartSerializer<? super V> getValueSerializer() {
        return valueSerializer;
    }

    public KeyValidator<K, V> getKeyValidator() {
        return keyValidator;
    }

    public KeyStorage getKeyStorage() {
        return keyStorage;
    }

    public OffsetStorage getOffsetStorage() {
        return offsetStorage;
    }

    public RangeChecking getRangeChecking() {
        return rangeChecking;
    }

    public int getSignatureWidth() {
        return signatureWidth;
    }

    public long getMaxHeapUsage() {
        return maxHeapUsage;
    }

    public long getMaxDataHeapUsage() {
        return maxDataHeapUsage;
    }

    public long getTempShardSize() {
        return tempShardSize;
    }

    public boolean getDebugDuplicateKeys() {
        return debugDuplicateKeys;
    }

    public LinearDiophantineEquation getEntrySize() {
        return entrySizeEq;
    }

    public long sizeOf(final K key, final V value) throws IOException {
        return (TableConfig.KeyStorage.IMPLICIT.equals(getKeyStorage()) ? 0 : getKeySerializer().sizeOf(key)) +
            (getValueSerializer() == null ? 0 : getValueSerializer().sizeOf(value));
    }

    // We add in an extra 1*n to ensure that every compressed offset
    // is unique.
    // TODO: consider checking case-by-case if this is needed

    public long compressOffset(final long offset, final long n) {
        return entrySizeEq.solveForNth(offset, n) + n;
    }

    public long decompressOffset(final long value, final long n) {
        return entrySizeEq.applyNth(value - n, n);
    }

    public OffsetStorage chooseBestOffsetStorage(final long numEntries, final long dataSize) {
        if (entrySizeEq.isConstant()) {
            return OffsetStorage.FIXED;
        }
        final long indexedSize = getIndexedOffsetSize(numEntries, dataSize);
        final long selectedSize = getSelectedOffsetSize(numEntries, dataSize);
        return indexedSize <= selectedSize ? OffsetStorage.INDEXED : OffsetStorage.SELECTED;
    }

    public long getIndexedOffsetSize(final long numEntries, final long dataSize) {
        return numEntries * bytesPerOffset(numEntries, dataSize);
    }

    public long getSelectedOffsetSize(final long numEntries, final long dataSize) {
        final long maxValue = compressOffset(dataSize, numEntries);
        return ((maxValue * 3L) / 64L);
    }

    public int bytesPerOffset(final long numEntries, final long dataSize) {
        return bytesPerLong(dataSize);
    }

    public static int bytesPerLong(final long maxValue) { // currently handle only power of 2 bytes
        if (maxValue < Short.MAX_VALUE) {
            return 2;
        }
        if (maxValue < Integer.MAX_VALUE) {
            return 4;
        }
        return 8;
    }

    public boolean isValid() {
        return keySerializer != null &&
            (!OffsetStorage.FIXED.equals(offsetStorage) || entrySizeEq.isConstant());
    }

    public K readKey(final DataInput in) throws IOException {
        return (K) ((TableConfig.KeyStorage.IMPLICIT.equals(keyStorage) || keySerializer == null) ? null :
                    keySerializer.read(in));
    }

    public V readValue(final DataInput in) throws IOException {
        return (V) ((valueSerializer == null) ? null : valueSerializer.read(in));
    }

    public void write(final K k, final V v, final DataOutput out) throws IOException {
        if (!KeyStorage.IMPLICIT.equals(keyStorage)) {
            keySerializer.write(k, out);
        }
        if (valueSerializer != null) {
            valueSerializer.write(v, out);
        }
    }

    public TableConfig<K, V> withKeySerializer(final SmartSerializer<? super K> serializer) {
        return new TableConfig<K,V>(serializer, valueSerializer, keyValidator, keyStorage, offsetStorage, rangeChecking, signatureWidth, maxHeapUsage, maxDataHeapUsage, tempShardSize, debugDuplicateKeys);
    }

    public TableConfig<K, V> withValueSerializer(final SmartSerializer<? super V> serializer) {
        return new TableConfig<K,V>(keySerializer, serializer, keyValidator, keyStorage, offsetStorage, rangeChecking, signatureWidth, maxHeapUsage, maxDataHeapUsage, tempShardSize, debugDuplicateKeys);
    }

    public TableConfig<K, V> withKeyValidator(final KeyValidator<K, V> validator) {
        return new TableConfig<K,V>(keySerializer, valueSerializer, validator, keyStorage, offsetStorage, rangeChecking, signatureWidth, maxHeapUsage, maxDataHeapUsage, tempShardSize, debugDuplicateKeys);
    }

    public TableConfig<K, V> withKeyStorage(final KeyStorage storage) {
        return new TableConfig<K,V>(keySerializer, valueSerializer, keyValidator, storage, offsetStorage, rangeChecking, signatureWidth, maxHeapUsage, maxDataHeapUsage, tempShardSize, debugDuplicateKeys);
    }

    public TableConfig<K, V> withOffsetStorage(final OffsetStorage storage) {
        return new TableConfig<K,V>(keySerializer, valueSerializer, keyValidator, keyStorage, storage, rangeChecking, signatureWidth, maxHeapUsage, maxDataHeapUsage, tempShardSize, debugDuplicateKeys);
    }

    public TableConfig<K, V> withRangeChecking(final RangeChecking rangeCheck) {
        return new TableConfig<K,V>(keySerializer, valueSerializer, keyValidator, keyStorage, offsetStorage, rangeCheck, signatureWidth, maxHeapUsage, maxDataHeapUsage, tempShardSize, debugDuplicateKeys);
    }

    public TableConfig<K, V> withSignatureWidth(final int width) {
        return new TableConfig<K,V>(keySerializer, valueSerializer, keyValidator, keyStorage, offsetStorage, rangeChecking, width, maxHeapUsage, maxDataHeapUsage, tempShardSize, debugDuplicateKeys);
    }

    public TableConfig<K, V> withMaxHeapUsage(final long maxHeap) {
        return new TableConfig<K,V>(keySerializer, valueSerializer, keyValidator, keyStorage, offsetStorage, rangeChecking, signatureWidth, maxHeap, maxDataHeapUsage, tempShardSize, debugDuplicateKeys);
    }

    public TableConfig<K, V> withMaxDataHeapUsage(final long maxDataHeap) {
        return new TableConfig<K,V>(keySerializer, valueSerializer, keyValidator, keyStorage, offsetStorage, rangeChecking, signatureWidth, maxHeapUsage, maxDataHeap, tempShardSize, debugDuplicateKeys);
    }

    public TableConfig<K, V> withTempShardSize(final long shardSize) {
        return new TableConfig<K,V>(keySerializer, valueSerializer, keyValidator, keyStorage, offsetStorage, rangeChecking, signatureWidth, maxHeapUsage, maxDataHeapUsage, shardSize, debugDuplicateKeys);
    }

    public TableConfig<K, V> withDebugDuplicateKeys(final boolean debugDupKeys) {
        return new TableConfig<K,V>(keySerializer, valueSerializer, keyValidator, keyStorage, offsetStorage, rangeChecking, signatureWidth, maxHeapUsage, maxDataHeapUsage, tempShardSize, debugDupKeys);
    }

    public String toString() {
        return "[TableConfig keys: " + keySerializer + " values: " + valueSerializer +
            " keyStorage: " + keyStorage + " offsetStorage: " + offsetStorage +
            " rangeChecking: " + rangeChecking +
            " validator: " + keyValidator + " signatureWidth: " + signatureWidth +
            " maxHeapUsage: " + maxHeapUsage + " maxDataHeapUsage: " + maxDataHeapUsage +
            " entrySize: " + entrySizeEq + " debugDupKeys: " + debugDuplicateKeys + "]";
    }

    public enum KeyStorage {
        EXPLICIT,               // default
        IMPLICIT                // don't actually store the keys
    }

    public enum OffsetStorage {
        AUTOMATIC,              // choose optimal storage
        INDEXED,                // an indexed array of offsets per hash
        SELECTED,               // a rank-select lookup per hash
        FIXED                   // fixed size entries
    }

    public enum RangeChecking {
        MIN_AND_MAX,
        NONE,
        AUTOMATIC,
    }
}
