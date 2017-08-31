package com.indeed.mph.serializers;

import com.indeed.mph.LinearDiophantineEquation;
import com.indeed.mph.TableWriter;
import com.indeed.util.io.Files;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.sux4j.mph.GOV4Function;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Map;

/**
 * General serializer for any "dictionary" of terms, storing the terms
 * as unique ids.  This is useful for enums or fixed sets of terms
 * which are repeated, but if most inputs are unique it's more
 * efficient to just serialize as a string.  The same serializer (or
 * equivalent after serialization) must be used to deserialize, or the
 * ids will not match.
 *
 * @author alexs
 */
public class SmartDictionarySerializer extends AbstractSmartSerializer<String> {
    private static final long serialVersionUID = 2138609301;
    private final SmartVLongSerializer serializer = new SmartVLongSerializer();
    private Object2IntMap<String> dictionary = new Object2IntOpenHashMap<>();
    private GOV4Function<String> mphFunction;
    private String[] words;
    private boolean onlyUsedInValue;

    public SmartDictionarySerializer() {
        this(false);
    }

    public SmartDictionarySerializer(final boolean onlyUsedInValue) {
        this.onlyUsedInValue = onlyUsedInValue;
        this.mphFunction = null;
    }

    @Override
    public String parseFromString(final String str) throws IOException {
        return str;
    }

    @Override
    public void write(final String str, final DataOutput out) throws IOException {
        serializer.write(getIndex(str), out);
    }

    @Override
    public String read(final DataInput in) throws IOException {
        final Long n = serializer.read(in);
        if (words == null) {
            synchronized (this) {
                if (words == null) {
                    words = dictionaryToIndex(dictionary);
                }
            }
        }
        if (n < 0 || n >= words.length) {
            throw new IOException("read unknown serialized id: " + n);
        }
        return words[n.intValue()];
    }

    @Override
    public LinearDiophantineEquation size() {
        return serializer.size();
    }

    private long getIndex(final String str) throws IOException {
        if (mphFunction != null) {
            final long index = mphFunction.getLong(str);
            // Validate the index and string matched.
            if (index < 0 || index >= words.length || !str.equals(words[(int)(index)])) {
                return -1;
            }
            return index;
        }
        if (dictionary == null) {
            if (words == null) {
                throw new IOException("invalid dictionary, flat and mapped indexes both null");
            }
            synchronized (this) {
                if (dictionary == null) {
                    dictionary = indexToDictionary(words);
                }
            }
        }
        final Integer n = dictionary.get(str);
        if (n == null) {
            synchronized (this) {
                final Integer n2 = dictionary.get(str);
                if (n2 != null) {
                    return n2;
                }
                final int result = dictionary.size();
                dictionary.put(str, result);
                words = null;       // invalidate current index
                return result;
            }
        }
        return n;
    }

    private Object2IntMap<String> indexToDictionary(final String[] words) throws IOException {
        final Object2IntMap<String> result = new Object2IntOpenHashMap<>();
        for (int i = 0; i < words.length; ++i) {
            result.put(words[i], i);
        }
        return result;
    }

    private String[] dictionaryToIndex(final Object2IntMap<String> dict) throws IOException {
        final String[] result = new String[dict.size()];
        for (final Map.Entry<String, Integer> entry : dict.entrySet()) {
            final String word = entry.getKey();
            final Integer index = entry.getValue();
            if (index == null || index < 0 || index >= result.length) {
                throw new IOException("inconsistent dictionary, has " + result.length + " entries but an index of " + word + " -> " + index);
            }
            if (result[index] != null) {
                throw new IOException("inconsistent dictionary, both " + result[index] + " and " + word + " map to " + index);
            }
            result[index] = word;
        }
        return result;
    }

    private GOV4Function<String> buildMphFunction() throws IOException {
        final File tempFolder = File.createTempFile("smartDictionarySerializer", ".tmp");
        if (!Files.delete(tempFolder.getAbsolutePath())) {
            throw new IOException("Can't delete tempFolder: " + tempFolder);
        }
        if (!tempFolder.mkdir()) {
            throw new IOException("Can't create tempFolder: " + tempFolder);
        }
        return new GOV4Function.Builder<String>()
                .keys(Arrays.asList(words))
                .tempDir(tempFolder)
                .transform(new TableWriter.SerializerTransformationStrategy<>(new SmartStringSerializer()))
                .build();
    }

    // use default serialization, but compact to just the flat index first
    private void writeObject(final ObjectOutputStream outputStream) throws IOException {
        if (words == null) {
            words = dictionaryToIndex(dictionary);
        }
        dictionary = null;
        if (onlyUsedInValue) {
            mphFunction = null;
        } else {
            mphFunction = buildMphFunction();
        }
        outputStream.defaultWriteObject();
        // To support this serializer used in many configs.
        mphFunction = null;
    }

    private void readObject(final ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        inputStream.defaultReadObject();
        if (words == null) {
            throw new IOException("words can't be null");
        }
        if (!onlyUsedInValue && mphFunction == null) {
            mphFunction = buildMphFunction();
        }
    }
}
