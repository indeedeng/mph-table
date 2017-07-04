package com.indeed.mph;

import com.indeed.util.core.Pair;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

/**
 * @author alexs
 */
public class ParseableInputKeyValueIterator<K, V> implements Iterator<Pair<K, V>>, Closeable {
    private static final Logger LOGGER = Logger.getLogger(ParseableInputKeyValueIterator.class);
    private static final int MIN_COUNT_TO_CHECK_ERRORS = 100;
    private final BufferedReader reader;
    private final Parseable<? super K> keyParser;
    private final Parseable<? super V> valueParser;
    private final Pattern separator;
    private final Pattern replace;
    private final String to;
    private final boolean replaceValueOnly;
    private final double maxErrorRatio;
    private K nextKey;
    private V nextValue;
    private long errorCount;
    private long totalCount;

    public ParseableInputKeyValueIterator(final InputStream input,
                                          final Parseable<? super K> keyParser,
                                          final Parseable<? super V> valueParser,
                                          final Pattern separator,
                                          final Pattern replace,
                                          final String to,
                                          final boolean replaceValueOnly,
                                          final double maxErrorRatio) throws IOException {
        this.reader = new BufferedReader(new InputStreamReader(input));
        this.keyParser = keyParser;
        this.valueParser = valueParser;
        this.separator = separator;
        this.replace = replace;
        this.to = to;
        this.replaceValueOnly = replaceValueOnly;
        this.maxErrorRatio = maxErrorRatio;
        this.errorCount = 0;
        this.totalCount = 0;
    }

    public ParseableInputKeyValueIterator(final InputStream input,
                                          final Parseable<? super K> keyParser,
                                          final Parseable<? super V> valueParser,
                                          final Pattern separator,
                                          final Pattern replace,
                                          final String to,
                                          final double maxErrorRatio) throws IOException {
        this(input, keyParser, valueParser, separator, replace, to, false, maxErrorRatio);
    }

    public ParseableInputKeyValueIterator(final InputStream input,
                                          final Parseable<? super K> keyParser,
                                          final Parseable<? super V> valueParser,
                                          final Pattern separator,
                                          final Pattern replace,
                                          final String to,
                                          final boolean ignoreErrors) throws IOException {
        this(input, keyParser, valueParser, separator, replace, to, ignoreErrors ? 1.0 : 0.0);
    }

    public ParseableInputKeyValueIterator(final InputStream input,
                                          final Parseable<? super K> keyParser,
                                          final Parseable<? super V> valueParser,
                                          final String separator,
                                          final String replace,
                                          final String to,
                                          final double maxErrorRatio) throws IOException {
        this(input, keyParser, valueParser, Pattern.compile(separator), replace == null ? null : Pattern.compile(replace), to, maxErrorRatio);
    }

    public ParseableInputKeyValueIterator(final InputStream input,
                                          final Parseable<? super K> keyParser,
                                          final Parseable<? super V> valueParser,
                                          final String separator,
                                          final String replace,
                                          final String to,
                                          final boolean ignoreErrors) throws IOException {
        this(input, keyParser, valueParser, separator, replace, to, ignoreErrors ? 1.0 : 0.0);
    }

    public ParseableInputKeyValueIterator(final InputStream input,
                                          final Parseable<? super K> keyParser,
                                          final Parseable<? super V> valueParser,
                                          final String separator,
                                          final String replace,
                                          final String to,
                                          final boolean replaceValueOnly,
                                          final boolean ignoreErrors) throws IOException {
        this(input, keyParser, valueParser, Pattern.compile(separator), replace == null ? null : Pattern.compile(replace), to, replaceValueOnly, ignoreErrors ? 1.0 : 0.0);
    }

    public ParseableInputKeyValueIterator(final InputStream input,
                                          final Parseable<? super K> keyParser,
                                          final Parseable<? super V> valueParser,
                                          final Pattern separator,
                                          final Pattern replace,
                                          final String to) throws IOException {
        this(input, keyParser, valueParser, separator, replace, to, false);
    }

    public ParseableInputKeyValueIterator(final InputStream input,
                                          final Parseable<? super K> keyParser,
                                          final Parseable<? super V> valueParser,
                                          final Pattern separator,
                                          final Pattern replace) throws IOException {
        this(input, keyParser, valueParser, separator, replace, "");
    }

    public ParseableInputKeyValueIterator(final InputStream input,
                                          final Parseable<? super K> keyParser,
                                          final Parseable<? super V> valueParser,
                                          final Pattern separator) throws IOException {
        this(input, keyParser, valueParser, separator, null);
    }

    public ParseableInputKeyValueIterator(final InputStream input,
                                          final Parseable<? super K> keyParser,
                                          final Parseable<? super V> valueParser,
                                          final String separator) throws IOException {
        this(input, keyParser, valueParser, Pattern.compile(separator), null);
    }

    public ParseableInputKeyValueIterator(final InputStream input, final Parseable<? super K> keyParser, final Parseable<? super V> valueParser) throws IOException {
        this(input, keyParser, valueParser, "\t");
    }

    @Override
    public void close() throws IOException {
        reader.close();
        LOGGER.info("finished iterating with " + errorCount + "/" + totalCount + " errors (" + ((100.0 * errorCount)/totalCount) + "%)");
    }
   
    @Override
    protected void finalize() throws Throwable {
        close();
    }

    @Override
    public boolean hasNext() {
        while (nextKey == null) {
            String line = null;
            try {
                line = reader.readLine();
                if (line == null) {
                    reader.close();
                    break;
                } else {
                    ++totalCount;
                    final String[] keyValue;
                    if (!replaceValueOnly) {
                        final String cleanLine = replace == null ? line : replace.matcher(line).replaceAll(to);
                        keyValue = separator.split(cleanLine, 2);
                    } else {
                        keyValue = separator.split(line, 2);
                        if (keyValue.length == 2) {
                            keyValue[1] = replace == null ? keyValue[1] : replace.matcher(keyValue[1]).replaceAll(to);
                        }
                    }
                    nextKey = (K) keyParser.parseFromString(keyValue[0]);
                    nextValue = (V) (keyValue.length == 2 ? valueParser.parseFromString(keyValue[1]) : null);
                }
            } catch (final Exception e) {
                nextKey = null;
                if (maxErrorRatio > 0.0) {
                    if (++errorCount % 1000 == 0) {
                        LOGGER.warn("error while parsing: " + line, e);
                    }
                    final double errorRatio = ((double) errorCount) / totalCount;
                    if (totalCount > MIN_COUNT_TO_CHECK_ERRORS && errorRatio > maxErrorRatio) {
                        throw new IllegalStateException("too many errors (" + errorCount + "/" + totalCount +
                                                        " = " + (100.0*errorRatio) + "% > " + (maxErrorRatio*100.0) +
                                                        "%) while reading: " + line, e);
                    }
                } else {
                    throw new IllegalStateException("error while reading: " + line, e);
                }
            }
        }
        return nextKey != null;
    }

    @Override
    public Pair<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final Pair<K, V> result = new Pair<>(nextKey, nextValue);
        nextKey = null;
        nextValue = null;
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
