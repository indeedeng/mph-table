package com.indeed.lsmtree;

import com.indeed.util.core.Pair;
import com.indeed.util.serialization.Serializer;
import com.indeed.mph.Parseable;
import com.indeed.mph.SmartSerializer;
import com.indeed.mph.TableWriter;
import com.indeed.mph.serializers.SmartStringSerializer;
import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.core.StoreBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WriteLsmTree {
    public static void main(final String[] args) throws IOException {
        SmartSerializer keySerializer = new SmartStringSerializer();
        SmartSerializer valueSerializer = new SmartStringSerializer();
        int i = 0;
        parse_opts:
        for ( ; i < args.length && args[i].startsWith("-"); ++i) {
            switch (args[i]) {
            case "--":
                break parse_opts;
            case "--keySerializer":
                keySerializer = TableWriter.parseSerializer(args[++i]); break;
            case "--valueSerializer":
                valueSerializer = TableWriter.parseSerializer(args[++i]); break;
            default:
                throw new RuntimeException("unknown option: " + args[i]);
            }
        }
        if (args.length - i < 2) {
            throw new RuntimeException("usage: WriteLsmTree [options] <dir> <inputs> ...");
        }
        final long startTime = System.currentTimeMillis();
        final File outputDir = new File(args[i]);
        final List<File> files = new ArrayList<>();
        for (int j = i + 1; j < args.length; ++j) {
            files.add(new File(args[j]));
        }
        final Store<Object, Object> store =
            new StoreBuilder<Object, Object>(outputDir, (Serializer<Object>) keySerializer, (Serializer<Object>) valueSerializer)
               .build();
        for (final Pair<Object, Object> p : new TableWriter.TsvFileReader<Object, Object>(files, (Parseable<Object>) keySerializer, (Parseable<Object>) valueSerializer, "\t", null, null, 0.0)) {
            store.put(p.getFirst(), p.getSecond());
        }
        store.close();
        System.out.println("completed in " + (System.currentTimeMillis() - startTime) + " ms");
    }
}
