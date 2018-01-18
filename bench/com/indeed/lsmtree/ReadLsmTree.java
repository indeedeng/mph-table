package com.indeed.lsmtree;

import com.indeed.util.serialization.Serializer;
import com.indeed.mph.SmartSerializer;
import com.indeed.mph.TableWriter;
import com.indeed.mph.serializers.SmartStringSerializer;
import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.core.StoreBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class ReadLsmTree {
    public static void main(final String[] args) throws IOException {
        SmartSerializer keySerializer = new SmartStringSerializer();
        SmartSerializer valueSerializer = new SmartStringSerializer();
        boolean quiet = false;
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
            case "--quiet":
                quiet = true; break;
            default:
                throw new RuntimeException("unknown option: " + args[i]);
            }
        }
        if (args.length - i != 1) {
            throw new RuntimeException("usage: ReadLsmTree [options] <input>");
        }
        final long startTime = System.currentTimeMillis();
        final Store<Object, Object> reader =
            new StoreBuilder<>(new File(args[i]), keySerializer, valueSerializer).build();
        final Scanner in = new Scanner(System.in);
        while (in.hasNextLine()) {
            try {
                final Object value = reader.get(keySerializer.parseFromString(in.nextLine()));
                if (!quiet) {
                    System.out.println(valueSerializer.printToString(value));
                }
            } catch (final Exception e) {
            }
        }
        System.out.println("completed in " + (System.currentTimeMillis() - startTime) + " ms");
    }
}
