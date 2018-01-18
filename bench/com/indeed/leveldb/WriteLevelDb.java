package com.indeed.leveldb;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import java.io.File;
import java.util.Scanner;

public class WriteLevelDb {
    public static void main(final String[] args) {
        final Options options = new Options();
        options.createIfMissing(true);
        try (final DB db = factory.open(new File(args[0]), options)) {
            final Scanner in = new Scanner(System.in);
            while (in.hasNextLine()) {
                final String line = in.nextLine();
                final int tab = line.indexOf('\t');
                if (tab >= 0) {
                    db.put(bytes(line.substring(0, tab)), bytes(line.substring(tab + 1)));
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("failed to write", e);
        }
    }
}
