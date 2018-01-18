package com.indeed.leveldb;

import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class ReadLevelDb {
    public static void main(final String[] args) {
        final Options options = new Options();
        try (final DB db = factory.open(new File(args[0]), options)) {
            final Scanner in = new Scanner(System.in);
            while (in.hasNextLine()) {
                final String line = in.nextLine();
                System.out.println(new String(db.get(bytes(line)), StandardCharsets.UTF_8));
            }
        } catch (final Exception e) {
            throw new RuntimeException("failed to read", e);
        }
    }
}
