# Minimal Perfect Hash Tables

![NetflixOSS Lifecycle](https://img.shields.io/osslifecycle/indeedeng/mph-table.svg)

## About

Minimal Perfect Hash Tables are an immutable key/value store with
efficient space utilization and fast reads.  They are ideal for the
use-case of tables built by batch processes and shipped to multiple
servers.

## Usage

Indeed MPH is available on [Maven Central](https://mvnrepository.com/artifact/com.indeed/mph-table),
just add the following dependency:
```
<dependency>
    <groupId>com.indeed</groupId>
    <artifactId>mph-table</artifactId>
    <version>1.0.4</version>
</dependency>
```

The primary interfaces are
[TableReader](src/main/java/com/indeed/mph/TableReader.java), to
construct a reader to an existing table,
[TableWriter](src/main/java/com/indeed/mph/TableWriter.java), to build
a table, and
[TableConfig](src/main/java/com/indeed/mph/TableConfig.java), to
specify the configuration for the writer.

How to write a table:
```java
final TableConfig<Long, Long> config = new TableConfig()
    .withKeySerializer(new SmartLongSerializer())
    .withValueSerializer(new SmartVLongSerializer());
final Set<Pair<Long, Long>> entries = new HashSet<>();
for (long i = 0; i < 20; ++i) {
    entries.add(new Pair(i, i * i));
}
TableWriter.write(new File("squares"), config, entries);
```

How to read a table:
```java
try (final TableReader<Long, Long> reader = TableReader.open("squares")) {
  final Long value = reader.get(3L);          // get one
  for (final Pair<Long, Long> p : reader) {   // iterate over all
     ...
  }
}
```

## Command Line

In addition to the Java API, TableReader and TableWriter provide
convenience command-line interfaces to read and write tables, allowing
you to quickly get started without writing any code:

    # print all key-values in a table as TSV
    $ java com.indeed.mph.TableReader --dump <table>

    # print the value for a single key
    $ java com.indeed.mph.TableReader --get <key> <table>

    # create a table from a TSV file of words with counts
    $ java com.indeed.mph.TableWriter --valueSerializer .SmartVLongSerializer <table to create> <counts.tsv>

    # create a table from a TSV file mapping movie ids to lists of actor names (compressed by reference)
    $ java com.indeed.mph.TableWriter --keySerializer .SmartVLongSerializer --valueSerializer '.SmartListSerializer(.SmartDictionarySerializer)' <table to create> <movies.tsv>

    # same as above, not actually storing the movie ids but still allowing retrieval by them
    $ java com.indeed.mph.TableWriter --keyStorage IMPLICIT --keySerializer .SmartVLongSerializer --valueSerializer '.SmartListSerializer(.SmartDictionarySerializer)' <table to create> <movies.tsv>

## Code of Conduct
This project is governed by the [Contributor Covenant v 1.4.1](CODE_OF_CONDUCT.md)

## License

This project is licensed under the Apache-2.0 License - see the [LICENSE](LICENSE) file for details.
