# Minimal Perfect Hash Tables

## About

Minimal Perfect Hash Tables are an immutable key/value store with
efficient space utilization and fast reads.  They are ideal for the
use-case of tables built by batch processes and shipped to multiple
servers.

## Usage

The primary interfaces are TableReader, to construct a reader to an
existing table, TableWriter, to build a table, and TableConfig, to
specify the configuration for the writer.  In addition, TableReader
and TableWriter provide convenience main methods to read and write
tables without writing any code:

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

## License

This project is licensed under the Apache-2.0 License - see the [LICENSE](LICENSE) file for details.
