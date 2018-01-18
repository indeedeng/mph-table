#!/bin/bash

set -e
DIR=$(dirname $0)

# Benchmark multiple key-value stores.  Install the key-value stores
# you want to benchmark, set your CLASSPATH as described below, and
# run:
#
#  make -C ./bench
#  ./bench/run-bench.sh > bench.out
#  ./bench/report.pl bench.out

# You can set these env variables to restrict to running only certain
# stores, or only certain variants.
ENABLED_DBS="${ENABLED_DBS:-mph|lsm|leveldb|sqlite|discodb|cdb}"
ENABLED_VARIANTS="${ENABLED_VARIANTS:-.*}"

# These are optionally set by command-line options below.
# 50 million should take 1 to 10 minutes for any store on
# a fast machine with SSD.
NUM_KEYS=50000000
MAX_VALUES=10
SINGLETON_RATE=10
ITERS=$( echo {0..0} )

# CLASSPATH must include at least:
#   * indeed.mph-table (>=1.0.3)
#   * indeed.lsmtree-core (>=1.0.7)
#   * indeed.util-{core,io,mmap,serialization} (>=1.0.24)
#   * it.unimi.dsi.fastutil (>=6.5.15)
#   * it.unimi.dsi.sux4j (>=4.0.0)
#   * com.martiansoftware.jsap (>=2.1)
#   * org.apache.commons.commons-math (>=3.6)
CLASSPATH="${CLASSPATH:-/usr/share/java/\*}"

if uname -a | grep --quiet -i darwin; then
    # TODO: update report.pl to parse this output
    TIME="/usr/bin/time -l"
else
    TIME="/usr/bin/time -v"
fi

BASE32_SERIALIZER='.SmartBase32Serializer(80)'
READ_MPH="java -cp $DIR/bench.jar:$CLASSPATH com.indeed.mph.TableReader"
READ_LSMTREE="java -cp $DIR/bench.jar:$CLASSPATH com.indeed.lsmtree.ReadLsmTree"
READ_SQLITE3="$DIR/sqlite3get"
READ_CDB="$DIR/cdbget"
READ_DISCO="$DIR/discoget.py"
READ_LEVELDB="$DIR/leveldbget"  # no real difference from the java perf
READ_LEVELDB_JAVA="java -cp $DIR/bench.jar:/usr/share/java/* com.indeed.leveldb.ReadLevelDb"
WRITE_MPH="java -cp $CLASSPATH com.indeed.mph.TableWriter --withTempStorage"
WRITE_LSMTREE="java -cp $DIR/bench.jar:$CLASSPATH com.indeed.lsmtree.WriteLsmTree"
WRITE_SQLITE3='sqlite3'
WRITE_CDB='cdb -c -m'
WRITE_DISCO="$DIR/tsv2disco.py -"
WRITE_LEVELDB="$DIR/tsv2leveldb"
WRITE_LEVELDB_JAVA="java -cp $DIR/bench.jar:/usr/share/java/* com.indeed.leveldb.WriteLevelDb"

gen_data() {
    echo "---- generating benchmark data: $NUM_KEYS"
    mkdir -p bench/data
    mkdir -p bench/logs
    if [ ! -f bench/data/hash2items.tsv ]; then
        perl -e 'sub rand32{$x=int(rand(32));chr($x+($x<10?ord("0"):ord("a")-10))} sub randitem{$k=join("",map{rand32()}(0..15));$k=~s/([g-v])(..)$/0$2/;$k} for($i=0;$i<'$NUM_KEYS';++$i){$h=int(rand(~0))-(~0>>1)-1;print $h."\t".join(",",map{randitem()}(0..(int(rand('$SINGLETON_RATE'))?0:int(rand('$MAX_VALUES')))))."\n"}' > bench/data/hash2items.tsv
        rm -f bench/data/item2hash.tsv
    fi
    if [ ! -f bench/data/item2hash.tsv ]; then
        perl -F'\t' -ane 'while($F[1]=~/\b(\w+)\b/g){print"$1\t$F[0]\n"}' < bench/data/hash2items.tsv \
            | sort | perl -F'\t' -ane 'print unless $prev eq $F[0]; $prev=$F[0]' \
            | shuf > bench/data/item2hash.tsv
    fi
}

do_write() {
    local OUTPUT="$1"
    shift
    if echo "$OUTPUT" | egrep --quiet "_${ENABLED_VARIANTS}\\.${ENABLED_DBS}\$"; then
        rm -rf "${OUTPUT}"
        echo "---- write ${OUTPUT}"
        $TIME "$@" 2>&1 > "${OUTPUT//\/data\//\/logs\/}.out"
        echo "---- size ${OUTPUT} $(du -sb ${OUTPUT} | cut -f 1)"
    fi
}

do_write_mph() {
    local STORE="$1"
    local VARIANT="$2"
    shift 2
    local INPUT="bench/data/${STORE}.tsv"
    local OUTPUT="bench/data/${STORE}_${VARIANT}.mph"
    do_write "${OUTPUT}" $WRITE_MPH "$@" "${OUTPUT}" "${INPUT}"
}

do_write_lsm() {
    local STORE="$1"
    local VARIANT="$2"
    shift 2
    local INPUT="bench/data/${STORE}.tsv"
    local OUTPUT="bench/data/${STORE}_${VARIANT}.lsm"
    do_write "${OUTPUT}" $WRITE_LSMTREE "$@" "${OUTPUT}" "${INPUT}"
}

do_write_stdin() {
    local IMPL="$1"
    local WRITE="$2"
    local STORE="$3"
    local VARIANT="$4"
    shift 4
    local INPUT="bench/data/${STORE}.tsv"
    local OUTPUT="bench/data/${STORE}_${VARIANT}.${IMPL}"
    if echo "$OUTPUT" | egrep --quiet "_${ENABLED_VARIANTS}\\.${ENABLED_DBS}\$"; then
        rm -rf "${OUTPUT}"
        echo "---- write ${OUTPUT}"
        $TIME $WRITE "$@" "${OUTPUT}" < "${INPUT}" 2>&1 > "${OUTPUT//\/data\//\/logs\/}.out"
        echo "---- size ${OUTPUT} $(du -sb ${OUTPUT} | cut -f 1)"
    fi
}

do_write_leveldb() {
    #do_write_stdin leveldb "$WRITE_LEVELDB_JAVA" "$@"
    do_write_stdin leveldb "$WRITE_LEVELDB" "$@"
}
do_write_cdb() {
    do_write_stdin cdb "$WRITE_CDB" "$@"
}
do_write_discodb() {
    do_write_stdin discodb "$WRITE_DISCO" "$@"
}

do_write_sqlite3() {
    local STORE="$1"
    local VARIANT="$2"
    shift 2
    local OUTPUT="bench/data/${STORE}_${VARIANT}.sqlite3"
    if echo "$OUTPUT" | egrep --quiet "_${ENABLED_VARIANTS}\\.${ENABLED_DBS}\$"; then
        rm -rf "${OUTPUT}"
        echo "---- write ${OUTPUT}"
        (for cmd in "$@"; do echo "$cmd"; done; echo 'VACUUM FULL;') |\
            $TIME sqlite3 "$OUTPUT" 2>&1 > "${OUTPUT//\/data\//\/logs\/}.out"
        echo "---- size ${OUTPUT} $(du -sb ${OUTPUT} | cut -f 1)"
    fi
}

do_read() {
    local INPUT="$1"
    shift
    if echo "$INPUT" | egrep --quiet "_${ENABLED_VARIANTS}\\.${ENABLED_DBS}\$"; then
        echo "---- read ${INPUT}"
        awk '-F\t' '{if($1~/[0-9]/){print$1}}' "${INPUT%_*}.tsv" |\
            $TIME "$@" 2>&1 >/dev/null
    fi
}

bench_write_hash2items() {
    echo "---- writing hash2items"
    INPUT="bench/data/hash2items.tsv"
    for i in $ITERS; do
        KEY_SERIALIZER="--keySerializer .SmartStringSerializer"
        VALUE_SERIALIZER="--valueSerializer .SmartListSerializer(.SmartStringSerializer,254)"
        do_write_mph hash2items str2str $KEY_SERIALIZER $VALUE_SERIALIZER
        do_write_lsm hash2items str2str $KEY_SERIALIZER $VALUE_SERIALIZER

        KEY_SERIALIZER="--keySerializer .SmartLongSerializer"
        do_write_mph hash2items long2str $KEY_SERIALIZER $VALUE_SERIALIZER
        do_write_lsm hash2items long2str $KEY_SERIALIZER $VALUE_SERIALIZER

        VALUE_SERIALIZER="--valueSerializer .SmartListSerializer($BASE32_SERIALIZER,254)"
        do_write_mph hash2items long2base32 $KEY_SERIALIZER $VALUE_SERIALIZER
        do_write_lsm hash2items long2base32 $KEY_SERIALIZER $VALUE_SERIALIZER

        do_write_sqlite3 hash2items long2str \
            'create table hash2items(hash BIGINT, items VARCHAR(256));' \
            'create unique index hash_idx on hash2items(hash);' \
            '.separator "\t"' \
            '.import '"$INPUT"' hash2items'
        do_write_cdb hash2items str2str
        do_write_discodb hash2items str2str
        do_write_leveldb hash2items str2str
    done
}

bench_write_item2hash() {
    echo "---- writing item2hash"
    INPUT="bench/data/item2hash.tsv"
    for i in $ITERS; do
        KEY_SERIALIZER="--keySerializer $BASE32_SERIALIZER"
        VALUE_SERIALIZER="--valueSerializer .SmartLongSerializer"
        do_write_mph item2hash base $KEY_SERIALIZER $VALUE_SERIALIZER
        do_write_mph item2hash nokeys $KEY_SERIALIZER $VALUE_SERIALIZER --keyStorage IMPLICIT --signatureWidth 5
        do_write_mph item2hash hashlink $KEY_SERIALIZER --valueSerializer '.SmartHashSerializer("bench/data/hash2items_long2base32.mph")' --keyStorage IMPLICIT --signatureWidth 5
        do_write_lsm item2hash base $KEY_SERIALIZER $VALUE_SERIALIZER
        do_write_sqlite3 item2hash base \
            'create table item2hash(item VARCHAR(16), hash BIGINT);' \
            'create unique index item_idx on item2hash(item);' \
            '.separator "\t"' \
            '.import '"$INPUT"' item2hash'
        do_write_sqlite3 item2hash doubleidx \
            'create table item2hash(item VARCHAR(16), hash BIGINT);' \
            'create unique index item_idx on item2hash(item);' \
            'create index hash_idx on item2hash(hash);' \
            '.separator "\t"' \
            '.import '"$INPUT"' item2hash'
        do_write_cdb item2hash base
        do_write_discodb item2hash base
        do_write_leveldb item2hash base
    done
}

bench_write() {
    bench_write_hash2items
    bench_write_item2hash
}

bench_read_hash2items() {
    echo "---- reading hash2items"
    for i in $ITERS; do
        for variant in str2str long2str long2base32; do
            case $variant in
                str2*) KEY_SERIALIZER="--keySerializer .SmartStringSerializer" ;;
                *)     KEY_SERIALIZER="--keySerializer .SmartLongSerializer" ;;
            esac
            case $variant in
                *2base32) VALUE_SERIALIZER="--valueSerializer $BASE32_SERIALIZER" ;;
                *)        VALUE_SERIALIZER="--valueSerializer .SmartStringSerializer" ;;
            esac
            INPUT="bench/data/hash2items_${variant}"
            do_read "${INPUT}.mph" $READ_MPH "${INPUT}.mph"
            do_read "${INPUT}.lsm" $READ_LSMTREE $KEY_SERIALIZER $VALUE_SERIALIZER "${INPUT}.lsm"
        done
        INPUT="bench/data/hash2items_str2str"
        do_read "${INPUT//str2str/long2str}.sqlite3" $READ_SQLITE3 "${INPUT//str2str/long2str}.sqlite3" hash2items hash items
        do_read "${INPUT}.cdb" $READ_CDB "${INPUT}.cdb"
        do_read "${INPUT}.discodb" $READ_DISCO "${INPUT}.discodb"
        do_read "${INPUT}.leveldb" $READ_LEVELDB_JAVA "${INPUT}.leveldb"
    done
}

bench_read_item2hash() {
    echo "---- reading item2hash"
    for i in $ITERS; do
        INPUT="bench/data/item2hash"
        KEY_SERIALIZER="--keySerializer $BASE32_SERIALIZER"
        VALUE_SERIALIZER="--valueSerializer .SmartLongSerializer"
        do_read ${INPUT}_base.mph $READ_MPH ${INPUT}_base.mph
        do_read ${INPUT}_nokeys.mph $READ_MPH ${INPUT}_nokeys.mph
        do_read ${INPUT}_hashlink.mph $READ_MPH ${INPUT}_hashlink.mph
        do_read ${INPUT}_base.lsm $READ_LSMTREE $KEY_SERIALIZER $VALUE_SERIALIZER ${INPUT}_base.lsm
        do_read ${INPUT}_base.sqlite3 $READ_SQLITE3 ${INPUT}_base.sqlite3 item2hash item hash
        do_read ${INPUT}_doubleidx.sqlite3 $READ_SQLITE3 ${INPUT}_doubleidx.sqlite3 item2hash item hash
        do_read ${INPUT}_base.cdb $READ_CDB ${INPUT}_base.cdb
        do_read ${INPUT}_base.discodb $READ_DISCO ${INPUT}_base.discodb
        do_read ${INPUT}_base.leveldb $READ_LEVELDB_JAVA ${INPUT}_base.leveldb
    done
}

bench_read() {
    bench_read_hash2items
    bench_read_item2hash
}

run_all() {
    gen_data
    bench_write
    bench_read
}

ran=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        -i|--iters)
            ITERS=$(seq $2 | tr '\n' ' '); shift 2;;
        -n|--num-keys)
            NUM_KEYS="$2"; shift 2;;
        -m|--max-values)
            MAX_VALUES="$2"; shift 2;;
        -s|--singleton-rate)
            SINGLETON_RATE="$2"; shift 2;;
        data|generate-data)
            gen_data; ran=1; shift 1;;
        write|bench-write)
            bench_write; ran=1; shift 1;;
        write-hash2items|bench-write-hash2items)
            bench_write_hash2items; ran=1; shift 1;;
        write-item2hash|bench-write-item2hash)
            bench_write_item2hash; ran=1; shift 1;;
        read|bench-read)
            bench_read; ran=1; shift 1;;
        read-hash2items|bench-read-hash2items)
            bench_read_hash2items; ran=1; shift 1;;
        read-item2hash|bench-read-item2hash)
            bench_read_item2hash; ran=1 shift 1;;
        all)
            run_all; ran=1; shift;;
        clean)
            rm -rf bench/data/*; ran=1; shift;;
        *)
            echo "unknown command: $1"; break;;
    esac
done

if [ -z "$ran" ]; then
    run_all
fi
