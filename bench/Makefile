
CLASSPATH ?= "/usr/share/java/\*"

all: cdbget sqlite3get leveldbget bench.jar

cdbget: cdbget.c
	cc -O3 -g -o $@ $< -lcdb

sqlite3get: sqlite3get.c
	cc -O3 -o $@ $< -lsqlite3

leveldbget: leveldbget.cc
	c++ -O3 -g -o $@ $< -lleveldb

%.class: %.java
	javac -cp $(CLASSPATH) $<

bench.jar: com/indeed/leveldb/ReadLevelDb.class com/indeed/leveldb/WriteLevelDb.class com/indeed/lsmtree/ReadLsmTree.class com/indeed/lsmtree/WriteLsmTree.class
	jar cvf $@ com
