#include <cassert>
#include <iostream>
#include <string>
#include "leveldb/db.h"

int main(int argc, char** argv) {
  assert(argc == 2);
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, argv[1], &db);
  assert(status.ok());
  std::string line;
  while (getline(std::cin, line)) {
    size_t tab = line.find_first_of('\t');
    if (tab != std::string::npos) {
      db->Put(leveldb::WriteOptions(), line.substr(0, tab), line.substr(tab + 1));
    }
  }
  delete db;
}
