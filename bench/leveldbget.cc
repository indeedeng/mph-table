#include <cassert>
#include <iostream>
#include <string>
#include <leveldb/db.h>

int main(int argc, char** argv) {
  assert(argc == 2);
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, argv[1], &db);
  assert(status.ok());
  std::string line;
  std::string value;
  while (getline(std::cin, line)) {
    db->Get(leveldb::ReadOptions(), line, &value);
    std::cout << value << "\n";
  }
  delete db;
}
