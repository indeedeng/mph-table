#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>

#define KEY_SIZE 128
#define QUERY_SIZE 256
#define VALUE_SIZE 1024

int main(int argc, char** argv) {
  sqlite3* db = NULL;
  sqlite3_stmt* get_stmt = NULL;
  char key[KEY_SIZE], value[VALUE_SIZE], query[QUERY_SIZE], *dbfile, *table, *key_field, *value_field;
  int err, type=-1;
  if (argc != 5) {
    fprintf(stderr, "usage: sqlite3_lookup <db> <table> <key-field> <value-field>\n");
    return -1;
  }
  dbfile = argv[1];
  table = argv[2];
  key_field = argv[3];
  value_field = argv[4];
  snprintf(query, QUERY_SIZE, "SELECT %s FROM %s WHERE %s=?;", value_field, table, key_field);
  if (sqlite3_open(dbfile, &db) != SQLITE_OK) {
    fprintf(stderr, "ERROR: Can't open database: %s\n", sqlite3_errmsg(db));
    return -1;
  }
  if (sqlite3_prepare_v2(db, query, -1, &get_stmt, NULL) != SQLITE_OK) {
    fprintf(stderr, "ERROR: Can't prepare query: %s: %s\n", query, sqlite3_errmsg(db));
    return -1;
  }
  while (fgets(key, KEY_SIZE, stdin) != NULL) {
    key[strnlen(key, KEY_SIZE) - 1] = '\0';
    sqlite3_reset(get_stmt);
    if (sqlite3_bind_text(get_stmt, 1, key, -1, NULL) != SQLITE_OK)
      fprintf(stderr, "ERROR: couldn't bind key: %s: %s\n", key, sqlite3_errmsg(db));
    err = sqlite3_step(get_stmt);
    if (err == SQLITE_OK || err == SQLITE_ROW) {
      if (type < 0)
        type = sqlite3_column_type(get_stmt, 0);
      if (type == SQLITE_INTEGER)
        printf("%lld\n", sqlite3_column_int64(get_stmt, 0));
      else
        printf("%s\n", sqlite3_column_text(get_stmt, 0));
    } else {
      fprintf(stderr, "ERROR: couldn't read key: %s: %s\n", key, sqlite3_errmsg(db));
    }
  }
  return 0;
}
