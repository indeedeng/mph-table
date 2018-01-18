#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cdb.h>

int main(int argc, char** argv) {
  int fd;
  struct cdb cdb;
  char key[128], data[1024];
  unsigned int keylen, datalen;

  fd = open(argv[1], O_RDONLY);
  cdb_init(&cdb, fd);
  while (fgets(key, 128, stdin) != NULL) {
    keylen = strnlen(key, 128);
    if (key[keylen-1] == '\n') {
      key[--keylen] = '\0';
    }
    if (cdb_find(&cdb, key, keylen) > 0) {
      datalen = cdb_datalen(&cdb);
      cdb_read(&cdb, data, datalen, cdb_datapos(&cdb));
      data[datalen] = '\0';
      printf("%s\n", data);
    } else {
      /* fprintf(stderr, "key=%s not found\n", key); */
    }
  }
  cdb_free(&cdb);
  close(fd);
}
