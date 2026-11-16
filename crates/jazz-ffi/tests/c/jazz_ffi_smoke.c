/* A portable smoke test of the C ABI (no threads, standard C only), for the
 * platforms jazz_ffi_test.c (POSIX) doesn't build on, such as Windows. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "jazz_ffi.h"

#define CHECK(cond)                                                              \
  do {                                                                           \
    if (!(cond)) {                                                               \
      fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #cond);   \
      exit(1);                                                                   \
    }                                                                            \
  } while (0)

static const char *CONFIG =
    "{\"app\": \"{\\\"format\\\": 1, \\\"tables\\\": {\\\"todos\\\": {\\\"columns\\\": "
    "{\\\"title\\\": \\\"string\\\", \\\"done\\\": \\\"boolean\\\"}}}}\"}";

static char *call(JazzHandle *h, const char *method, const char *args) {
  char *err = NULL;
  char *out = jazz_call(h, method, args, &err);
  if (!out) {
    fprintf(stderr, "%s failed: %s\n", method, err ? err : "(no error)");
    exit(1);
  }
  CHECK(err == NULL);
  return out;
}

int main(void) {
  char *err = NULL;
  JazzHandle *h = jazz_open(CONFIG, &err);
  if (!h) {
    fprintf(stderr, "open failed: %s\n", err ? err : "(no error)");
    return 1;
  }

  char *row = call(h, "insert", "{\"table\": \"todos\", \"row\": {\"title\": \"milk\", \"done\": false}}");
  CHECK(strstr(row, "\"title\":\"milk\"") != NULL);
  jazz_string_free(row);

  char *rows = call(h, "all", "{\"query\": {\"table\": \"todos\"}}");
  CHECK(strstr(rows, "milk") != NULL);
  jazz_string_free(rows);

  CHECK(jazz_call(h, "insert", "{\"table\": \"nope\", \"row\": {}}", &err) == NULL);
  CHECK(err != NULL && strstr(err, "\"kind\":\"refused\"") != NULL);
  jazz_string_free(err);

  jazz_close(h);
  puts("jazz-ffi smoke test passed");
  return 0;
}
