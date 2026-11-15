/* Links libjazz_ffi.a and drives a local app through the C ABI. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>

#include "jazz_ffi.h"

#define CHECK(cond)                                                              \
  do {                                                                           \
    if (!(cond)) {                                                               \
      fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #cond);   \
      exit(1);                                                                   \
    }                                                                            \
  } while (0)

static const char *APP =
    "{\"format\": 1, \"tables\": {\"todos\": {\"columns\": "
    "{\"title\": \"string\", \"done\": \"boolean\", \"rank\": \"float\"}}}}";

struct seen {
  pthread_mutex_t lock;
  int deltas;
  int resets;
};

static void on_event(void *ctx, const char *kind, const char *json) {
  struct seen *seen = ctx;
  pthread_mutex_lock(&seen->lock);
  if (strcmp(kind, "delta") == 0) {
    seen->deltas++;
    if (strstr(json, "\"reset\":true")) seen->resets++;
  }
  pthread_mutex_unlock(&seen->lock);
}

/* A callback that is still running when jazz_unsubscribe is called. */
struct slow {
  pthread_mutex_t lock;
  int entered;
  int finished;
};

static void on_slow(void *ctx, const char *kind, const char *json) {
  (void)kind;
  (void)json;
  struct slow *slow = ctx;
  pthread_mutex_lock(&slow->lock);
  slow->entered++;
  pthread_mutex_unlock(&slow->lock);
  struct timespec pause = {0, 200 * 1000 * 1000};
  nanosleep(&pause, NULL);
  pthread_mutex_lock(&slow->lock);
  slow->finished++;
  pthread_mutex_unlock(&slow->lock);
}

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
  char config[512];
  char *err = NULL;
  /* An app config needs the app's text as a JSON string: escape APP. */
  char escaped[400];
  size_t j = 0;
  for (const char *p = APP; *p; p++) {
    if (*p == '"') escaped[j++] = '\\';
    escaped[j++] = *p;
  }
  escaped[j] = 0;
  snprintf(config, sizeof config, "{\"app\": \"%s\"}", escaped);

  JazzHandle *h = jazz_open(config, &err);
  if (!h) {
    fprintf(stderr, "open failed: %s\n", err);
    return 1;
  }

  struct seen seen = {PTHREAD_MUTEX_INITIALIZER, 0, 0};
  JazzSubscription *sub = jazz_subscribe(h, "{\"table\": \"todos\"}", on_event, &seen, &err);
  CHECK(sub != NULL);

  char *row = call(h, "insert",
                   "{\"table\": \"todos\", \"row\": {\"title\": \"milk\", \"done\": false, \"rank\": 1.5}}");
  CHECK(strstr(row, "\"title\":\"milk\"") != NULL);
  CHECK(strstr(row, "\"rank\":1.5") != NULL);
  jazz_string_free(row);

  char *rows = call(h, "all", "{\"query\": {\"table\": \"todos\", \"where\": "
                              "[{\"column\": \"done\", \"op\": \"eq\", \"value\": false}]}}");
  CHECK(strstr(rows, "milk") != NULL);
  jazz_string_free(rows);

  char *account = call(h, "account", "{}");
  CHECK(strcmp(account, "null") == 0);
  jazz_string_free(account);

  /* An error comes back as JSON in *err. */
  CHECK(jazz_call(h, "insert", "{\"table\": \"nope\", \"row\": {}}", &err) == NULL);
  CHECK(err != NULL && strstr(err, "\"kind\":\"refused\"") != NULL);
  jazz_string_free(err);
  err = NULL;

  for (int i = 0; i < 500; i++) {
    pthread_mutex_lock(&seen.lock);
    int deltas = seen.deltas;
    pthread_mutex_unlock(&seen.lock);
    if (deltas >= 2) break;
    struct timespec ten_ms = {0, 10 * 1000 * 1000};
    nanosleep(&ten_ms, NULL);
  }
  pthread_mutex_lock(&seen.lock);
  CHECK(seen.deltas >= 2);
  CHECK(seen.resets == 1);
  pthread_mutex_unlock(&seen.lock);

  jazz_unsubscribe(sub);

  /* jazz_unsubscribe waits for a running callback: ctx may be freed after. */
  struct slow *slow = calloc(1, sizeof *slow);
  pthread_mutex_init(&slow->lock, NULL);
  JazzSubscription *slow_sub = jazz_subscribe(h, "{\"table\": \"todos\"}", on_slow, slow, &err);
  CHECK(slow_sub != NULL);
  for (int i = 0; i < 500; i++) {
    pthread_mutex_lock(&slow->lock);
    int entered = slow->entered;
    pthread_mutex_unlock(&slow->lock);
    if (entered) break;
    struct timespec ten_ms = {0, 10 * 1000 * 1000};
    nanosleep(&ten_ms, NULL);
  }
  jazz_unsubscribe(slow_sub);
  CHECK(slow->entered == 1);
  CHECK(slow->finished == slow->entered);
  pthread_mutex_destroy(&slow->lock);
  free(slow);

  jazz_close(h);
  jazz_string_free(NULL);
  printf("jazz_ffi C ABI: ok\n");
  return 0;
}
