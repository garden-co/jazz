/* Independent group record framing. Compile, run with root or delivery, then base64. */
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define GROUP "11111111-1111-4111-8111-111111111111"
#define EPOCH "22222222-2222-4222-8222-222222222222"
#define DEVICE "33333333-3333-4333-8333-333333333333"
#define ACCOUNT_EPOCH "44444444-4444-4444-8444-444444444444"
#define DELIVERY "55555555-5555-4555-8555-555555555555"
#define RECIPIENT "66666666-6666-4666-8666-666666666666"

static void size(uint32_t n) {
  unsigned char bytes[] = { n >> 24, n >> 16, n >> 8, n };
  fwrite(bytes, 1, 4, stdout);
}
static void field(const char *value) {
  size((uint32_t)strlen(value));
  fwrite(value, 1, strlen(value), stdout);
}
int main(int argc, char **argv) {
  if (argc != 2 || (strcmp(argv[1], "root") && strcmp(argv[1], "delivery"))) return 2;
  int root = !strcmp(argv[1], "root");
  const char *column = root
    ? "[\"root\",\"account-id\",\"" DEVICE "\",\"" ACCOUNT_EPOCH "\"]"
    : "[\"delivery\",\"" DELIVERY "\",\"account-id\",\"" DEVICE "\"]";
  const char *recipient = root ? "" : "[\"account-id\",\"" RECIPIENT "\"]";
  const char *fields[] = { "fixture", "jazz.e2ee.group.v1", "group", GROUP,
    "__e2ee_groups", GROUP, column, EPOCH, recipient };
  uint32_t length = 5;
  for (unsigned i = 0; i < 9; i++) length += 4 + (uint32_t)strlen(fields[i]);
  size(length);
  fwrite("JE2C\001", 1, 5, stdout);
  for (unsigned i = 0; i < 9; i++) field(fields[i]);
  if (root) {
    const unsigned char envelope[] = {
      'J', 'E', '2', 'E', 1, 4, 't', 'e', 's', 't', 0, 0, 0, 1, 0xaa
    };
    size(sizeof envelope);
    fwrite(envelope, 1, sizeof envelope, stdout);
  } else {
    const unsigned char envelope[] = { 0xaa, 0xbb };
    size(sizeof envelope);
    fwrite(envelope, 1, sizeof envelope, stdout);
  }
  return ferror(stdout) ? 1 : 0;
}
