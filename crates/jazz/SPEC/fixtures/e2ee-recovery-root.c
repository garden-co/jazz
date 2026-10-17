/* Independent recovery-root v1 transcript. Compile and pipe stdout to base64. */
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static void size(uint32_t n) {
  const unsigned char bytes[] = { n >> 24, n >> 16, n >> 8, n };
  fwrite(bytes, 1, 4, stdout);
}
int main(int argc, char **argv) {
  const int delivery = argc == 2 && strcmp(argv[1], "delivery") == 0;
  if (argc > 1 && !delivery) return 2;
  const char *fields[] = {
    "fixture", "jazz.e2ee.recovery.v1", "account", "account-id",
    delivery ? "__e2ee_recovery_deliveries" : "__e2ee_recovery_roots", "11111111-1111-4111-8111-111111111111",
    delivery ? "envelope" : "[\"root\",\"33333333-3333-4333-8333-333333333333\"]",
    "22222222-2222-4222-8222-222222222222", delivery ? "33333333-3333-4333-8333-333333333333" : ""
  };
  uint32_t length = 5;
  for (unsigned i = 0; i < 9; i++) length += 4 + (uint32_t)strlen(fields[i]);
  if (!delivery) size(length);
  fwrite("JE2C\001", 1, 5, stdout);
  for (unsigned i = 0; i < 9; i++) {
    size((uint32_t)strlen(fields[i]));
    fwrite(fields[i], 1, strlen(fields[i]), stdout);
  }
  if (delivery) return ferror(stdout) ? 1 : 0;
  const unsigned char signing[] = {
    'J', 'E', '2', 'E', 1, 4, 's', 'i', 'g', 'n', 0, 0, 0, 1, 0xaa
  };
  const unsigned char recipient[] = {
    'J', 'E', '2', 'E', 1, 3, 'k', 'e', 'y', 0, 0, 0, 2, 0xbb, 0xcc
  };
  size(sizeof signing);
  fwrite(signing, 1, sizeof signing, stdout);
  size(sizeof recipient);
  fwrite(recipient, 1, sizeof recipient, stdout);
  return ferror(stdout) ? 1 : 0;
}
