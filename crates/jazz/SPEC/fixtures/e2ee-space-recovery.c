/* Independent JE2C v1 space-recovery frame. Emit raw bytes; hex externally. */
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#define ROOT "11111111-1111-4111-8111-111111111111"
#define EPOCH "22222222-2222-4222-8222-222222222222"
#define DEVICE "33333333-3333-4333-8333-333333333333"
#define ACCOUNT_EPOCH "44444444-4444-4444-8444-444444444444"
#define DELIVERY "55555555-5555-4555-8555-555555555555"
#define RECOVERY "66666666-6666-4666-8666-666666666666"
#define SCOPE "77777777-7777-4777-8777-777777777777"
#define IDENTIFIER "88888888-8888-4888-8888-888888888888"
static void size(uint32_t n) {
  unsigned char b[] = {n >> 24, n >> 16, n >> 8, n};
  fwrite(b, 1, 4, stdout);
}
static void field(const char *s) { size((uint32_t)strlen(s)); fwrite(s, 1, strlen(s), stdout); }
int main(void) {
  const char *fields[] = {"fixture", "jazz.e2ee.space.v1", SCOPE, IDENTIFIER,
    "__e2ee_spaces", ROOT,
    "[\"recovery-delivery\",\"" DELIVERY "\",\"sender\",\"" DEVICE "\",\"" ACCOUNT_EPOCH "\"]",
    EPOCH, "[\"recipient\",\"" RECOVERY "\",\"" ACCOUNT_EPOCH "\"]"};
  uint32_t length = 5;
  for (unsigned i = 0; i < 9; i++) length += 4 + (uint32_t)strlen(fields[i]);
  size(length);
  fwrite("JE2C\001", 1, 5, stdout);
  for (unsigned i = 0; i < 9; i++) field(fields[i]);
  const unsigned char envelope[] = {1, 2, 3};
  size(3); fwrite(envelope, 1, 3, stdout);
  return ferror(stdout) ? 1 : 0;
}
