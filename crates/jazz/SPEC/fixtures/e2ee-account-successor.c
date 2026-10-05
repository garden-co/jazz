/* Independent format fixture: cc this file, then pipe its stdout through base64. */
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static void field(const char *value) {
  uint32_t n = (uint32_t)strlen(value);
  unsigned char size[] = { n >> 24, n >> 16, n >> 8, n };
  fwrite(size, 1, 4, stdout);
  fwrite(value, 1, n, stdout);
}

int main(void) {
  const char *context[] = {
    "fixture", "jazz.e2ee.account-successor.v1", "account", "account-id",
    "__e2ee_account_successors", "11111111-1111-4111-8111-111111111111", "signature",
    "33333333-3333-4333-8333-333333333333", "44444444-4444-4444-8444-444444444444"
  };
  const char *fields[] = {
    "22222222-2222-4222-8222-222222222222", "55555555-5555-4555-8555-555555555555",
    "[\"44444444-4444-4444-8444-444444444444\"]", "[]", "\002", "\003",
    "[[\"44444444-4444-4444-8444-444444444444\",[1]]]"
  };
  fwrite("JE2C\001", 1, 5, stdout);
  for (unsigned i = 0; i < 9; i++) field(context[i]);
  for (unsigned i = 0; i < 7; i++) field(fields[i]);
  return ferror(stdout) ? 1 : 0;
}
