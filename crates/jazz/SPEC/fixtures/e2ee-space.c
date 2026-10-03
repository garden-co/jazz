/* Independent space transcript fixture. Compile; run root, grant or delivery; base64. */
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#define ROOT "11111111-1111-4111-8111-111111111111"
#define EPOCH "22222222-2222-4222-8222-222222222222"
#define DEVICE "33333333-3333-4333-8333-333333333333"
#define ACCOUNT_EPOCH "44444444-4444-4444-8444-444444444444"
#define GRANT "55555555-5555-4555-8555-555555555555"
#define DELIVERY "66666666-6666-4666-8666-666666666666"
#define SCOPE "77777777-7777-4777-8777-777777777777"
#define IDENTIFIER "88888888-8888-4888-8888-888888888888"
#define RECIPIENT "99999999-9999-4999-8999-999999999999"
static void size(uint32_t n) {
  unsigned char b[] = {n >> 24, n >> 16, n >> 8, n};
  fwrite(b, 1, 4, stdout);
}
static void field(const char *s) { size((uint32_t)strlen(s)); fwrite(s, 1, strlen(s), stdout); }
int main(int argc, char **argv) {
  if (argc != 2) return 2;
  int root = !strcmp(argv[1], "root"), grant = !strcmp(argv[1], "grant");
  if (!root && !grant && strcmp(argv[1], "delivery")) return 2;
  const char *column = root
    ? "[\"root\",\"account-id\",\"" DEVICE "\",\"" ACCOUNT_EPOCH "\",\"" GRANT "\"]"
    : grant ? "[\"grant\",\"" GRANT "\",\"account-id\",\"" DEVICE "\",\"" ACCOUNT_EPOCH "\",\"add\"]"
    : "[\"delivery\",\"" DELIVERY "\",\"account-id\",\"" DEVICE "\",\"" ACCOUNT_EPOCH "\"]";
  const char *recipient = root ? "" : grant
    ? "[\"account\",\"account-id\",\"" ACCOUNT_EPOCH "\"]"
    : "[\"account-id\",\"" RECIPIENT "\",\"" ACCOUNT_EPOCH "\"]";
  const char *fields[] = {"fixture", "jazz.e2ee.space.v1", SCOPE, IDENTIFIER,
    "__e2ee_spaces", ROOT, column, EPOCH, recipient};
  uint32_t length = 5;
  for (unsigned i = 0; i < 9; i++) length += 4 + (uint32_t)strlen(fields[i]);
  if (!grant) size(length);
  fwrite("JE2C\001", 1, 5, stdout);
  for (unsigned i = 0; i < 9; i++) field(fields[i]);
  if (root) {
    const unsigned char verification[] = {'J','E','2','E',1,4,'t','e','s','t',0,0,0,1,0xaa};
    size(sizeof verification); fwrite(verification, 1, sizeof verification, stdout);
  }
  if (!grant) {
    const unsigned char envelope[] = {0xbb, 0xcc};
    size(sizeof envelope); fwrite(envelope, 1, sizeof envelope, stdout);
  }
  return ferror(stdout) ? 1 : 0;
}
