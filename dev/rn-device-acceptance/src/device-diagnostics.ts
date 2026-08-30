export const DEVICE_DIAGNOSTIC_CODES = [
  "fixture-metadata-failed",
  "native-admission-failed",
  "relay-command-abi-failed",
  "relay-open-failed",
  "relay-attach-failed",
  "relay-probe-failed",
  "relay-cleanup-failed",
  "foreground-byte-abi-failed",
  "logout-revocation-failed",
  "public-client-seed-failed",
  "scope-isolation-failed",
  "auth-switch-failed",
  "foreground-write-failed",
  "same-runtime-subscription-failed",
  "scope-reopen-failed",
  "public-client-restart-failed",
  "receipt-write-failed",
] as const;

export type DeviceDiagnosticCode = (typeof DEVICE_DIAGNOSTIC_CODES)[number];

export function isDeviceDiagnosticCode(value: string): value is DeviceDiagnosticCode {
  return (DEVICE_DIAGNOSTIC_CODES as readonly string[]).includes(value);
}
