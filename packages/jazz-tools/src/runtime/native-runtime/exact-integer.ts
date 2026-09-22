const I64_MIN = -(1n << 63n);
const I64_MAX = (1n << 63n) - 1n;

/**
 * Converts a JavaScript integer to the exact signed i64 value accepted by
 * native storage and postcard. Numbers must remain inside JavaScript's exact
 * integer range; callers that need the rest of i64 must use bigint.
 */
export function exactSignedI64(value: bigint | number, label: string): bigint {
  if (typeof value === "number" && !Number.isSafeInteger(value)) {
    throw new Error(`${label} must be a safe integer when passed as a number, got ${value}`);
  }
  const integer = BigInt(value);
  if (integer < I64_MIN || integer > I64_MAX) {
    throw new Error(`${label} must be a signed 64-bit integer`);
  }
  return integer;
}
