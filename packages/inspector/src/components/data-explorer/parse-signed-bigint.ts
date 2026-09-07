const SIGNED_I64_MIN = -(1n << 63n);
const SIGNED_I64_MAX = (1n << 63n) - 1n;
const SIGNED_DECIMAL_INTEGER = /^[+-]?\d+$/;

export function parseSignedBigInt64(value: string): bigint {
  if (!SIGNED_DECIMAL_INTEGER.test(value)) {
    throw new Error("Value must be a signed 64-bit integer.");
  }

  const parsed = BigInt(value);
  if (parsed < SIGNED_I64_MIN || parsed > SIGNED_I64_MAX) {
    throw new Error("Value must be a signed 64-bit integer.");
  }

  return parsed;
}
