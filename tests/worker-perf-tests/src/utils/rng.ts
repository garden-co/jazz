/**
 * Mulberry32 PRNG - deterministic random number generator.
 */
export function makeRng(seed: number): () => number {
  let t = seed >>> 0;
  return () => {
    t += 0x6d2b79f5;
    let x = t;
    x = Math.imul(x ^ (x >>> 15), x | 1);
    x ^= x + Math.imul(x ^ (x >>> 7), x | 61);
    return ((x ^ (x >>> 14)) >>> 0) / 4294967296;
  };
}

/**
 * Pick a random element from an array.
 */
export function pick<T>(arr: T[], rng: () => number): T {
  return arr[Math.floor(rng() * arr.length)]!;
}

/**
 * Generate a random size between min and max bytes.
 */
export function randomSizeInRange(
  minBytes: number,
  maxBytes: number,
  seed: number,
): number {
  const rng = makeRng(seed);
  return Math.floor(minBytes + rng() * (maxBytes - minBytes + 1));
}

/**
 * Generate a random string of approximately targetBytes size.
 */
export function generateSizedPayload(
  targetBytes: number,
  seed: number,
): string {
  const rng = makeRng(seed);
  const chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let result = "";
  for (let i = 0; i < targetBytes; i++) {
    result += chars[Math.floor(rng() * chars.length)];
  }
  return result;
}
