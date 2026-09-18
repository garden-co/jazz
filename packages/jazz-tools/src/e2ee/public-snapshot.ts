// Compare complete public snapshots, including bytes and authority positions.
// Unsupported objects miss the cache rather than weakening validation.
export function sameSnapshotValue(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (!left || !right || typeof left !== "object" || typeof right !== "object") return false;
  if (Object.getPrototypeOf(left) !== Object.getPrototypeOf(right)) return false;
  if (left instanceof Uint8Array && right instanceof Uint8Array)
    return left.length === right.length && left.every((byte, index) => byte === right[index]);
  if (left instanceof Set && right instanceof Set)
    return (
      left.size === right.size &&
      [...left].every((value) => typeof value === "string" && right.has(value))
    );
  if (left instanceof Map && right instanceof Map)
    return (
      left.size === right.size &&
      [...left].every(([key, value]) => right.has(key) && sameSnapshotValue(value, right.get(key)))
    );
  if (Array.isArray(left) && Array.isArray(right))
    return (
      left.length === right.length &&
      left.every((value, index) => sameSnapshotValue(value, right[index]))
    );
  if (Object.getPrototypeOf(left) !== Object.prototype) return false;
  const keys = Object.keys(left);
  return (
    keys.length === Object.keys(right).length &&
    keys.every(
      (key) =>
        Object.hasOwn(right, key) &&
        sameSnapshotValue(
          (left as Record<string, unknown>)[key],
          (right as Record<string, unknown>)[key],
        ),
    )
  );
}
