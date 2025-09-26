/**
 * Pick a subset of keys from an object
 * @param obj - The object to pick keys from
 * @param keys - The keys to pick
 * @returns A new object with the picked keys
 */
export function pick<T extends object, K extends keyof T>(
  obj: T,
  keys: K[],
): Pick<T, K> {
  const result = {} as Pick<T, K>;
  for (const key of keys) {
    if (key in obj) {
      result[key] = obj[key];
    }
  }
  return result;
}

function isObject(val: unknown): val is Record<string, unknown> {
  return val !== null && typeof val === "object";
}

/**
 * Performs a structural equality check on two JSON values.
 */
export function structuralEquals(a: unknown, b: unknown): boolean {
  if (a === b) return true;

  // Handle NaN (since NaN !== NaN)
  if (typeof a === "number" && typeof b === "number" && isNaN(a) && isNaN(b)) {
    return true;
  }

  // Primitive types (number, string, boolean, null, undefined) already handled
  if (!isObject(a) || !isObject(b)) {
    return false;
  }

  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b)) return false;
    if (a.length !== b.length) return false;

    for (let i = 0; i < a.length; i++) {
      if (!structuralEquals(a[i], b[i])) return false;
    }
    return true;
  }

  const keysA = Object.keys(a);
  const keysB = Object.keys(b);
  if (keysA.length !== keysB.length) return false;

  for (const key of keysA) {
    if (!Object.prototype.hasOwnProperty.call(b, key)) return false;
    if (!structuralEquals((a as any)[key], (b as any)[key])) return false;
  }

  return true;
}
