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
