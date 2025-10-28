/**
 * Remove getters from an object
 *
 * @param obj - The object to remove getters from.
 * @returns A new object with the getters removed.
 */
export function removeGetters<T extends object>(obj: T): Partial<T> {
  const result: any = {};

  for (const key of Object.keys(obj)) {
    const descriptor = Object.getOwnPropertyDescriptor(obj, key);
    if (!descriptor?.get) {
      result[key] = (obj as any)[key];
    }
  }

  return result;
}
