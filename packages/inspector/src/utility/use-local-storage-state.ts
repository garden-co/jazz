import { useCallback, useState, type Dispatch, type SetStateAction } from "react";

interface UseLocalStorageStateOptions<T> {
  /**
   * Validates values read from localStorage and values passed to the setter.
   * Invalid stored values fall back to the default value; invalid setter values
   * are ignored.
   */
  isValid?: (value: unknown) => value is T;
}

function readLocalStorageState<T>(
  key: string,
  defaultValue: T,
  { isValid }: UseLocalStorageStateOptions<T>,
): T {
  try {
    const storedValue = localStorage.getItem(key);
    if (storedValue === null) {
      return defaultValue;
    }

    const parsedValue = JSON.parse(storedValue) as unknown;
    if (isValid && !isValid(parsedValue)) {
      return defaultValue;
    }

    return parsedValue as T;
  } catch {
    return defaultValue;
  }
}

/**
 * Similar to {@link useState}, but persists the value to localStorage.
 *
 * Values are serialized with JSON.stringify and restored with JSON.parse. If
 * localStorage is unavailable, the stored value is missing, parsing fails, or
 * validation fails, the hook uses the provided default value.
 *
 * The returned setter supports the same direct and functional updates as
 * useState. Valid updates are saved to localStorage; if saving fails, the
 * in-memory state still updates so the UI remains responsive.
 *
 * @param key localStorage key used to store the value.
 * @param defaultValue value used when no valid stored value exists.
 * @param options optional validation for restored and updated values.
 * @returns A state tuple containing the current value and a setter.
 */
export function useLocalStorageState<T>(
  key: string,
  defaultValue: T,
  options: UseLocalStorageStateOptions<T> = {},
): [T, Dispatch<SetStateAction<T>>] {
  const { isValid } = options;
  const [value, setValue] = useState(() => readLocalStorageState(key, defaultValue, options));

  const setStoredValue = useCallback<Dispatch<SetStateAction<T>>>(
    (nextValue) => {
      setValue((currentValue) => {
        const valueToStore =
          typeof nextValue === "function"
            ? (nextValue as (currentValue: T) => T)(currentValue)
            : nextValue;

        if (isValid && !isValid(valueToStore)) {
          return currentValue;
        }

        try {
          localStorage.setItem(key, JSON.stringify(valueToStore));
        } catch {
          // Keep the in-memory state responsive when storage is unavailable.
        }

        return valueToStore;
      });
    },
    [isValid, key],
  );

  return [value, setStoredValue];
}
