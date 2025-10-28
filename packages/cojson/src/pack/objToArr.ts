import { JsonObject, JsonValue } from "../jsonValue.js";

/**
 * Ordered keys to represent insertion operations in array format.
 * Defines the structure: ["op", "value", "after", "compacted"]
 * Used to convert append/prepend operation objects into compact arrays.
 */
export const LIST_KEYS_INSERTION_APPEND = ["op", "value", "after", "compacted"];

/**
 * Ordered keys to represent prepend operations in array format.
 * Defines the structure: ["op", "value", "before", "compacted"]
 * Used to convert prepend operation objects into compact arrays.
 */
export const LIST_KEYS_INSERTION_PREPEND = [
  "op",
  "value",
  "before",
  "compacted",
];

/**
 * Ordered keys to represent deletion operations in array format.
 * Defines the structure: ["op", "insertion", "compacted"]
 * Used to convert delete operation objects into compact arrays.
 */
export const LIST_KEYS_DELETION = ["op", "insertion", "compacted"];

export const LIST_TO_KEYS_MAP = {
  app: LIST_KEYS_INSERTION_APPEND,
  pre: LIST_KEYS_INSERTION_PREPEND,
  del: LIST_KEYS_DELETION,
} as const;

/**
 * Extracts the operation type from the first element of an array.
 *
 * @param arr - Array of JSON values where the first element indicates the operation type
 * @returns The operation type: "app" (append), "del" (delete), or "pre" (prepend)
 *
 * @example
 * getOperationType(["app", "value", "after"]) // returns "app"
 * getOperationType(["del", "insertionId"]) // returns "del"
 */
export function getOperationType<T extends string>(arr: JsonValue[]): T {
  return arr[0] as T;
}

/**
 * Converts a JSON object into an array using an ordered set of keys.
 * Optimizes space by removing trailing nulls from the resulting array.
 *
 * @param keys - Array of keys that defines the order and properties to extract
 * @param json - JSON object to convert into an array
 * @returns Array of JSON values in the order specified by the keys
 *
 * @remarks
 * - Missing values in the object are represented as null
 * - Trailing nulls are removed one at a time to save space
 * - Nulls in intermediate positions are preserved to maintain order
 *
 * @example
 * packObjectToArr(["name", "age"], { name: "Alice", age: 30 })
 * // returns ["Alice", 30]
 *
 * packObjectToArr(["name", "age", "city"], { name: "Bob" })
 * // returns ["Bob", null]  // trailing "city" null removed
 *
 * packObjectToArr(["name", "age", "city"], { name: "Charlie", city: "NYC" })
 * // returns ["Charlie", null, "NYC"]  // intermediate null preserved
 */
export function packObjectToArr(keys: string[], json: JsonObject) {
  const arr = new Array<JsonValue>(keys.length);
  for (let i = 0; i < keys.length; i++) {
    arr[i] = json[keys[i]!] ?? null;
  }

  // remove trailing nulls
  for (let i = arr.length - 1; i >= 0; i--) {
    if (arr[i] === null) {
      arr.pop();
    } else {
      break;
    }
  }
  return arr;
}

/**
 * Converts an array of values into a JSON object using an ordered set of keys.
 * This is the inverse operation of packObjectToArr.
 *
 * @param keys - Array of keys that defines how to map the array values
 * @param arr - Array of JSON values to convert into an object
 * @returns JSON object with keys mapped to their corresponding values
 *
 * @remarks
 * - Null or undefined values in the array are skipped (not included in the object)
 * - If the array is shorter than the keys, missing keys are not included
 * - All falsy values except null/undefined are preserved (0, false, "")
 *
 * @example
 * unpackArrToObject(["name", "age"], ["Alice", 30])
 * // returns { name: "Alice", age: 30 }
 *
 * unpackArrToObject(["name", "age", "city"], ["Bob", null, "LA"])
 * // returns { name: "Bob", city: "LA" }  // null skipped
 *
 * unpackArrToObject(["count", "active"], [0, false])
 * // returns { count: 0, active: false }  // falsy values preserved
 */
export function unpackArrToObject(keys: string[], arr: JsonValue[]) {
  const obj: JsonObject = {};
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i]!;
    const data = arr[i];

    // Skip null or undefined values to keep the object compact
    if (data === null || data === undefined) {
      continue;
    }
    obj[key] = data;
  }
  return obj;
}

/**
 * Decompresses an array of arrays into an array of objects for CoList operations.
 * Each sub-array is converted into an object using the appropriate keys
 * based on the operation type (insertion or deletion).
 *
 * @param arr - Array of arrays where each sub-array represents an operation
 * @returns Array of objects representing CoList operations
 *
 * @remarks
 * - Automatically determines the operation type from the first element
 * - Uses LIST_KEYS_INSERTION for "app" and "pre" operations
 * - Uses LIST_KEYS_DELETION for "del" operations
 *
 * @example
 * unpackArrOfObjectsCoList([
 *   ["app", "value1", "after1"],
 *   ["del", "insertionId1"]
 * ])
 * // returns [
 * //   { op: "app", value: "value1", after: "after1" },
 * //   { op: "del", insertion: "insertionId1" }
 * // ]
 */
export function unpackArrOfObjectsCoList<T extends JsonValue>(arr: T[][]) {
  return arr.map((item) => {
    const operationType = getOperationType<"app" | "pre" | "del">(item);
    return unpackArrToObject(LIST_TO_KEYS_MAP[operationType], item);
  });
}

/**
 * Compresses an array of CoList operation objects into an array of arrays.
 * Each object is converted into an array using the appropriate keys
 * based on the operation type.
 *
 * @param arr - Array of operation objects with an "op" property indicating the type
 * @returns Array of compact arrays representing the operations
 *
 * @remarks
 * - Uses LIST_KEYS_DELETION for operations with op: "del"
 * - Uses LIST_KEYS_INSERTION for operations with op: "app" or "pre"
 * - Significantly reduces JSON size by eliminating repeated keys
 *
 * @example
 * packArrOfObjectsCoList([
 *   { op: "app", value: "value1", after: "after1" },
 *   { op: "del", insertion: "insertionId1" }
 * ])
 * // returns [
 * //   ["app", "value1", "after1"],
 * //   ["del", "insertionId1"]
 * // ]
 */
export function packArrOfObjectsCoList<T extends JsonObject>(
  arr: (T & { op: "app" | "del" | "pre" })[],
) {
  return arr.map((item) => {
    return packObjectToArr(LIST_TO_KEYS_MAP[item.op], item);
  });
}
