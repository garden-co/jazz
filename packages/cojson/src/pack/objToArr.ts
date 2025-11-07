import { OpID } from "../exports.js";
import { JsonObject, JsonValue } from "../jsonValue.js";
import { packOpID, unpackOpID } from "./opID.js";

/**
 * Maps JavaScript primitive values to numeric codes for compact JSON representation.
 * These numbers are used in place of actual primitives to reduce payload size.
 * - null → 0
 * - false → 4
 * - true → 5
 *
 * @remarks
 * Using small integers (0, 4, 5) instead of actual primitives saves bytes in JSON:
 * - "null" (4 chars) vs 0 (1 char)
 * - "false" (5 chars) vs 4 (1 char)
 * - "true" (4 chars) vs 5 (1 char)
 */
export const ENCODING_MAP_PRIMITIVES_VALUES = {
  null: 0,
  false: 4,
  true: 5,
  undefined: 6,
} as const;

/**
 * Maps operation types to numeric indices for compact representation.
 * Numbers are smaller than strings in JSON, reducing payload size.
 * - 1 = "app" (append)
 * - 2 = "pre" (prepend)
 * - 3 = "del" (delete)
 */
export const ENCODING_MAP_OPERATION_TYPES = {
  app: 1,
  pre: 2,
  del: 3,
} as const;

/**
 * Reverse mapping from numeric indices to operation type strings.
 * Used to decode compressed operation arrays back to objects.
 */
export const ENCODING_MAP_OPERATION_TYPES_REVERSE = {
  [ENCODING_MAP_OPERATION_TYPES.app]: "app",
  [ENCODING_MAP_OPERATION_TYPES.pre]: "pre",
  [ENCODING_MAP_OPERATION_TYPES.del]: "del",
} as const;

/**
 * The index position where the operation type number is stored in compressed arrays.
 * All operation arrays have the operation type at index 2 (third element).
 *
 * @remarks
 * This constant ensures consistency across packing/unpacking operations.
 * The operation type is always the third element [value, ref, OPERATION_TYPE, ...]
 */
const OPERATION_INDEX = 2 as const;
/**
 * Ordered keys for append (app) operations in array format.
 * Defines the structure: [value, after, op, compacted]
 *
 * @remarks
 * This key order determines how append operations are serialized:
 * - Index 0: "value" - The value being inserted
 * - Index 1: "after" - The OpID this value comes after
 * - Index 2: "op" - The operation type (1 for "app")
 * - Index 3: "compacted" - Flag indicating if this is part of a compacted sequence
 */
export const LIST_KEYS_INSERTION_APPEND = ["value", "after", "op", "compacted"];

/**
 * Ordered keys for prepend (pre) operations in array format.
 * Defines the structure: [value, before, op, compacted]
 *
 * @remarks
 * This key order determines how prepend operations are serialized:
 * - Index 0: "value" - The value being inserted
 * - Index 1: "before" - The OpID this value comes before
 * - Index 2: "op" - The operation type (2 for "pre")
 * - Index 3: "compacted" - Flag indicating if this is part of a compacted sequence
 */
export const LIST_KEYS_INSERTION_PREPEND = [
  "value",
  "before",
  "op",
  "compacted",
];

/**
 * Ordered keys for deletion (del) operations in array format.
 * Defines the structure: [insertion, compacted, op]
 *
 * @remarks
 * This key order determines how deletion operations are serialized:
 * - Index 0: "insertion" - The OpID of the insertion being deleted
 * - Index 1: "compacted" - Flag indicating if this is part of a compacted sequence
 * - Index 2: "op" - The operation type (3 for "del")
 */
export const LIST_KEYS_DELETION = ["insertion", "compacted", "op"];

/**
 * Maps operation types to their corresponding key arrays.
 * Provides quick lookup for determining which key order to use when packing/unpacking.
 *
 * @remarks
 * This mapping is used throughout the packing process to select the correct
 * key array based on the operation type, ensuring consistent serialization.
 */
export const LIST_TO_KEYS_MAP = {
  app: LIST_KEYS_INSERTION_APPEND,
  pre: LIST_KEYS_INSERTION_PREPEND,
  del: LIST_KEYS_DELETION,
} as const;

/**
 * Extracts the operation type from the third element of an array (index 2).
 *
 * @param arr - Array of JSON values where the third element is a number (1/2/3) indicating the operation type
 * @returns The operation type: "app" (append), "del" (delete), or "pre" (prepend)
 *
 * @example
 * getOperationType(["value", "after", 1]) // returns "app" (1 = app)
 * getOperationType(["insertionId", 0, 3]) // returns "del" (3 = del)
 */
export function getOperationType<T extends string>(
  arr: JsonValue[],
  defaultValue: T,
): T {
  return (
    (ENCODING_MAP_OPERATION_TYPES_REVERSE[
      arr[OPERATION_INDEX] as keyof typeof ENCODING_MAP_OPERATION_TYPES_REVERSE
    ] as T) ?? defaultValue
  );
}

/**
 * Converts a JSON object into an array using an ordered set of keys.
 * Optimizes space by removing trailing nulls and encoding primitives as numbers.
 *
 * @param keys - Array of keys that defines the order and properties to extract
 * @param json - JSON object to convert into an array
 * @param skipEncodingIndexes - Optional indexes to skip primitive encoding (useful for preserving operation types)
 * @returns Array of JSON values in the order specified by the keys
 *
 * @remarks
 * Optimization strategy:
 * 1. Missing values in the object are represented as null initially
 * 2. Trailing nulls are removed to save space (shorter arrays = less JSON)
 * 3. Remaining primitives are encoded as numbers:
 *    - null → 0
 *    - false → 4
 *    - true → 5
 * 4. The skipEncodingIndexes parameter allows skipping encoding for specific positions
 *    (e.g., preserving the operation type number)
 *
 * @example
 * packObjectToArr(["name", "age"], { name: "Alice", age: 30 })
 * // returns ["Alice", 30]
 *
 * packObjectToArr(["name", "age", "city"], { name: "Bob" })
 * // returns ["Bob"]  // trailing "age" and "city" nulls removed
 *
 * packObjectToArr(["name", "age", "city"], { name: "Charlie", city: "NYC" })
 * // returns ["Charlie", 0, "NYC"]  // intermediate null converted to 0
 *
 * packObjectToArr(["value", "after", "op"], { value: "x", op: 1 }, 2)
 * // returns ["x", 0, 1]  // op at index 2 is preserved, not encoded
 */
export function packObjectToArr(
  keys: string[],
  json: JsonObject,
  skipEncodingIndexes?: number[],
): JsonValue[] {
  const arr = new Array<JsonValue | undefined>(keys.length);
  for (let i = 0; i < keys.length; i++) {
    arr[i] = json[keys[i]!];
  }

  // remove trailing nulls
  for (let i = arr.length - 1; i >= 0; i--) {
    if (arr[i] === undefined) {
      arr.pop();
    } else {
      break;
    }
  }

  // convert remaining nulls (intermediate) to ENCODING_MAP_PRIMITIVES_VALUES.null for better compression
  // convert remaining false to ENCODING_MAP_PRIMITIVES_VALUES.false for better compression
  // convert remaining true to ENCODING_MAP_PRIMITIVES_VALUES.true for better compression
  for (let i = 0; i < arr.length; i++) {
    if (skipEncodingIndexes?.includes(i)) {
      continue;
    }
    if (arr[i] === null) {
      arr[i] = ENCODING_MAP_PRIMITIVES_VALUES.null;
    } else if (arr[i] === false) {
      arr[i] = ENCODING_MAP_PRIMITIVES_VALUES.false;
    } else if (arr[i] === true) {
      arr[i] = ENCODING_MAP_PRIMITIVES_VALUES.true;
    } else if (arr[i] === undefined) {
      arr[i] = ENCODING_MAP_PRIMITIVES_VALUES.undefined;
    }
  }

  return arr as JsonValue[];
}

/**
 * Converts an array of values into a JSON object using an ordered set of keys.
 * This is the inverse operation of packObjectToArr.
 *
 * @param keys - Array of keys that defines how to map the array values
 * @param arr - Array of JSON values to convert into an object
 * @param skipIndex - Optional index to skip primitive decoding (useful for operation types)
 * @returns JSON object with keys mapped to their corresponding values
 *
 * @remarks
 * Decoding strategy:
 * 1. Values at skipIndex are preserved as-is without decoding
 * 2. Null, undefined, or 0 are treated as missing values (not included)
 * 3. Encoded primitives are decoded:
 *    - 4 → false
 *    - 5 → true
 * 4. Other values are preserved as-is
 * 5. If array is shorter than keys, remaining keys are not included
 *
 * @example
 * unpackArrToObject(["name", "age"], ["Alice", 30])
 * // returns { name: "Alice", age: 30 }
 *
 * unpackArrToObject(["name", "age", "city"], ["Bob", 0, "LA"])
 * // returns { name: "Bob", city: "LA" }  // 0 treated as missing value
 *
 * unpackArrToObject(["active", "status"], [4, "pending"])
 * // returns { active: false, status: "pending" }  // 4 decoded to false
 *
 * unpackArrToObject(["value", "after", "op"], ["x", 0, 1], 2)
 * // returns { value: "x", op: 1 }  // op at index 2 preserved without decoding
 */
export function unpackArrToObject(
  keys: string[],
  arr: JsonValue[],
  skipDecodingIndexes?: number[],
) {
  const obj: JsonObject = {};
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i]!;
    const data = arr[i];

    if (skipDecodingIndexes?.includes(i)) {
      obj[key] = data;
      continue;
    }
    // Skip undefined, it's used as placeholder for missing values
    if (data === ENCODING_MAP_PRIMITIVES_VALUES.undefined) {
      continue;
    }
    if (data === ENCODING_MAP_PRIMITIVES_VALUES.false) {
      obj[key] = false;
    } else if (data === ENCODING_MAP_PRIMITIVES_VALUES.true) {
      obj[key] = true;
    } else if (data === ENCODING_MAP_PRIMITIVES_VALUES.null) {
      obj[key] = null;
    } else {
      obj[key] = data;
    }
  }
  return obj;
}

/**
 * Unpacks a single CoList operation from array format to object format.
 * Extracts the operation type and uses the appropriate key mapping.
 *
 * @param item - Array representing a single CoList operation
 * @returns Object representing the CoList operation with op property
 *
 * @remarks
 * Process:
 * 1. Reads operation type from index 2 (third element)
 * 2. Maps number to operation type: 1="app", 2="pre", 3="del"
 * 3. Selects appropriate keys from LIST_TO_KEYS_MAP
 * 4. Unpacks array values to object properties
 * 5. Adds the "op" property to identify the operation type
 *
 * The skipIndex=0 parameter preserves the value field without decoding.
 *
 * @example
 * unpackArrToObjectCoList(["value1", "after1", 1])
 * // returns { op: "app", value: "value1", after: "after1" }
 *
 * unpackArrToObjectCoList(["insertionId1", 0, 3])
 * // returns { op: "del", insertion: "insertionId1" }
 *
 * unpackArrToObjectCoList(["value2", 0, 2, 5])
 * // returns { op: "pre", value: "value2", compacted: true }
 */
export function unpackArrToObjectCoList<T extends JsonValue>(item: T[]) {
  const operationType = getOperationType<"app" | "pre" | "del">(
    item,
    "app" as const,
  );
  const data = unpackArrToObject(LIST_TO_KEYS_MAP[operationType], item, [0]);
  data.op = operationType;

  if (operationType !== "del" && item[1] !== "start" && item[1] !== "end") {
    if (typeof item[1] === "string") {
      if (data.after) {
        data.after = unpackOpID(item[1]);
      }
      if (data.before) {
        data.before = unpackOpID(item[1]);
      }
    }
  }
  return data;
}

/**
 * Packs a single CoList operation from object format to array format.
 * Converts the operation object to a compact array using numeric operation types.
 *
 * @param item - Operation object with an "op" property indicating the type
 * @returns Compact array representing the operation
 *
 * @remarks
 * Packing strategy:
 * 1. Looks up the operation type number (1/2/3)
 * 2. Selects appropriate keys from LIST_TO_KEYS_MAP
 * 3. For "app" operations: removes the "op" field to save space (it's the default)
 * 4. Packs object to array using packObjectToArr
 * 5. For non-"app" operations: sets the operation type at index 2
 *
 * The skipIndex=0 parameter preserves the value field without encoding.
 *
 * Space optimization: "app" operations omit the op field since 1 is default,
 * saving bytes in the most common case.
 *
 * @example
 * packArrToObjectCoList({ op: "app", value: "value1", after: "after1" })
 * // returns ["value1", "after1"]  // op omitted for "app"
 *
 * packArrToObjectCoList({ op: "del", insertion: "insertionId1" })
 * // returns ["insertionId1", 0, 3]
 *
 * packArrToObjectCoList({ op: "pre", value: "value2", compacted: true })
 * // returns ["value2", 0, 2, 5]
 */
export function packArrToObjectCoList<T extends JsonObject>(
  item: T & { op: "app" | "pre" | "del" },
) {
  const operationType =
    ENCODING_MAP_OPERATION_TYPES[
      item.op as keyof typeof ENCODING_MAP_OPERATION_TYPES
    ];

  const arrData = packObjectToArr(
    LIST_TO_KEYS_MAP[item.op],
    // To save less data, we remove the op key for app operations. It's default value.
    item.op === "app" ? { ...item, op: undefined } : item,
    [0],
  );
  if (item.op !== "app") {
    arrData[OPERATION_INDEX] = operationType;
  }

  if (item.op === "app" || item.op === "pre") {
    if (typeof arrData[1] === "object") {
      arrData[1] = packOpID(arrData[1] as OpID);
    }
  }
  return arrData;
}

/**
 * Unpacks an array of CoList operation arrays into operation objects.
 * Each sub-array is individually unpacked using unpackArrToObjectCoList.
 *
 * @param arr - Array of arrays where each sub-array represents an operation
 * @returns Array of objects representing CoList operations
 *
 * @remarks
 * This is a batch version of unpackArrToObjectCoList that processes multiple
 * operations at once. Each operation is unpacked independently:
 * - Operation type is determined from index 2 of each sub-array
 * - Appropriate key mapping is selected based on operation type
 * - Array values are converted to object properties
 *
 * @example
 * unpackArrOfObjectsCoList([
 *   ["value1", "after1", 1],
 *   ["insertionId1", 0, 3],
 *   ["value2", 0, 2]
 * ])
 * // returns [
 * //   { op: "app", value: "value1", after: "after1" },
 * //   { op: "del", insertion: "insertionId1" },
 * //   { op: "pre", value: "value2" }
 * // ]
 */
export function unpackArrOfObjectsCoList<T extends JsonValue>(arr: T[][]) {
  return arr.map((item) => {
    return unpackArrToObjectCoList(item);
  });
}

/**
 * Packs an array of CoList operation objects into arrays.
 * Each object is individually packed using packArrToObjectCoList.
 *
 * @param arr - Array of operation objects with an "op" property indicating the type
 * @returns Array of compact arrays representing the operations
 *
 * @remarks
 * This is a batch version of packArrToObjectCoList that processes multiple
 * operations at once. Benefits:
 * - Eliminates repeated property names (huge space savings)
 * - Uses numeric operation types instead of strings
 * - Applies trailing null removal to each operation
 * - Encodes primitives as numbers
 *
 * This is the first level of compression; further compression via compaction
 * is handled by CoListPack and CoPlainTextPack implementations.
 *
 * @example
 * packArrOfObjectsCoList([
 *   { op: "app", value: "value1", after: "after1" },
 *   { op: "del", insertion: "insertionId1" },
 *   { op: "pre", value: "value2", before: "id1" }
 * ])
 * // returns [
 * //   ["value1", "after1"],      // "app" omits op
 * //   ["insertionId1", 0, 3],    // "del"
 * //   ["value2", "id1", 2]       // "pre"
 * // ]
 */
export function packArrOfObjectsCoList<T extends JsonObject>(
  arr: (T & { op: "app" | "del" | "pre" })[],
) {
  return arr.map((item) => {
    return packArrToObjectCoList(item);
  });
}
