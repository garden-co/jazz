import { JsonValue } from "../jsonValue.js";
import {
  MapOpPayloadDel,
  MapOpPayloadSet,
  MapOpPayload,
} from "../coValues/coMap.js";
import { packObjectToArr, unpackArrToObject } from "./objToArr.js";

/**
 * Maps CoMap operation types to numeric indices for compact representation.
 * Numbers are smaller than strings in JSON, reducing payload size.
 * - 1 = "set" (set key-value pair)
 * - 2 = "del" (delete key)
 *
 * @remarks
 * Using single-digit numbers instead of strings saves bytes:
 * - "set" (5 chars including quotes) vs 1 (1 char)
 * - "del" (5 chars including quotes) vs 2 (1 char)
 */
export const ENCODING_MAP_COMAP_OPERATION_TYPES = {
  set: 1,
  del: 2,
} as const;

/**
 * Reverse mapping from numeric indices to CoMap operation type strings.
 * Used to decode compressed operation arrays back to objects.
 */
export const ENCODING_MAP_COMAP_OPERATION_TYPES_REVERSE = {
  [ENCODING_MAP_COMAP_OPERATION_TYPES.set]: "set",
  [ENCODING_MAP_COMAP_OPERATION_TYPES.del]: "del",
} as const;

/**
 * The index position where the operation type number is stored in compressed arrays.
 * All CoMap operation arrays have the operation type at index 0 (first element).
 *
 * @remarks
 * This differs from CoList where the operation type is at index 2.
 * For CoMap, the structure is simpler: [OPERATION_TYPE, key, value?]
 */
const OPERATION_INDEX = 0 as const;

/**
 * Ordered keys for set operations in array format.
 * Defines the structure: [op, key, value]
 *
 * @remarks
 * This key order determines how set operations are serialized:
 * - Index 0: "op" - The operation type (1 for "set")
 * - Index 1: "key" - The key being set
 * - Index 2: "value" - The value being assigned to the key
 */
export const MAP_KEYS_SET = ["op", "key", "value"];

/**
 * Ordered keys for deletion operations in array format.
 * Defines the structure: [op, key]
 *
 * @remarks
 * This key order determines how deletion operations are serialized:
 * - Index 0: "op" - The operation type (2 for "del")
 * - Index 1: "key" - The key being deleted
 */
export const MAP_KEYS_DELETION = ["op", "key"];

/**
 * Maps operation types to their corresponding key arrays.
 * Provides quick lookup for determining which key order to use when packing/unpacking.
 *
 * @remarks
 * This mapping is used throughout the packing process to select the correct
 * key array based on the operation type, ensuring consistent serialization.
 */
export const MAP_TO_KEYS_MAP = {
  set: MAP_KEYS_SET,
  del: MAP_KEYS_DELETION,
} as const;

/**
 * Extracts the operation type from the first element of an array (index 0).
 *
 * @param arr - Array of JSON values where the first element is a number (1/2) indicating the operation type
 * @returns The operation type: "set" or "del"
 *
 * @example
 * getCoMapOperationType([1, "name", "Alice"]) // returns "set" (1 = set)
 * getCoMapOperationType([2, "age"]) // returns "del" (2 = del)
 */
export function getCoMapOperationType<T extends string>(
  arr: JsonValue[],
  defaultValue: T,
): T {
  return (
    (ENCODING_MAP_COMAP_OPERATION_TYPES_REVERSE[
      arr[
        OPERATION_INDEX
      ] as keyof typeof ENCODING_MAP_COMAP_OPERATION_TYPES_REVERSE
    ] as T) ?? defaultValue
  );
}

/**
 * Unpacks a single CoMap operation from array format to object format.
 * Extracts the operation type and uses the appropriate key mapping.
 *
 * @param item - Array representing a single CoMap operation
 * @returns Object representing the CoMap operation with op property
 *
 * @remarks
 * Process:
 * 1. Reads operation type from index 0 (first element)
 * 2. Maps number to operation type: 1="set", 2="del"
 * 3. Selects appropriate keys from MAP_TO_KEYS_MAP
 * 4. Unpacks array values to object properties
 * 5. Preserves the operation type in the result
 *
 * The skipDecodingIndexes=[1] parameter preserves the key field without decoding,
 * and for set operations also preserves the value field (index 2).
 *
 * @example
 * unpackArrToObjectCoMap([1, "name", "Alice"])
 * // returns { op: "set", key: "name", value: "Alice" }
 *
 * unpackArrToObjectCoMap([2, "age"])
 * // returns { op: "del", key: "age" }
 */
export function unpackArrToObjectCoMap<
  K extends string,
  V extends JsonValue | undefined,
>(item: JsonValue[]) {
  const operationType = getCoMapOperationType<"set" | "del">(item, "set");
  // Skip decoding for key (index 1) and value (index 2 for set operations)
  const skipIndexes = operationType === "set" ? [1, 2] : [1];
  const data = unpackArrToObject(
    MAP_TO_KEYS_MAP[operationType],
    item,
    skipIndexes,
  );
  data.op = operationType;
  return data as MapOpPayload<K, V>;
}

/**
 * Packs a single CoMap operation from object format to array format.
 * Converts the operation object to a compact array using numeric operation types.
 *
 * @param item - Operation object with an "op" property indicating the type
 * @returns Compact array representing the operation
 *
 * @remarks
 * Packing strategy:
 * 1. Looks up the operation type number (1 for "set", 2 for "del")
 * 2. Selects appropriate keys from MAP_TO_KEYS_MAP
 * 3. Packs object to array using packObjectToArr
 * 4. Sets the operation type at index 0
 *
 * The skipEncodingIndexes parameter preserves:
 * - Index 1: key (always preserved)
 * - Index 2: value (preserved for set operations)
 *
 * Space optimization: Arrays are significantly smaller than objects in JSON.
 *
 * @example
 * packArrToObjectCoMap({ op: "set", key: "name", value: "Alice" })
 * // returns [1, "name", "Alice"]
 *
 * packArrToObjectCoMap({ op: "del", key: "age" })
 * // returns [2, "age"]
 */
export function packArrToObjectCoMap<
  K extends string,
  V extends JsonValue | undefined,
>(item: MapOpPayload<K, V>) {
  const operationType =
    ENCODING_MAP_COMAP_OPERATION_TYPES[
      item.op as keyof typeof ENCODING_MAP_COMAP_OPERATION_TYPES
    ];

  // Skip encoding for key (index 1) and value (index 2 for set operations)
  const skipIndexes = item.op === "set" ? [1, 2] : [1];

  const arrData = packObjectToArr(
    MAP_TO_KEYS_MAP[item.op],
    item as Record<string, JsonValue>,
    skipIndexes,
  );

  arrData[OPERATION_INDEX] = operationType;
  return arrData;
}

/**
 * Unpacks an array of CoMap operation arrays into operation objects.
 * Each sub-array is individually unpacked using unpackArrToObjectCoMap.
 *
 * @param arr - Array of arrays where each sub-array represents an operation
 * @returns Array of objects representing CoMap operations
 *
 * @remarks
 * This is a batch version of unpackArrToObjectCoMap that processes multiple
 * operations at once. Each operation is unpacked independently:
 * - Operation type is determined from index 0 of each sub-array
 * - Appropriate key mapping is selected based on operation type
 * - Array values are converted to object properties
 *
 * @example
 * unpackArrOfObjectsCoMap([
 *   [1, "name", "Alice"],
 *   [2, "age"],
 *   [1, "city", "NYC"]
 * ])
 * // returns [
 * //   { op: "set", key: "name", value: "Alice" },
 * //   { op: "del", key: "age" },
 * //   { op: "set", key: "city", value: "NYC" }
 * // ]
 */
export function unpackArrOfObjectsCoMap<
  K extends string,
  V extends JsonValue | undefined,
>(arr: JsonValue[][]) {
  return arr.map((item) => {
    return unpackArrToObjectCoMap<K, V>(item);
  });
}

/**
 * Packs an array of CoMap operation objects into arrays.
 * Each object is individually packed using packArrToObjectCoMap.
 *
 * @param arr - Array of operation objects with an "op" property indicating the type
 * @returns Array of compact arrays representing the operations
 *
 * @remarks
 * This is a batch version of packArrToObjectCoMap that processes multiple
 * operations at once. Benefits:
 * - Eliminates repeated property names (huge space savings)
 * - Uses numeric operation types instead of strings
 * - Applies trailing null removal to each operation
 * - Encodes primitives as numbers (where applicable)
 *
 * This is the primary compression mechanism for CoMap operations.
 *
 * @example
 * packArrOfObjectsCoMap([
 *   { op: "set", key: "name", value: "Alice" },
 *   { op: "del", key: "age" },
 *   { op: "set", key: "city", value: "NYC" }
 * ])
 * // returns [
 * //   [1, "name", "Alice"],
 * //   [2, "age"],
 * //   [1, "city", "NYC"]
 * // ]
 */
export function packArrOfObjectsCoMap<
  K extends string,
  V extends JsonValue | undefined,
>(arr: MapOpPayload<K, V>[]) {
  return arr.map((item) => {
    return packArrToObjectCoMap(item);
  });
}

/**
 * Interface for packing and unpacking CoMap operations.
 * Defines the contract for compressing map operations to reduce storage and network overhead.
 *
 * @template K - The type of keys in the map (extends string)
 * @template V - The type of values in the map (extends JsonValue | undefined)
 *
 * @remarks
 * Implementations of this interface apply compression through:
 * 1. Object-to-array conversion (removes property names)
 * 2. Numeric operation types (1="set", 2="del")
 * 3. Primitive encoding (null/false/true → 0/4/5 where applicable)
 *
 * The result is significantly smaller JSON payloads for map operations.
 */
export interface CoMapPack<K extends string, V extends JsonValue | undefined> {
  /**
   * Compresses an array of map operations into a more compact format.
   *
   * @param changes - Array of map operations to pack
   * @returns Packed representation as a single array containing operation arrays
   *
   * @remarks
   * The implementation should:
   * - Convert each operation object to an array
   * - Use numeric operation types
   * - Preserve keys and values without encoding
   */
  packChanges(changes: MapOpPayload<K, V>[]): JsonValue[];

  /**
   * Decompresses packed operations back into full operation objects.
   *
   * @param changes - Packed operations (JsonValue[]) or already-unpacked operations
   * @returns Array of full operation objects ready to be applied
   *
   * @remarks
   * The implementation should:
   * - Detect the format of incoming data (packed vs unpacked)
   * - Handle already-unpacked operations gracefully (pass through)
   * - Reconstruct full operations from arrays
   */
  unpackChanges(
    changes: JsonValue[] | MapOpPayload<K, V>[],
  ): MapOpPayload<K, V>[];
}

/**
 * Standard implementation of CoMap packing/unpacking for map operations.
 *
 * @template K - The type of keys in the map
 * @template V - The type of values in the map
 *
 * @remarks
 * **Compression Strategy:**
 *
 * This implementation focuses on basic but effective compression:
 * - Converts operation objects to arrays (removes property names)
 * - Uses numeric operation types (1 for "set", 2 for "del")
 * - Applies primitive encoding where safe
 *
 * Unlike CoList, CoMap operations don't typically benefit from compaction
 * since operations are usually on different keys. Each operation is
 * independently packed.
 *
 * **Space Savings Example:**
 * ```
 * Before packing: ~80 bytes
 * [
 *   { op: "set", key: "name", value: "Alice" },
 *   { op: "set", key: "age", value: 30 },
 *   { op: "del", key: "city" }
 * ]
 *
 * After packing: ~40 bytes
 * [
 *   [1, "name", "Alice"],
 *   [1, "age", 30],
 *   [2, "city"]
 * ]
 * ```
 * Savings: ~50% reduction in size
 *
 * **Performance Characteristics:**
 * - Packing: O(n) linear pass through operations
 * - Unpacking: O(n) linear pass through operations
 * - Memory: Minimal overhead, in-place transformations
 *
 * **Usage Patterns:**
 * - Key-value state synchronization
 * - Object property updates in collaborative editing
 * - Configuration management
 * - Any scenario with map-like data structures
 *
 * @example
 * const packer = new CoMapPackImplementation<string, JsonValue>();
 *
 * // Packing operations
 * const operations = [
 *   { op: "set", key: "name", value: "Alice" },
 *   { op: "set", key: "age", value: 30 },
 *   { op: "del", key: "city" }
 * ];
 *
 * const packed = packer.packChanges(operations);
 * // Result: [[1, "name", "Alice"], [1, "age", 30], [2, "city"]]
 *
 * const unpacked = packer.unpackChanges(packed);
 * // Result: Original operations array
 */
export class CoMapPackImplementation<
  K extends string,
  V extends JsonValue | undefined,
> implements CoMapPack<K, V>
{
  /**
   * Packs an array of CoMap operations into a compact format.
   *
   * @param changes - Array of map operations to compress
   * @returns Single array containing compact arrays representing the operations
   *
   * @remarks
   * **Packing Process:**
   *
   * For each operation:
   * 1. Determine operation type ("set" or "del")
   * 2. Convert to numeric operation type (1 or 2)
   * 3. Select appropriate key order (MAP_KEYS_SET or MAP_KEYS_DELETION)
   * 4. Convert object to array using packObjectToArr
   * 5. Preserve keys and values without primitive encoding
   *
   * **No Compaction:**
   * Unlike CoList, CoMap operations are not compacted together because:
   * - Operations typically affect different keys
   * - No shared metadata to deduplicate
   * - Each operation is independent
   *
   * **Size Optimization:**
   * Even without compaction, packing provides significant benefits:
   * - Property names removed (9+ bytes per operation)
   * - Operation type as number instead of string (3-4 bytes saved)
   * - Trailing nulls removed
   * - Shorter JSON representation overall
   *
   * @example
   * packChanges([
   *   { op: "set", key: "name", value: "Alice" },
   *   { op: "del", key: "age" }
   * ])
   * // returns [[1, "name", "Alice"], [2, "age"]]
   */
  packChanges(changes: MapOpPayload<K, V>[]): JsonValue[] {
    return packArrOfObjectsCoMap(changes) as JsonValue[];
  }

  /**
   * Unpacks compressed CoMap operations back into full operation objects.
   *
   * @param changes - Packed operations (JsonValue[]) or already-unpacked operations
   * @returns Array of full operation objects ready to be applied to the map
   *
   * @remarks
   * **Input Format Detection:**
   *
   * This method intelligently handles multiple input formats:
   *
   * 1. **Already unpacked** (changes[0] is an object with "op" property)
   *    - Pass through as-is, no processing
   *    - Example: [{ op: "set", key: "name", value: "Alice" }, ...]
   *
   * 2. **Empty array**
   *    - Return [] immediately
   *
   * 3. **Packed format** (array of arrays)
   *    - Unpack each sub-array individually
   *    - Reconstruct full operation objects
   *    - Example: [[1, "name", "Alice"]] → [{ op: "set", key: "name", value: "Alice" }]
   *
   * **Reconstruction Process:**
   * For each packed operation:
   * 1. Read operation type from index 0
   * 2. Map number to operation type: 1="set", 2="del"
   * 3. Select appropriate keys based on operation type
   * 4. Create operation object with all properties
   * 5. Decode any encoded primitives (except keys and values)
   *
   * @example
   * Packed input:
   * [[1, "name", "Alice"], [2, "age"], [1, "city", "NYC"]]
   *
   * Unpacked output:
   * [
   *   { op: "set", key: "name", value: "Alice" },
   *   { op: "del", key: "age" },
   *   { op: "set", key: "city", value: "NYC" }
   * ]
   */
  unpackChanges(
    changes: JsonValue[] | MapOpPayload<K, V>[],
  ): MapOpPayload<K, V>[] {
    // Empty array - return as-is
    if (changes.length === 0) {
      return [];
    }

    // Already unpacked - check if first element has "op" property (is an object with op field)
    const firstElement = changes[0];
    if (
      firstElement &&
      typeof firstElement === "object" &&
      !Array.isArray(firstElement) &&
      "op" in firstElement
    ) {
      return changes as MapOpPayload<K, V>[];
    }

    // Packed format - unpack each operation
    return unpackArrOfObjectsCoMap<K, V>(changes as JsonValue[][]);
  }
}
