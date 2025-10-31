import { JsonValue } from "../jsonValue";
import {
  AppOpPayload,
  DeletionOpPayload,
  ListOpPayload,
} from "../coValues/coList.js";
import {
  packArrOfObjectsCoList,
  unpackArrOfObjectsCoList,
  unpackArrToObjectCoList,
  packArrToObjectCoList,
} from "./objToArr.js";

/**
 * Type representing the compacted format for CoList operations.
 * Uses a highly optimized structure: first operation as array + subsequent values.
 *
 * @template T - The type of values stored in the list
 *
 * @remarks
 * Structure breakdown:
 * - First element: Packed array containing the first operation with compacted=true
 * - Remaining elements: Just the raw values (not full operations)
 *
 * This format dramatically reduces JSON size when multiple operations share the same "after" reference.
 * Instead of repeating "op", "after" for each item, we store them once and include just the values.
 *
 * @example
 * Compacted format:
 * [
 *   ["value1", "start", 1, 5],  // First op: [value, after, op=1, compacted=5(true)]
 *   "value2",                     // Just the value (inherits after="start")
 *   "value3"                      // Just the value (inherits after="start")
 * ]
 *
 * Represents these full operations:
 * [
 *   { op: "app", value: "value1", after: "start", compacted: true },
 *   { op: "app", value: "value2", after: "start" },
 *   { op: "app", value: "value3", after: "start" }
 * ]
 */
export type PackedChanges<T extends JsonValue> = [
  JsonValue[], // Packed AppOpPayload<T> & { compacted: true }
  ...T[],
];

/**
 * Interface for packing and unpacking CoList operations.
 * Defines the contract for compressing list operations to reduce storage and network overhead.
 *
 * @template Item - The type of items stored in the CoList
 * @template PackedItems - The type of the packed representation (default: PackedChanges<Item>)
 *
 * @remarks
 * Implementations of this interface apply multi-level compression:
 * 1. Object-to-array conversion (removes property names)
 * 2. Primitive encoding (null/false/true → 0/4/5)
 * 3. Compaction (shared metadata for sequential operations)
 *
 * The result is significantly smaller JSON payloads, especially for operations
 * involving many sequential insertions or deletions.
 */
export interface CoListPack<
  Item extends JsonValue,
  PackedItems extends JsonValue[] = PackedChanges<Item>,
> {
  /**
   * Compresses an array of operations into a more compact format.
   *
   * @param changes - Array of list operations to pack
   * @returns Packed representation (if compaction possible) or array of arrays (fallback)
   *
   * @remarks
   * The implementation should:
   * - Detect sequences of similar operations (same "after" reference)
   * - Apply compaction when beneficial
   * - Fall back to simple array packing when compaction isn't applicable
   */
  packChanges(changes: ListOpPayload<Item>[]): PackedItems | JsonValue[][];

  /**
   * Decompresses packed operations back into full operation objects.
   *
   * @param changes - Packed operations, array of arrays, or already-unpacked operations
   * @returns Array of full operation objects ready to be applied
   *
   * @remarks
   * The implementation should:
   * - Detect the format of incoming data (compacted vs non-compacted)
   * - Handle already-unpacked operations gracefully (pass through)
   * - Reconstruct full operations from compacted format
   */
  unpackChanges(
    changes: PackedItems | JsonValue[][] | ListOpPayload<Item>[],
  ): ListOpPayload<Item>[];
}

/**
 * Standard implementation of CoList packing/unpacking with sequential operation optimization.
 *
 * @template Item - The type of items stored in the list
 *
 * @remarks
 * Compression Strategy:
 *
 * This implementation applies compaction when it detects multiple append operations
 * that share the same "after" reference. This is a common pattern when inserting
 * multiple items in sequence (e.g., typing text, adding list items).
 *
 * **Compaction conditions:**
 * - 2+ operations
 * - All operations are "app" (append) type
 * - All operations share the same "after" reference
 *
 * **Compaction result:**
 * - First operation stored with compacted=true flag
 * - Subsequent operations stored as just their values
 * - The shared "after" reference is stored only once
 *
 * **Space savings example:**
 * ```
 * Before compaction: ~200 bytes
 * [
 *   { op: "app", value: "a", after: "xyz123" },
 *   { op: "app", value: "b", after: "xyz123" },
 *   { op: "app", value: "c", after: "xyz123" }
 * ]
 *
 * After compaction: ~50 bytes
 * [
 *   ["a", "xyz123", 1, 5],  // Full first operation
 *   "b",                      // Just the value
 *   "c"                       // Just the value
 * ]
 * ```
 *
 * @example
 * const packer = new CoListPackImplementation<string>();
 *
 * // Packing operations
 * const packed = packer.packChanges([
 *   { op: "app", value: "a", after: "start" },
 *   { op: "app", value: "b", after: "start" },
 *   { op: "app", value: "c", after: "start" }
 * ]);
 * // Result: [["a", "start", 1, 5], "b", "c"]
 *
 * // Unpacking back to full operations
 * const unpacked = packer.unpackChanges(packed);
 * // Result: Original array of operations
 */
export class CoListPackImplementation<Item extends JsonValue>
  implements CoListPack<Item, PackedChanges<Item>>
{
  /**
   * Packs an array of CoList operations into a compact format.
   *
   * @param changes - Array of list operations to compress
   * @returns Compacted representation or array of arrays if compaction isn't applicable
   *
   * @remarks
   * **Compaction Decision Tree:**
   *
   * 1. **Empty array** → Return [] (no processing needed)
   *
   * 2. **Single operation** → Pack normally without compaction
   *    - Uses packArrOfObjectsCoList to convert object to array
   *    - No benefit from compaction with only one operation
   *
   * 3. **Multiple operations** → Check compaction eligibility:
   *    - ✅ All operations must be "app" (append) type
   *    - ✅ All operations must share the same "after" reference
   *    - ❌ If any operation is different type → Pack without compaction
   *    - ❌ If any operation has different "after" → Pack without compaction
   *
   * 4. **Eligible for compaction** → Apply compaction:
   *    - Mark first operation with compacted=true
   *    - Pack first operation using packArrToObjectCoList
   *    - Extract just the values from remaining operations
   *    - Return [packedFirstOp, value2, value3, ...]
   *
   * **Performance notes:**
   * - Compaction provides 60-80% size reduction for typical sequential insertions
   * - The "after" reference is typically an OpID (20-30 bytes), so savings are significant
   * - Compaction overhead is minimal (single pass through operations)
   */
  packChanges(
    changes: ListOpPayload<Item>[],
  ): PackedChanges<Item> | JsonValue[][] {
    // Empty array - return as-is
    if (changes.length === 0) {
      return [];
    }

    // Single operation - pack normally without compaction
    if (changes.length === 1) {
      return packArrOfObjectsCoList(
        changes as (AppOpPayload<Item> | DeletionOpPayload)[],
      );
    }

    const firstElement = changes[0];

    // First operation must be "app" for compaction
    if (firstElement?.op !== "app") {
      return packArrOfObjectsCoList(
        changes as (AppOpPayload<Item> | DeletionOpPayload)[],
      );
    }

    // Check if all changes are app operations with the same after reference
    // This is required for compaction to work correctly
    for (const change of changes) {
      if (change.op !== "app" || change.after !== firstElement.after) {
        // Can't compact - operations don't all share same "after" reference
        return packArrOfObjectsCoList(
          changes as (AppOpPayload<Item> | DeletionOpPayload)[],
        );
      }
    }

    // All checks passed - perform compaction
    // Mark the first element as compacted
    const firstElementCompacted = firstElement as AppOpPayload<Item> & {
      compacted: true;
    };
    firstElementCompacted.compacted = true;

    // Return: packed first operation + raw values of remaining operations
    return [
      packArrToObjectCoList(firstElementCompacted),
      ...(changes as AppOpPayload<Item>[])
        .slice(1)
        .map((change) => change.value),
    ] as PackedChanges<Item>;
  }

  /**
   * Unpacks compressed CoList operations back into full operation objects.
   *
   * @param changes - Packed changes, array of arrays, or already-unpacked operations
   * @returns Array of full operation objects ready to be applied to the list
   *
   * @remarks
   * **Input Format Detection:**
   *
   * This method intelligently handles multiple input formats:
   *
   * 1. **Already unpacked** (changes[0] is not an array)
   *    - Pass through as-is
   *    - No processing needed
   *    - Example: [{ op: "app", value: "a", after: "start" }, ...]
   *
   * 2. **Empty array**
   *    - Return [] immediately
   *    - No processing needed
   *
   * 3. **Single operation** (changes.length === 1)
   *    - Unpack using unpackArrOfObjectsCoList
   *    - Cannot be compacted by definition
   *    - Example: [["a", "start", 1]] → [{ op: "app", value: "a", after: "start" }]
   *
   * 4. **Multiple operations - non-compacted**
   *    - First operation lacks compacted flag
   *    - Unpack each operation individually
   *    - Example: [["a", "id1"], ["b", "id2"]] → full operations
   *
   * 5. **Multiple operations - compacted**
   *    - First operation has compacted=true flag
   *    - Reconstruct full operations from values
   *    - First operation contains shared "after" reference
   *    - Subsequent elements are just values
   *    - Example: [["a", "start", 1, 5], "b", "c"] → three full operations
   *
   * **Reconstruction Process (Compacted Format):**
   * 1. Unpack first operation to get shared "after" reference
   * 2. Keep first operation as-is (with compacted flag)
   * 3. For each subsequent value:
   *    - Create new { op: "app", value, after } operation
   *    - Use the "after" reference from first operation
   *
   * @example
   * Compacted input:
   * [
   *   ["a", "start", 1, 5],  // [value, after, op, compacted]
   *   "b",
   *   "c"
   * ]
   *
   * Unpacked output:
   * [
   *   { op: "app", value: "a", after: "start", compacted: true },
   *   { op: "app", value: "b", after: "start" },
   *   { op: "app", value: "c", after: "start" }
   * ]
   */
  unpackChanges(
    changes: PackedChanges<Item> | JsonValue[][] | ListOpPayload<Item>[],
  ): ListOpPayload<Item>[] {
    // Already unpacked or empty - return as-is
    if (!Array.isArray(changes[0]) || changes.length === 0) {
      return changes as ListOpPayload<Item>[];
    }

    // Single operation - unpack normally
    if (changes.length === 1) {
      return unpackArrOfObjectsCoList(
        changes as JsonValue[][],
      ) as ListOpPayload<Item>[];
    }

    // Unpack the first element to check if it's compacted
    const firstElement = unpackArrToObjectCoList(
      changes[0],
    ) as ListOpPayload<Item> & { compacted?: true };

    // Not compacted - unpack each operation individually
    if (!firstElement?.compacted) {
      return unpackArrOfObjectsCoList(
        changes as JsonValue[][],
      ) as ListOpPayload<Item>[];
    }

    // Compacted format - reconstruct full operations from values
    // The first element has the "after" reference shared by all operations
    return [
      firstElement,
      ...changes.slice(1).map((value) => ({
        op: "app" as const,
        value: value as Item,
        after: (firstElement as AppOpPayload<Item>).after,
      })),
    ];
  }
}
