import { JsonValue } from "../jsonValue";
import {
  AppOpPayload,
  DeletionOpPayload,
  ListOpPayload,
} from "../coValues/coList.js";
import {
  getOperationType,
  packArrOfObjectsCoList,
  packObjectToArr,
  unpackArrToObject,
  unpackArrOfObjectsCoList,
  LIST_TO_KEYS_MAP,
} from "./objToArr.js";

export type Operations<T extends JsonValue> = ListOpPayload<T>["op"];

/**
 * Type representing compacted changes for CoList operations.
 * The first element is a packed array containing the first operation with compacted flag,
 * followed by the raw values of subsequent operations.
 *
 * @example
 * [
 *   ["app", "value1", "start", true],  // First operation with compacted=true
 *   "value2",                            // Just the value of second operation
 *   "value3"                             // Just the value of third operation
 * ]
 */
export type PackedChanges<T extends JsonValue> = [
  JsonValue[], // Packed AppOpPayload<T> & { compacted: true }
  ...T[],
];

/**
 * Interface for packing and unpacking CoList operations.
 * Implementations should compress sequential operations to save storage space.
 *
 * @template Item - The type of items stored in the CoList
 * @template PackedItems - The type of the packed representation
 */
export interface CoListPack<
  Item extends JsonValue,
  PackedItems extends JsonValue[] = PackedChanges<Item>,
> {
  /**
   * Compresses an array of operations into a more compact format.
   *
   * @param changes - Array of list operations to pack
   * @returns Packed representation or array of arrays if compaction is not possible
   */
  packChanges(changes: ListOpPayload<Item>[]): PackedItems | JsonValue[][];

  /**
   * Decompresses packed operations back into full operation objects.
   *
   * @param changes - Packed operations, array of arrays, or already unpacked operations
   * @returns Array of full operation objects
   */
  unpackChanges(
    changes: PackedItems | JsonValue[][] | ListOpPayload<Item>[],
  ): ListOpPayload<Item>[];
}

/**
 * Implementation of CoList packing/unpacking that optimizes storage for sequential operations.
 *
 * @remarks
 * This class performs smart compression when multiple append operations share the same 'after' reference.
 * Instead of storing the full operation object for each item, it stores:
 * - First operation with compacted=true flag
 * - Just the values for subsequent operations
 *
 * This significantly reduces JSON size when adding multiple items in sequence.
 *
 * @example
 * Input: [
 *   { op: "app", value: "a", after: "start" },
 *   { op: "app", value: "b", after: "start" },
 *   { op: "app", value: "c", after: "start" }
 * ]
 *
 * Output: [
 *   ["app", "a", "start", true],
 *   "b",
 *   "c"
 * ]
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
   * Compaction is only applied when:
   * - There are 2+ operations
   * - All operations are "app" (append) operations
   * - All operations share the same "after" reference
   *
   * When compaction is possible:
   * - First operation is packed with compacted=true
   * - Remaining operations are stored as just their values
   *
   * Otherwise, operations are packed individually as arrays.
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
      packObjectToArr(LIST_TO_KEYS_MAP["app"], firstElementCompacted),
      ...(changes as AppOpPayload<Item>[])
        .slice(1)
        .map((change) => change.value),
    ] as PackedChanges<Item>;
  }

  /**
   * Unpacks compressed CoList operations back into full operation objects.
   *
   * @param changes - Packed changes, array of arrays, or already-unpacked operations
   * @returns Array of full operation objects ready to be applied
   *
   * @remarks
   * This method handles three input formats:
   * 1. Already unpacked operations (passed through as-is)
   * 2. Array of arrays without compaction (unpacked individually)
   * 3. Compacted format (first operation + values → reconstructed operations)
   *
   * When unpacking compacted format:
   * - Reads the first operation which contains the shared "after" reference
   * - Reconstructs full operations for remaining values using the shared "after"
   *
   * @example
   * Input: [
   *   ["app", "a", "start", true],
   *   "b",
   *   "c"
   * ]
   *
   * Output: [
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

    const op: Operations<Item> = getOperationType(changes[0] as JsonValue[]);

    // Unpack the first element to check if it's compacted
    const firstElement = unpackArrToObject(
      LIST_TO_KEYS_MAP[op],
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
