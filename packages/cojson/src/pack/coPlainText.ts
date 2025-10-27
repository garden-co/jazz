import { splitGraphemes } from "unicode-segmenter/grapheme";
import {
  AppOpPayload,
  DeletionOpPayload,
  ListOpPayload,
  OpID,
} from "../coValues/coList.js";
import { CoListPack } from "./coList.js";
import { JsonValue } from "../jsonValue.js";
import {
  getOperationType,
  LIST_KEYS_DELETION,
  packArrOfObjectsCoList,
  packObjectToArr,
  unpackArrToObject,
  unpackArrOfObjectsCoList,
  LIST_TO_KEYS_MAP,
} from "./objToArr.js";

/**
 * Union type representing packed changes for CoPlainText.
 * Can be either compacted append operations or compacted deletion operations.
 */
export type PackedChangesCoPlainText = PackedChangesApp | PackedChangesDel;

/**
 * Compacted format for text append operations.
 * First element contains the packed first operation with compacted flag.
 * Second element is a concatenated string of all remaining character values.
 *
 * @example
 * [
 *   ["app", "H", "start", true],  // First character operation
 *   "ello"                         // Remaining characters joined
 * ]
 * Represents: "Hello"
 */
export type PackedChangesApp = [JsonValue[], string];

/**
 * Compacted format for deletion operations.
 * First element contains the packed first deletion with compacted flag.
 * Remaining elements are the OpIDs of subsequent deletions.
 *
 * @example
 * [
 *   ["del", opID1, true],  // First deletion operation
 *   opID2,                 // Second deletion OpID
 *   opID3                  // Third deletion OpID
 * ]
 */
export type PackedChangesDel = [JsonValue[], ...OpID[]];

/**
 * Implementation of packing/unpacking optimized for plain text CoLists.
 *
 * @remarks
 * This class provides text-specific optimizations beyond the generic CoList packer:
 *
 * For append operations:
 * - Joins multiple character values into a single string
 * - Significantly reduces JSON overhead for long text sequences
 *
 * For deletion operations:
 * - Stores first deletion + array of OpIDs
 * - Optimizes batch deletions
 *
 * Handles Unicode properly by using grapheme segmentation during unpacking.
 *
 * @example
 * Packing text "Hello":
 * Input: [
 *   { op: "app", value: "H", after: "start" },
 *   { op: "app", value: "e", after: "start" },
 *   { op: "app", value: "l", after: "start" },
 *   { op: "app", value: "l", after: "start" },
 *   { op: "app", value: "o", after: "start" }
 * ]
 *
 * Output: [
 *   ["app", "H", "start", true],
 *   "ello"
 * ]
 */
export class CoPlainTextPackImplementation
  implements CoListPack<string, PackedChangesCoPlainText>
{
  /**
   * Packs text operations into a highly compact format.
   *
   * @param changes - Array of string-based list operations
   * @returns Compacted format or array of arrays if compaction isn't applicable
   *
   * @remarks
   * Compaction strategies:
   *
   * For append operations (when all share same "after"):
   * - First character stored with full operation + compacted flag
   * - Remaining characters joined into single string
   * - Massive space savings for long text
   *
   * For deletion operations (when all are deletions):
   * - First deletion stored with full operation + compacted flag
   * - Remaining deletions stored as just their OpIDs
   *
   * No compaction for:
   * - 0-1 operations
   * - Mixed operation types
   * - Different "after" references
   * - Operations containing "pre" (prepend)
   */
  packChanges(
    changes: ListOpPayload<string>[],
  ): PackedChangesCoPlainText | JsonValue[][] {
    // 0-1 operations: pack normally without compaction
    if (changes.length === 0 || changes.length === 1) {
      return packArrOfObjectsCoList(
        changes as (AppOpPayload<string> | DeletionOpPayload)[],
      );
    }

    const firstElement = changes[0];

    // First element must be "app" or "del" for compaction
    if (firstElement?.op !== "app" && firstElement?.op !== "del") {
      return packArrOfObjectsCoList(
        changes as (AppOpPayload<string> | DeletionOpPayload)[],
      );
    }

    // Handle append operations compaction
    if (firstElement?.op === "app") {
      // Verify all operations are appends with same "after" reference
      for (const change of changes) {
        if (change.op !== "app" || change.after !== firstElement.after) {
          // Can't compact - different operation types or "after" references
          return packArrOfObjectsCoList(
            changes as (AppOpPayload<string> | DeletionOpPayload)[],
          );
        }
      }

      // Compact: first character + joined remaining characters
      const firstElementCompacted = firstElement as AppOpPayload<string> & {
        compacted: true;
      };
      firstElementCompacted.compacted = true;

      // Join all subsequent character values into a single string
      return [
        packObjectToArr(LIST_TO_KEYS_MAP["app"], firstElementCompacted),
        (changes as AppOpPayload<string>[])
          .slice(1)
          .map((change) => change.value)
          .join(""),
      ];
    } else if (firstElement?.op === "del") {
      // Verify all operations are deletions
      for (const change of changes) {
        if (change.op !== "del") {
          // Can't compact - mixed with non-deletion operations
          return packArrOfObjectsCoList(
            changes as (AppOpPayload<string> | DeletionOpPayload)[],
          );
        }
      }

      // Compact: first deletion + array of OpIDs
      const firstElementCompacted = firstElement as DeletionOpPayload & {
        compacted: true;
      };
      firstElementCompacted.compacted = true;

      return [
        packObjectToArr(LIST_TO_KEYS_MAP["del"], firstElementCompacted),
        ...(changes as DeletionOpPayload[])
          .slice(1)
          .map((change) => change.insertion),
      ];
    }

    // Fallback: pack without compaction
    return packArrOfObjectsCoList(
      changes as (AppOpPayload<string> | DeletionOpPayload)[],
    );
  }

  /**
   * Unpacks compressed text operations back into individual character operations.
   *
   * @param changes - Packed text changes, array of arrays, or already-unpacked operations
   * @returns Array of full operation objects with individual characters
   *
   * @remarks
   * Handles three input formats:
   * 1. Already unpacked operations (passed through)
   * 2. Non-compacted array of arrays (unpacked individually)
   * 3. Compacted format (reconstructed from joined string or OpID array)
   *
   * For compacted append operations:
   * - Splits the joined string back into graphemes (not just characters!)
   * - Uses Unicode grapheme segmentation to handle emojis and complex characters correctly
   * - Each grapheme becomes a separate operation with the shared "after" reference
   *
   * For compacted deletion operations:
   * - Reconstructs deletion operations from the OpID array
   * - Each OpID becomes a full deletion operation
   *
   * @example
   * Input: [
   *   ["app", "👋", "start", true],
   *   "🌍✨"
   * ]
   *
   * Output: [
   *   { op: "app", value: "👋", after: "start", compacted: true },
   *   { op: "app", value: "🌍", after: "start" },
   *   { op: "app", value: "✨", after: "start" }
   * ]
   *
   * Note: Grapheme splitting ensures emoji and combining characters are handled correctly
   */
  unpackChanges(
    changes: PackedChangesCoPlainText | ListOpPayload<string>[] | JsonValue[][],
  ): ListOpPayload<string>[] {
    // Already unpacked or empty - return as-is
    if (!Array.isArray(changes[0]) || changes.length === 0) {
      return changes as ListOpPayload<string>[];
    }

    // Single operation - unpack normally
    if (changes.length === 1) {
      return unpackArrOfObjectsCoList(
        changes as JsonValue[][],
      ) as ListOpPayload<string>[];
    }

    // Unpack first element to check if it's compacted
    const op = getOperationType(changes[0] as JsonValue[]);
    const firstElement = unpackArrToObject(
      LIST_TO_KEYS_MAP[op],
      changes[0],
    ) as ListOpPayload<string> & { compacted?: true };

    // Not compacted - unpack each operation individually
    if (!firstElement?.compacted) {
      return unpackArrOfObjectsCoList(
        changes as JsonValue[][],
      ) as ListOpPayload<string>[];
    }

    // Compacted deletions - reconstruct from OpID array
    if (firstElement?.op === "del") {
      return [
        unpackArrToObject(
          LIST_TO_KEYS_MAP["del"],
          changes[0] as JsonValue[],
        ) as DeletionOpPayload,
        ...changes.slice(1).map((insertion) => {
          return {
            op: "del",
            insertion,
          };
        }),
      ] as ListOpPayload<string>[];
    }

    // Compacted appends - split joined string into graphemes
    // Using grapheme segmentation to properly handle Unicode (emojis, combining chars, etc.)
    const elementsString = changes[1] as string;

    return [
      firstElement as AppOpPayload<string>,
      ...Array.from(splitGraphemes(elementsString)).map((grapheme) => ({
        op: "app" as const,
        value: grapheme,
        after: (firstElement as AppOpPayload<string>).after,
      })),
    ];
  }
}
