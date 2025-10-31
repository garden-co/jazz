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
  packArrOfObjectsCoList,
  unpackArrOfObjectsCoList,
  packArrToObjectCoList,
  unpackArrToObjectCoList,
} from "./objToArr.js";

/**
 * Union type representing packed changes for CoPlainText operations.
 * Supports two distinct compaction formats optimized for text.
 *
 * @remarks
 * This type can be either:
 * - PackedChangesApp: For compacted append operations (text insertion)
 * - PackedChangesDel: For compacted deletion operations (text removal)
 *
 * Each format has its own optimization strategy tailored to text operations.
 */
export type PackedChangesCoPlainText = PackedChangesApp | PackedChangesDel;

/**
 * Highly optimized compacted format for text append operations.
 * Uses string concatenation to maximize compression for text content.
 *
 * @remarks
 * Structure:
 * - First element: Packed array containing the first character operation with compacted=true
 * - Second element: Concatenated string of all remaining character values
 *
 * This format is extremely efficient for text because:
 * - Multiple characters stored as a single string (minimal JSON overhead)
 * - No repeated operation metadata for each character
 * - Perfect for representing typed text, pasted content, etc.
 *
 * Size comparison for "Hello World":
 * - Without compaction: ~500+ bytes (11 full operation objects)
 * - With this format: ~50 bytes (first operation + "ello World")
 *
 * @example
 * Compacted format:
 * [
 *   ["H", "start", 1, 5],  // [value, after, op=1, compacted=5(true)]
 *   "ello"                  // Remaining characters as single string
 * ]
 *
 * Represents these operations:
 * [
 *   { op: "app", value: "H", after: "start", compacted: true },
 *   { op: "app", value: "e", after: "start" },
 *   { op: "app", value: "l", after: "start" },
 *   { op: "app", value: "l", after: "start" },
 *   { op: "app", value: "o", after: "start" }
 * ]
 */
export type PackedChangesApp = [JsonValue[], string];

/**
 * Optimized compacted format for text deletion operations.
 * Stores the first deletion fully and subsequent deletions as just their OpIDs.
 *
 * @remarks
 * Structure:
 * - First element: Packed array containing the first deletion with compacted=true
 * - Remaining elements: OpIDs of subsequent deletions (just the insertion references)
 *
 * This format is efficient for batch deletions because:
 * - Eliminates repeated "op": "del" for each deletion
 * - Stores only the minimal information needed (OpIDs)
 * - Common when deleting multiple characters (backspace, selection delete, etc.)
 *
 * Size comparison for deleting 5 characters:
 * - Without compaction: ~300+ bytes (5 full deletion objects)
 * - With this format: ~100 bytes (first deletion + 4 OpIDs)
 *
 * @example
 * Compacted format:
 * [
 *   [opID1, 0, 3, 5],  // [insertion, compacted, op=3(del), compacted=5(true)]
 *   opID2,              // Just the OpID
 *   opID3               // Just the OpID
 * ]
 *
 * Represents these operations:
 * [
 *   { op: "del", insertion: opID1, compacted: true },
 *   { op: "del", insertion: opID2 },
 *   { op: "del", insertion: opID3 }
 * ]
 */
export type PackedChangesDel = [JsonValue[], ...OpID[]];

/**
 * Text-optimized implementation of packing/unpacking for plain text CoLists.
 * Provides superior compression for text operations compared to generic CoList packing.
 *
 * @remarks
 * **Text-Specific Optimizations:**
 *
 * This implementation goes beyond the generic CoListPack by providing two specialized
 * compaction strategies tailored for text editing operations:
 *
 * **1. Append Operation Compaction (String Joining):**
 * - Detects sequential character insertions (same "after" reference)
 * - Joins all character values into a single string
 * - Stores: [firstCharOperation, "remainingCharsAsString"]
 * - Ideal for: typing, pasting, text insertion
 * - Compression: 90%+ size reduction for typical text
 *
 * **2. Deletion Operation Compaction (OpID Array):**
 * - Detects sequential character deletions
 * - Stores first deletion fully, then just OpIDs
 * - Stores: [firstDeletion, opID2, opID3, ...]
 * - Ideal for: backspace, delete key, selection deletion
 * - Compression: 60-70% size reduction
 *
 * **Unicode Support:**
 * - Uses grapheme segmentation (not simple character splitting)
 * - Properly handles emojis, combining characters, and Unicode
 * - Example: "👨‍👩‍👧‍👦" is treated as one grapheme, not multiple characters
 *
 * **Performance Characteristics:**
 * - Packing: O(n) single pass through operations
 * - Unpacking: O(n) with grapheme segmentation overhead
 * - Memory: Minimal overhead, string operations are efficient
 *
 * **Usage Patterns:**
 * - Real-time text editing in collaborative documents
 * - Chat message synchronization
 * - Text field state management
 * - Any scenario with character-level operations
 *
 * @example
 * **Append Operations:**
 * ```typescript
 * const packer = new CoPlainTextPackImplementation();
 *
 * // Typing "Hello"
 * const operations = [
 *   { op: "app", value: "H", after: "start" },
 *   { op: "app", value: "e", after: "start" },
 *   { op: "app", value: "l", after: "start" },
 *   { op: "app", value: "l", after: "start" },
 *   { op: "app", value: "o", after: "start" }
 * ];
 *
 * const packed = packer.packChanges(operations);
 * // Result: [["H", "start", 1, 5], "ello"]
 *
 * const unpacked = packer.unpackChanges(packed);
 * // Result: Original 5 operations restored
 * ```
 *
 * @example
 * **Deletion Operations:**
 * ```typescript
 * // Deleting 3 characters
 * const deletions = [
 *   { op: "del", insertion: "opID1" },
 *   { op: "del", insertion: "opID2" },
 *   { op: "del", insertion: "opID3" }
 * ];
 *
 * const packed = packer.packChanges(deletions);
 * // Result: [["opID1", 0, 3, 5], "opID2", "opID3"]
 * ```
 */
export class CoPlainTextPackImplementation
  implements CoListPack<string, PackedChangesCoPlainText>
{
  /**
   * Packs text operations into a highly compact format using text-specific strategies.
   *
   * @param changes - Array of string-based list operations (characters)
   * @returns Compacted format (PackedChangesCoPlainText) or array of arrays (fallback)
   *
   * @remarks
   * **Compaction Decision Flow:**
   *
   * 1. **0-1 operations** → Pack without compaction
   *    - Too few operations to benefit from compaction
   *    - Return simple packed arrays
   *
   * 2. **Append operations (op === "app")**
   *    - ✅ All operations must be "app" type
   *    - ✅ All operations must share same "after" reference
   *    - If conditions met: Apply append compaction
   *      * Mark first operation with compacted=true
   *      * Pack first character operation
   *      * Join remaining characters: .map(change => change.value).join("")
   *      * Return: [packedFirstOp, "joinedString"]
   *    - If conditions not met: Pack without compaction
   *
   * 3. **Deletion operations (op === "del")**
   *    - ✅ All operations must be "del" type
   *    - If conditions met: Apply deletion compaction
   *      * Mark first operation with compacted=true
   *      * Pack first deletion operation
   *      * Extract remaining OpIDs: .map(change => change.insertion)
   *      * Return: [packedFirstOp, ...opIDs]
   *    - If conditions not met: Pack without compaction
   *
   * 4. **Mixed or prepend operations** → Pack without compaction
   *    - Cannot compact mixed operation types
   *    - Prepend operations are not compacted (rare in text editing)
   *    - Return array of packed arrays
   *
   * **Why Text-Specific Compaction Matters:**
   *
   * Text editing generates many sequential operations:
   * - Typing: Each keystroke is an append with same "after"
   * - Pasting: Multiple characters all inserted sequentially
   * - Backspace: Multiple deletions in sequence
   *
   * Without compaction, a 100-character paste would generate ~5KB of JSON.
   * With compaction, it's reduced to ~500 bytes (90% reduction).
   *
   * **Fallback Behavior:**
   *
   * When compaction isn't applicable (mixed operations, different "after" refs),
   * falls back to basic array packing via packArrOfObjectsCoList.
   * This still provides benefit from object-to-array conversion.
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
        packArrToObjectCoList(firstElementCompacted),
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
        packArrToObjectCoList(firstElementCompacted),
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
   * @returns Array of full operation objects with individual character values
   *
   * @remarks
   * **Input Format Detection:**
   *
   * This method handles multiple input formats intelligently:
   *
   * 1. **Already unpacked** (changes[0] is not an array)
   *    - Pass through as-is, no processing
   *    - Example: [{ op: "app", value: "a", ... }, ...]
   *
   * 2. **Empty array**
   *    - Return [] immediately
   *
   * 3. **Single operation** (changes.length === 1)
   *    - Unpack using unpackArrOfObjectsCoList
   *    - Cannot be compacted by definition
   *
   * 4. **Multiple operations - not compacted**
   *    - First operation lacks compacted flag
   *    - Unpack each operation individually
   *    - Example: [["a", "id1"], ["b", "id2"]]
   *
   * 5. **Multiple operations - compacted deletions** (firstElement.op === "del")
   *    - First element has compacted=true and op="del"
   *    - Remaining elements are OpIDs
   *    - Reconstruct: First deletion + array of deletions from OpIDs
   *    - Example: [["id1", 0, 3, 5], "id2"] → two deletion operations
   *
   * 6. **Multiple operations - compacted appends** (firstElement.op === "app")
   *    - First element has compacted=true and op="app"
   *    - Second element is a concatenated string
   *    - Split string into graphemes (Unicode-aware)
   *    - Reconstruct: First character + operations for each grapheme
   *    - Example: [["a", "start", 1, 5], "bcd"] → four append operations
   *
   * **Critical: Unicode Grapheme Segmentation**
   *
   * When unpacking compacted append operations, the joined string is split using
   * grapheme segmentation (via splitGraphemes), NOT simple character splitting.
   *
   * Why this matters:
   * - Emoji can be multiple Unicode codepoints: "👨‍👩‍👧‍👦" (family emoji)
   * - Combining characters: "é" can be "e" + combining acute accent
   * - Flag emoji: "🇺🇸" is two regional indicator symbols
   * - Simple .split("") would break these into invalid pieces
   * - Grapheme segmentation keeps them together as single "characters"
   *
   * This ensures:
   * - Text round-trips correctly (pack → unpack → same text)
   * - Emoji and special characters remain intact
   * - Character-based operations work as expected in UI
   *
   * **Reconstruction Process:**
   *
   * For compacted appends:
   * 1. Unpack first operation (contains shared "after" reference)
   * 2. Get the joined string from second element
   * 3. Split string into graphemes using Unicode segmentation
   * 4. Create append operation for each grapheme with shared "after"
   *
   * For compacted deletions:
   * 1. Unpack first operation (full deletion with compacted flag)
   * 2. For each remaining OpID, create { op: "del", insertion: opID }
   *
   * @example
   * **Unpacking compacted appends:**
   * ```typescript
   * Input: [
   *   ["👋", "start", 1, 5],  // [value, after, op=1, compacted=5(true)]
   *   "🌍✨"                    // Joined string
   * ]
   *
   * Output: [
   *   { op: "app", value: "👋", after: "start", compacted: true },
   *   { op: "app", value: "🌍", after: "start" },
   *   { op: "app", value: "✨", after: "start" }
   * ]
   * ```
   *
   * Note: Each emoji is correctly treated as a single grapheme, even if it's
   * composed of multiple Unicode codepoints internally.
   *
   * @example
   * **Unpacking compacted deletions:**
   * ```typescript
   * Input: [
   *   ["opID1", 0, 3, 5],  // [insertion, compacted, op=3, compacted=5(true)]
   *   "opID2",
   *   "opID3"
   * ]
   *
   * Output: [
   *   { op: "del", insertion: "opID1", compacted: true },
   *   { op: "del", insertion: "opID2" },
   *   { op: "del", insertion: "opID3" }
   * ]
   * ```
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
    const firstElement = unpackArrToObjectCoList(
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
        unpackArrToObjectCoList(changes[0]) as DeletionOpPayload,
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
