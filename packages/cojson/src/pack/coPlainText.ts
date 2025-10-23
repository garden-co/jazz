import { splitGraphemes } from "unicode-segmenter/grapheme";
import {
  AppOpPayload,
  DeletionOpPayload,
  ListOpPayload,
  OpID,
} from "../coValues/coList.js";
import { CoListPack } from "./coList.js";

export type PackedChangesCoPlainText = PackedChangesApp | PackedChangesDel;

export type PackedChangesApp = [
  AppOpPayload<string> & { compacted: true },
  string,
];

export type PackedChangesDel = [
  DeletionOpPayload & { compacted: true },
  ...OpID[],
];

/**
 * This class is used to pack and unpack changes for a CoPlainText.
 * It is used to reduce the storage size of the CoPlainText.
 */
export class CoPlainTextPackImplementation
  implements CoListPack<string, PackedChangesCoPlainText>
{
  packChanges(
    changes: ListOpPayload<string>[],
  ): PackedChangesCoPlainText | ListOpPayload<string>[] {
    if (changes.length === 0 || changes.length === 1) {
      return changes as ListOpPayload<string>[];
    }

    const firstElement = changes[0];

    if (firstElement?.op !== "app" && firstElement?.op !== "del") {
      return changes;
    }

    if (firstElement?.op === "app") {
      for (const change of changes) {
        if (change.op !== "app" || change.after !== firstElement.after) {
          return changes;
        }
      }

      const firstElementCompacted = firstElement as AppOpPayload<string> & {
        compacted: true;
      };
      // Set the compacted flag to true
      firstElementCompacted.compacted = true;

      // Return the compacted changes and the joined string
      return [
        firstElementCompacted,
        (changes as AppOpPayload<string>[])
          .slice(1)
          .map((change) => change.value)
          .join(""),
      ];
    } else if (firstElement?.op === "del") {
      // Check if all changes are deletion operations
      for (const change of changes) {
        if (change.op !== "del") {
          return changes;
        }
      }

      const firstElementCompacted = firstElement as DeletionOpPayload & {
        compacted: true;
      };
      // Set the compacted flag to true
      firstElementCompacted.compacted = true;

      return [
        firstElementCompacted,
        ...(changes as DeletionOpPayload[])
          .slice(1)
          .map((change) => change.insertion),
      ];
    }

    return changes;
  }

  unpackChanges(
    changes: PackedChangesCoPlainText | ListOpPayload<string>[],
  ): ListOpPayload<string>[] {
    if (changes.length === 0 || changes.length === 1) {
      return changes as ListOpPayload<string>[];
    }

    // Check if the first element is compacted
    const firstElement = changes[0] as (
      | AppOpPayload<string>
      | DeletionOpPayload
    ) & {
      compacted: true;
    };

    // Check if the first element is compacted
    if (!firstElement?.compacted) {
      return changes as ListOpPayload<string>[];
    }

    // If the first element is a deletion, return the unpacked changes and the deletions
    if (firstElement?.op === "del") {
      return [
        firstElement,
        ...changes.slice(1).map((insertion) => {
          return {
            op: "del",
            insertion,
          };
        }),
      ] as ListOpPayload<string>[];
    }

    // Get the joined string
    const elementsString = changes[1] as string;

    return [
      firstElement as AppOpPayload<string>,
      ...Array.from(splitGraphemes(elementsString)).map((grapheme) => ({
        op: "app" as const,
        value: grapheme,
        after: firstElement.after,
      })),
    ];
  }
}
