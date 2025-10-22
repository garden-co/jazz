import { splitGraphemes } from "unicode-segmenter/grapheme";
import { AppOpPayload, ListOpPayload, OpID } from "../coList";
import { CoListPack } from "./coList";

export type PackedChangesCoPlainText = [
  AppOpPayload<string> & { compacted: true },
  string,
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
    const firstElement = changes[0];

    if (firstElement?.op !== "app") {
      return changes;
    }

    // Check if all changes are app operations with the same after reference
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
  }

  unpackChanges(
    changes: PackedChangesCoPlainText | ListOpPayload<string>[],
  ): ListOpPayload<string>[] {
    // Check if the first element is compacted
    const firstElement = changes[0] as AppOpPayload<string> & {
      compacted: true;
    };

    // Check if the first element is compacted
    if (!firstElement?.compacted) {
      return changes as ListOpPayload<string>[];
    }

    // Get the joined string
    const elementsString = changes[1] as string;

    return [
      firstElement as AppOpPayload<string>,
      ...Array.from(splitGraphemes(elementsString)).map((grapheme) => ({
        op: "app" as const,
        value: grapheme,
        after: firstElement.after as OpID | "start",
      })),
    ];
  }
}
