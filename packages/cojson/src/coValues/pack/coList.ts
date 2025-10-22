import { JsonValue } from "../../jsonValue";
import { AppOpPayload, ListOpPayload, OpID } from "../coList";

export type PackedChanges<T extends JsonValue> = [
  AppOpPayload<T> & { compacted: true },
  ...T[],
];

export interface CoListPack<
  Item extends JsonValue,
  PackedItems extends JsonValue[],
> {
  packChanges(
    changes: ListOpPayload<Item>[],
  ): PackedItems | ListOpPayload<Item>[];
  unpackChanges(
    changes: PackedItems | ListOpPayload<Item>[],
  ): ListOpPayload<Item>[];
}

/**
 * This class is used to pack and unpack changes for a CoList.
 * It is used to reduce the storage size of the CoList.
 */
export class CoListPackImplementation<Item extends JsonValue>
  implements CoListPack<Item, PackedChanges<Item>>
{
  packChanges(
    changes: ListOpPayload<Item>[],
  ): PackedChanges<Item> | ListOpPayload<Item>[] {
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

    // Set the compacted flag to true
    const firstElementCompacted = firstElement as AppOpPayload<Item> & {
      compacted: true;
    };
    firstElementCompacted.compacted = true;

    // Return the compacted changes and the values
    return [
      firstElementCompacted,
      ...(changes as AppOpPayload<Item>[])
        .slice(1)
        .map((change) => change.value),
    ];
  }

  unpackChanges(
    changes: PackedChanges<Item> | ListOpPayload<Item>[],
  ): ListOpPayload<Item>[] {
    // Get the first element and the values
    const [firstElement, ...values] = changes as [
      AppOpPayload<Item> & { compacted: true },
      ...Item[],
    ];

    // Check if the first element is compacted
    if (!firstElement?.compacted) {
      return changes as ListOpPayload<Item>[];
    }

    // Return the unpacked changes and the values
    return [
      firstElement as AppOpPayload<Item>,
      ...values.map((value) => ({
        op: "app" as const,
        value,
        after: firstElement.after as OpID | "start",
      })),
    ];
  }
}
