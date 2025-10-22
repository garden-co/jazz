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

    for (const change of changes) {
      if (change.op !== "app" || change.after !== firstElement.after) {
        return changes;
      }
    }

    const firstElementCompacted = firstElement as AppOpPayload<Item> & {
      compacted: true;
    };
    firstElementCompacted.compacted = true;

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
    const [firstElement, ...values] = changes as [
      AppOpPayload<Item> & { compacted: true },
      ...Item[],
    ];

    if (!firstElement?.compacted) {
      return changes as ListOpPayload<Item>[];
    }

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
