import type {
  JsonObject,
  JsonValue,
  OpID,
  RawCoMap,
  RawCoPlainText,
  RawCoValue,
  Role,
} from "cojson";
import { stringifyOpID } from "cojson";
import type { VerifiedTransaction } from "cojson/dist/coValueCore/coValueCore.js";
import type { MapOpPayload } from "cojson/dist/coValues/coMap.js";
import * as TransactionChanges from "./transactions-changes";
import type {
  DeletionOpPayload,
  InsertionOpPayload,
} from "cojson/dist/coValues/coList.js";

export function areSameOpIds(
  opId1: OpID | string,
  opId2: OpID | string,
): boolean {
  if (typeof opId1 === "string" || typeof opId2 === "string") {
    return opId1 === opId2;
  }

  return (
    opId1.sessionID === opId2.sessionID &&
    opId1.txIndex === opId2.txIndex &&
    opId1.changeIdx === opId2.changeIdx
  );
}

export function isCoPlainText(coValue: RawCoValue): coValue is RawCoPlainText {
  return coValue.type === "coplaintext";
}

export function getTransactionChanges(
  tx: VerifiedTransaction,
  coValue: RawCoValue,
): JsonValue[] {
  if (tx.isValid === false && tx.tx.privacy === "private") {
    const readKey = coValue.core.getReadKey(tx.tx.keyUsed);
    if (!readKey) {
      return [
        `Unable to decrypt transaction: read key ${tx.tx.keyUsed} not found.`,
      ];
    }

    return (
      coValue.core.verified.decryptTransaction(
        tx.txID.sessionID,
        tx.txID.txIndex,
        readKey,
      ) ?? []
    );
  }

  // Trying to collapse multiple changes into a single action in the history
  if (isCoPlainText(coValue)) {
    if (tx.changes === undefined || tx.changes.length === 0) return [];
    const firstChange = tx.changes[0]!;

    if (
      TransactionChanges.isItemAppend(coValue, firstChange) &&
      tx.changes.every(
        (c) =>
          TransactionChanges.isItemAppend(coValue, c) &&
          areSameOpIds(c.after, firstChange.after),
      )
    ) {
      const changes = tx.changes as InsertionOpPayload<string>[];
      if (firstChange.after !== "start") {
        changes.reverse();
      }

      return [
        {
          op: "app",
          value: changes.map((c) => c.value).join(""),
          after: firstChange.after,
        },
      ];
    }

    if (
      TransactionChanges.isItemPrepend(coValue, firstChange) &&
      tx.changes.every(
        (c) =>
          TransactionChanges.isItemPrepend(coValue, c) &&
          areSameOpIds(c.before, firstChange.before),
      )
    ) {
      const changes = tx.changes as InsertionOpPayload<string>[];
      if (firstChange.before !== "end") {
        changes.reverse();
      }

      return [
        {
          op: "pre",
          value: changes.map((c) => c.value).join(""),
          before: firstChange.before,
        },
      ];
    }

    if (
      TransactionChanges.isItemDeletion(coValue, firstChange) &&
      tx.changes.every((c) => TransactionChanges.isItemDeletion(coValue, c))
    ) {
      const coValueBeforeDeletions = coValue.atTime(tx.madeAt - 1);

      // Verify if the deleted chars are consecutive
      function changesAreConsecutive(changes: DeletionOpPayload[]): boolean {
        if (changes.length < 2) return false;
        const mapping = coValueBeforeDeletions.mapping.idxAfterOpID;

        for (let i = 1; i < changes.length; ++i) {
          const prevIdx = mapping[stringifyOpID(changes[i - 1]!.insertion)];
          const currIdx = mapping[stringifyOpID(changes[i]!.insertion)];
          if (currIdx !== prevIdx && currIdx !== (prevIdx ?? -2) + 1) {
            return false;
          }
        }
        return true;
      }

      if (changesAreConsecutive(tx.changes)) {
        // Group the deletions by insertion.sessionID-txIndex
        // This is to help the readability of deletions that act on different previous transactions
        const groupedBySession: Map<string, DeletionOpPayload[]> = new Map();
        for (const change of tx.changes) {
          const group = `${change.insertion.sessionID}-${change.insertion.txIndex}`;
          if (!groupedBySession.has(group)) groupedBySession.set(group, []);
          groupedBySession.get(group)!.push(change);
        }

        return Array.from(groupedBySession.values()).map((changes) => {
          const stringDeleted = changes
            // order by txIndex and changeIdx
            .toSorted((a, b) => {
              if (a.insertion.txIndex === b.insertion.txIndex) {
                return a.insertion.changeIdx - b.insertion.changeIdx;
              }

              return a.insertion.txIndex - b.insertion.txIndex;
            })
            // extract the single char from the insertions
            .map((c) =>
              coValueBeforeDeletions.get(
                coValueBeforeDeletions.mapping.idxAfterOpID[
                  stringifyOpID(c.insertion)
                ]!,
              ),
            )
            .join("");

          return {
            op: "custom",
            action: `"${stringDeleted}" has been deleted`,
          };
        });
      }
    }
  }

  return tx.changes ?? (tx.tx as any).changes ?? [];
}

export function restoreCoMapToTimestamp(
  coValue: RawCoMap,
  timestamp: number,
  removeUnknownProperties: boolean,
): void {
  const myRole = coValue.group.myRole();

  if (
    myRole === undefined ||
    !(["admin", "manager", "writer", "writerOnly"] as Role[]).includes(myRole)
  ) {
    return;
  }

  const newCoValue = coValue.atTime(timestamp).toJSON() as JsonObject | null;
  const oldCoValue = coValue.toJSON() as JsonObject;

  if (newCoValue === null) return;

  let changes: MapOpPayload<string, JsonValue | undefined>[] = [];

  if (removeUnknownProperties) {
    for (const key in oldCoValue) {
      if (!(key in newCoValue)) {
        changes.push({
          op: "del",
          key,
        });
      }
    }
  }

  for (const key in newCoValue) {
    if (newCoValue[key] !== oldCoValue[key]) {
      changes.push({
        op: "set",
        key,
        value: newCoValue[key],
      });
    }
  }

  if (changes.length > 0) {
    coValue.core.makeTransaction(changes, "private");
  }
}
