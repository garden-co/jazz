import type { JsonObject, JsonValue, RawCoMap, Role } from "cojson";
import type { MapOpPayload } from "cojson/dist/coValues/coMap.js";

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
