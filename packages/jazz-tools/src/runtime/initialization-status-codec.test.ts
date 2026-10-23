import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { decodeInitializationStatuses, type ReservedTxId } from "./provisional-initialization.js";

it("decodes the SQLite host's exact initialization status bridge fixture", () => {
  const fixture = JSON.parse(
    readFileSync(
      new URL("../../../../crates/jazz/fixtures/sqlite_initialization_v1.json", import.meta.url),
      "utf8",
    ),
  );
  const id = fixture.reservation as ReservedTxId;
  expect(decodeInitializationStatuses(fixture.pendingStatusJson, [id])).toEqual([
    {
      kind: "complete",
      reservedTxId: id,
      fate: { kind: "pending" },
      durability: "local",
    },
  ]);
  expect(() =>
    decodeInitializationStatuses(fixture.pendingStatusJson, ["foreign" as ReservedTxId]),
  ).toThrow("identity mismatch");
});
