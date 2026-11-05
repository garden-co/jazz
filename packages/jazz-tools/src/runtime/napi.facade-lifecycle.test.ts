import { randomUUID } from "node:crypto";
import { expect, it } from "vitest";
import { encodeCellsForRow, encodeSchema } from "./native-runtime/native-runtime-adapter.js";
import { encodedCells, openConfig, queryFromTable } from "./native-runtime/native-codec.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";
import { loadNapiModule } from "./testing/napi-runtime-test-utils.js";

const schema = encodeSchema({
  files: { columns: [{ name: "body", column_type: { type: "Text" }, nullable: false }] },
});
const bytes = (text: string) => new TextEncoder().encode(text);

async function fixture() {
  const { NapiDb } = await loadNapiModule();
  return NapiDb.openMemory(
    schema,
    openConfig(new Uint8Array(16).fill(0x83), testAuthorBytes("facade-lifecycle"), 1, false),
  );
}

// Alice closes one schema facade while retaining its JS handles. Its pending
// reads, subscription and uploads retire; her sibling facade remains usable.
it.each(["view", "owner"] as const)(
  "retires retained resources when the %s closes",
  async (closed) => {
    const owner = await fixture();
    const view = owner.registerSchema(schema);
    const sibling = owner.registerSchema(schema);
    try {
      const stream = view.beginStreamingMutation(
        "files",
        new Uint8Array(16).fill(1),
        encodedCells([], []),
        "body",
      );
      stream.push(bytes("unpublished"));
      const stagedStream = view.beginStreamingMutation(
        "files",
        new Uint8Array(16).fill(2),
        encodedCells([], []),
        "body",
      );
      stagedStream.push(bytes("staged"));
      const staged = stagedStream.stage();
      const read = view.all(queryFromTable("files"), { tier: "remote" });
      if (read instanceof Uint8Array) throw new Error("offline Remote read must remain pending");
      expect(read.poll()).toBeNull();
      const subscription = view.subscribe(queryFromTable("files"), { tier: "remote" });
      expect(subscription.readAll()).toEqual([]);
      const target = closed === "owner" ? owner : view;
      await Promise.all([target.close(), target.close()]);
      expect(() => stream.push(bytes("late"))).toThrow(/closed/);
      expect(stream.abort()).toBe(false);
      expect(staged.abort()).toBe(false);
      expect(() => read.poll()).toThrow(/complete|closed/);
      expect(() => subscription.readAll()).toThrow(/closed/);
      await view.close();
      if (closed === "view") {
        const surviving = sibling.beginStreamingMutation(
          "files",
          new Uint8Array(16).fill(3),
          encodedCells([], []),
          "body",
        );
        surviving.push(bytes("sibling remains live"));
        await surviving.finish().wait("local");
      } else {
        expect(() => sibling.localCurrentRow("files", new Uint8Array(16))).toThrow(/closed/);
      }
    } finally {
      await sibling.close();
      await view.close();
      await owner.close();
    }
  },
);

// Alice can still explicitly abort a failed upload after either publication
// attempt is refused. Neither wrapper can turn a partial upload into a row.
it("keeps failed native uploads cleanup-only after finish and stage attempts", async () => {
  const owner = await fixture();
  try {
    owner.setLargeValueStagingPolicy(1, 60_000);
    const stream = owner.beginStreamingMutation(
      "files",
      new Uint8Array(16).fill(4),
      encodedCells([], []),
      "body",
    );
    expect(() => stream.push(bytes("x".repeat(256 * 1024)))).toThrow(/rate limit/);
    expect(() => stream.finish()).toThrow(/closed/);
    expect(() => stream.stage()).toThrow(/closed/);
    expect(() => stream.push(bytes("late"))).toThrow(/closed/);
    expect(stream.abort()).toBe(true);
    expect(stream.abort()).toBe(false);
  } finally {
    await owner.close();
  }
});

// Alice's unpublished initialization belongs to its issuing facade. Closing
// that facade revokes its seal without cancelling a sibling's independent seal.
it("revokes only the closing facade's initialization seals", async () => {
  const owner = await fixture();
  const view = owner.registerSchema(schema);
  const sibling = owner.registerSchema(schema);
  const seal = (db: typeof owner, id: number) => {
    const tx = randomUUID().replaceAll("-", "");
    const rowId = new Uint8Array(16).fill(id);
    db.beginTransaction(tx, "exclusive");
    db.recordInitializationInsertAbsence(tx, "files", rowId);
    db.insertInTransaction(
      tx,
      "files",
      encodeCellsForRow(
        { columns: [{ name: "body", column_type: { type: "Text" }, nullable: false }] },
        { body: { type: "Text", value: "sealed" } },
      ),
      { rowId },
    );
    const result = db.sealInitializationTransaction(tx);
    if (!("token" in result)) throw new Error("memory seal should complete in its first turn");
    return result;
  };
  try {
    const revoked = seal(view, 5);
    const retained = seal(sibling, 6);
    await view.close();
    expect(() => owner.publishInitializationTransaction(revoked.token)).toThrow(
      /consumed|another runtime/,
    );
    expect(owner.localCurrentRow("files", new Uint8Array(16).fill(5))).toEqual(new Uint8Array([0]));
    await owner.publishInitializationTransaction(retained.token).wait("local");
    await sibling.close();
    expect(owner.localCurrentRow("files", new Uint8Array(16).fill(6)).length).toBeGreaterThan(1);
  } finally {
    await sibling.close();
    await view.close();
    await owner.close();
  }
});
