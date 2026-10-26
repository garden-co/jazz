import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it, onTestFinished } from "vitest";
import type { NapiDb } from "jazz-napi";
import { encodeCellsForRow, encodeSchema } from "./native-runtime/native-runtime-adapter.js";
import { openConfig } from "./native-runtime/native-codec.js";
import { loadNapiModule } from "./testing/napi-runtime-test-utils.js";
import type { WasmSchema } from "../drivers/types.js";

const bytes = JSON.parse(
  readFileSync(
    new URL(
      "../../../../crates/jazz/fixtures/sqlite_private_owner_handoff_v1.json",
      import.meta.url,
    ),
    "utf8",
  ),
) as {
  node_owner: string;
  alice: { author: string; node_marker: string };
  bob: { author: string; node_marker: string };
};
const schema = {
  notes: { columns: [{ name: "text", column_type: { type: "Text" }, nullable: false }] },
} satisfies WasmSchema;
const encodedSchema = encodeSchema(schema);
const config = (who: "alice" | "bob") =>
  openConfig(new Uint8Array(16).fill(1), new TextEncoder().encode(bytes[who].author), 1, false);
const markerBytes = (who: "alice" | "bob") => Buffer.from(bytes[who].node_marker, "hex");

async function fixture() {
  const { NapiDb } = await loadNapiModule();
  const root = await mkdtemp(join(tmpdir(), "jazz-node-account-handoff-"));
  const handles: NapiDb[] = [];
  onTestFinished(async () => {
    for (const handle of handles.reverse()) await handle.close();
    await rm(root, { recursive: true, force: true });
  });
  return {
    root,
    marker: join(root, ".jazz-account-owner"),
    open(who: "alice" | "bob") {
      const db = NapiDb.openPersistentAccountOwner(
        root,
        encodedSchema,
        config(who),
        bytes.node_owner,
      );
      handles.push(db);
      return db;
    },
    raw(who: "alice" | "bob") {
      const db = NapiDb.openPersistent(root, encodedSchema, config(who));
      handles.push(db);
      return db;
    },
  };
}

it("requires Alice to close before linked Bob takes over and pins both v1 marker encodings", async () => {
  const { open, raw, marker } = await fixture();
  const alice = open("alice");
  const retained = alice.registerSchema(encodedSchema);
  expect(() => open("bob")).toThrow(/already open/);
  await alice.close();
  expect(await readFile(marker)).toEqual(markerBytes("alice"));
  expect(() => raw("bob")).toThrow(/another subject/);
  await open("bob").close();
  expect(await readFile(marker)).toEqual(markerBytes("bob"));
  expect(() => retained.localCurrentRow("notes", new Uint8Array(16))).toThrow();
  await retained.close();
  await open("alice").close();
  expect(await readFile(marker)).toEqual(markerBytes("alice"));
});

it("refuses Bob while Alice has pending writes and preserves Alice's row and marker", async () => {
  const { open, marker } = await fixture();
  const alice = open("alice");
  const row = new Uint8Array(16).fill(23);
  const write = alice.insert(
    "notes",
    encodeCellsForRow(schema.notes, { text: { type: "Text", value: "pending alice" } }),
    { rowId: row },
  );
  await write.wait("local");
  const original = alice.localCurrentRow("notes", row);
  await alice.close();
  expect(() => open("bob")).toThrow(/uploads are pending/);
  expect(await readFile(marker)).toEqual(markerBytes("alice"));
  const reopened = open("alice");
  expect(reopened.localCurrentRow("notes", row)).toEqual(original);
});

it.each(["alice", "bob"] as const)(
  "recovers an interrupted host marker update by reopening as %s",
  async (who) => {
    const { open, marker } = await fixture();
    await open("alice").close();
    await open("bob").close();
    // Stage the crash boundary after the core commits Bob but before the host
    // replaces Alice's marker. No runtime is live during this physical edit.
    await writeFile(marker, markerBytes("alice"));
    await open(who).close();
    expect(await readFile(marker)).toEqual(markerBytes(who));
    await open("alice").close();
    expect(await readFile(marker)).toEqual(markerBytes("alice"));
  },
);

it("keeps strict marker decoding when a different linked login requests handoff", async () => {
  const { open, marker } = await fixture();
  await open("alice").close();
  const valid = markerBytes("alice");
  const unknownVersion = Buffer.from(valid);
  unknownVersion[Buffer.byteLength("JAZZ-NODE-ACCOUNT-OWNER\0")] = 2;
  for (const corrupt of [
    valid.subarray(0, -1),
    Buffer.concat([valid, Buffer.of(0)]),
    unknownVersion,
  ]) {
    await writeFile(marker, corrupt);
    expect(() => open("bob")).toThrow();
    expect(await readFile(marker)).toEqual(corrupt);
  }
  await writeFile(marker, valid);
  await open("alice").close();
});
