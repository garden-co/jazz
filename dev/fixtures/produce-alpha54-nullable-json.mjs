// Explicit maintainer action only. Never invoke this producer in CI.
// Arguments are extracted, integrity-verified npm package directories and a
// fresh output directory. No workspace Jazz code is imported.
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
const [toolsPath, napiPath, outputPath] = process.argv.slice(2);
if (!outputPath)
  throw new Error(
    "usage: node produce-alpha54-nullable-json.mjs TOOLS_PACKAGE NAPI_PACKAGE NEW_OUTPUT",
  );
for (const [root, name] of [
  [toolsPath, "jazz-tools"],
  [napiPath, "jazz-napi"],
]) {
  const pkg = JSON.parse(await readFile(resolve(root, "package.json"), "utf8"));
  if (pkg.name !== name || pkg.version !== "2.0.0-alpha.54")
    throw new Error("producer requires the pinned alpha.54 packages");
}
const load = (root, file) => import(pathToFileURL(resolve(root, file)).href);
const { schema: s } = await load(toolsPath, "dist/schema-namespace.js");
const { encodeSchema, encodeCellsForPatch } = await load(
  toolsPath,
  "dist/runtime/native-runtime/native-runtime-adapter.js",
);
const { openConfig, PostcardReader, readNativeRowBatch } = await load(
  toolsPath,
  "dist/runtime/native-runtime/native-codec.js",
);
const { createRecordValueDecoder } = await load(
  toolsPath,
  "dist/runtime/native-runtime/native-row-codec.js",
);
const { NapiDb } = await load(napiPath, "index.mjs");
const reopen = process.argv[5] === "--reopen";
if (!reopen) await mkdir(outputPath); // Never reuse an existing database.
const app = s.defineApp({ documents: s.table({ name: s.string(), payload: s.json().optional() }) });
const cases = [
  ["omitted", undefined],
  ["object", { answer: 42 }],
  ["root-null", null],
  ["nested-null", { nested: null }],
  ["array", [null, 1]],
  ["string-null", "null"],
  ["large", { padding: "x".repeat(70000) }],
  ["indirect-null", null],
];
const config = openConfig(
  new Uint8Array(16).fill(42),
  new TextEncoder().encode('["https://fixture.invalid","producer"]'),
  1,
  true,
);
const database = resolve(outputPath, "rocksdb-epoch-1");
// The published optional-cell carrier rejects every present JSON value.
// Its public typed NAPI boundary also accepts the scalar carrier used by
// StoredScalar(Json); use the published codec for that carrier while keeping
// the registered schema nullable. This is not a high-level client success.
const scalarCarrier = {
  ...app.wasmSchema.documents,
  columns: app.wasmSchema.documents.columns.map((column) =>
    column.name === "payload" ? { ...column, nullable: false } : column,
  ),
};
const sourceFor = (name, value) =>
  name === "indirect-null" ? `${" ".repeat(4095)}null${"\n".repeat(90000)}` : JSON.stringify(value);
const open = () => NapiDb.openPersistentAsBackend(database, encodeSchema(app.wasmSchema), config);
let db = open();
if (!reopen) {
  try {
    for (const [index, [name, value]] of cases.entries()) {
      const cells = { name: { type: "Text", value: name } };
      if (value !== undefined) cells.payload = { type: "Text", value: sourceFor(name, value) };
      db.insert("documents", encodeCellsForPatch(scalarCarrier, cells), {
        rowId: new Uint8Array(16).fill(index + 43),
        updatedAtMs: 100 + index,
      });
      db.tick();
    }
    db.update(
      "documents",
      new Uint8Array(16).fill(44),
      encodeCellsForPatch(scalarCarrier, { payload: { type: "Text", value: '{"answer":43}' } }),
      { updatedAtMs: 120 },
    );
    db.tick();
  } finally {
    await db.close();
  }
  // The distributed runtime retains handles until process exit. Reopen in a
  // fresh process after this one exits; see the documented two commands.
  console.log("Published alpha.54 RocksDB producer completed");
} else {
  try {
    const receipts = [];
    for (const [index, [name, value]] of cases.entries()) {
      const row = db.localCurrentRow("documents", new Uint8Array(16).fill(index + 43));
      const [batch] = new PostcardReader(row).readVec(readNativeRowBatch);
      if (batch.rows.length !== 1) throw new Error(`missing published row ${name}`);
      const payloadIndex = batch.descriptor.findIndex(
        (field) => (field.outputName || field.name) === "payload",
      );
      const payload = createRecordValueDecoder(batch.descriptor)(batch.rows[0].raw, payloadIndex);
      const expected = name === "object" ? '{"answer":43}' : sourceFor(name, value);
      if (
        value === undefined
          ? payload !== null
          : payload?.[0] !== 2 || new TextDecoder().decode(payload.subarray(1)) !== expected
      ) {
        throw new Error(`published JSON value mismatch: ${name}`);
      }
      receipts.push({ name, row: Buffer.from(row).toString("base64") });
    }
    await writeFile(
      resolve(outputPath, "local-current-rows.json"),
      JSON.stringify(receipts) + "\n",
      { flag: "wx" },
    );
  } finally {
    await db.close();
  }
  console.log("Published alpha.54 RocksDB reopen completed");
}
