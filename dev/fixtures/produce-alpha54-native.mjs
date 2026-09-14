// Explicit maintainer action only. Never invoke this producer in CI.
// Arguments are extracted, integrity-verified npm package directories and a
// fresh output directory. No workspace Jazz code is imported.
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
const [toolsPath, napiPath, outputPath] = process.argv.slice(2);
if (!outputPath)
  throw new Error("usage: node produce-alpha54-native.mjs TOOLS_PACKAGE NAPI_PACKAGE NEW_OUTPUT");
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
const { encodeSchema, encodeCellsForRow, encodeCellsForPatch } = await load(
  toolsPath,
  "dist/runtime/native-runtime/native-runtime-adapter.js",
);
const { openConfig } = await load(toolsPath, "dist/runtime/native-runtime/native-codec.js");
const { NapiDb } = await load(napiPath, "index.mjs");
const reopen = process.argv[5] === "--reopen";
if (!reopen) await mkdir(outputPath); // Never reuse an existing database.
const app = s.defineApp({ notes: s.table({ body: s.string() }) });
const config = openConfig(
  new Uint8Array(16).fill(42),
  new TextEncoder().encode('["https://fixture.invalid","producer"]'),
  1,
  true,
);
const database = resolve(outputPath, "rocksdb-epoch-1");
const id = new Uint8Array(16).fill(43);
const open = () => NapiDb.openPersistentAsBackend(database, encodeSchema(app.wasmSchema), config);
let db = open();
if (!reopen) {
  try {
    db.insert(
      "notes",
      encodeCellsForRow(app.wasmSchema.notes, {
        body: { type: "Text", value: "published alpha.54 original" },
      }),
      { rowId: id, updatedAtMs: 100 },
    );
    db.tick();
    db.update(
      "notes",
      id,
      encodeCellsForPatch(app.wasmSchema.notes, {
        body: { type: "Text", value: "published alpha.54 current" },
      }),
      { updatedAtMs: 101 },
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
    const row = db.localCurrentRow("notes", id);
    // Binding bytes must contain the expected public text value, not only a row header.
    if (!new TextDecoder().decode(row).includes("published alpha.54 current"))
      throw new Error("published runtime failed to reopen its current row");
    await writeFile(
      resolve(outputPath, "local-current-row.base64"),
      Buffer.from(row).toString("base64") + "\n",
      { flag: "wx" },
    );
  } finally {
    await db.close();
  }
  console.log("Published alpha.54 RocksDB reopen completed");
}
