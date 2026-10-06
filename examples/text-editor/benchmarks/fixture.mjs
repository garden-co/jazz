import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { runInNewContext } from "node:vm";
import * as Y from "yjs";

export const sizes = [10_000, 50_000, 100_000, 259_778];
const directory = new URL("./.generated/", import.meta.url);
const hash = (value) => createHash("sha256").update(value).digest("hex");

export async function fixtures() {
  const manifest = JSON.parse(
    await readFile(new URL("../../../crates/jazz-sim/fixtures/manifest.json", import.meta.url)),
  );
  const pinned = manifest.fixtures.find((fixture) => fixture.name === "automerge-paper");
  await mkdir(directory, { recursive: true });
  const cache = new URL("editing-trace.js", directory);
  let source;
  try {
    source = await readFile(cache);
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  if (!source) {
    const response = await fetch(pinned.asset);
    if (!response.ok) throw new Error(`Trace download failed: ${response.status}`);
    source = Buffer.from(await response.arrayBuffer());
  }
  if (source.length !== pinned.bytes || hash(source) !== pinned.sha256)
    throw new Error("Trace checksum mismatch");
  await writeFile(cache, source);
  const context = { module: { exports: {} } };
  runInNewContext(source.toString("utf8"), context, { timeout: 5_000 });
  const { edits, finalText } = context.module.exports;
  const doc = new Y.Doc();
  doc.clientID = 1;
  const text = doc.getText("text");
  const records = [];
  const result = [];
  doc.on("update", (update) => records.push(Buffer.from(update)));
  for (let i = 0; i < edits.length; i++) {
    const [index, remove, insert = ""] = edits[i];
    doc.transact(() => {
      if (remove) text.delete(index, remove);
      if (insert) text.insert(index, insert);
    });
    if (sizes.includes(i + 1)) {
      const bytes = Buffer.concat(records);
      const value = text.toString();
      const path = new URL(`${i + 1}.yjs`, directory);
      await writeFile(path, bytes);
      result.push({ edits: i + 1, bytes: bytes.length, text: value, sha256: hash(bytes), path });
    }
  }
  if (text.toString() !== finalText || records.length !== edits.length)
    throw new Error("Trace replay mismatch");
  doc.destroy();
  return result;
}
