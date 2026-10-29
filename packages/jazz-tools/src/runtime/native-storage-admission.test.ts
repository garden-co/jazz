import { createHash } from "node:crypto";
import {
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  readlinkSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { NapiDb } from "jazz-napi";
import { describe, expect, it, onTestFinished } from "vitest";
import { schema as s } from "../index.js";
import { NativeRuntimeAdapter } from "./native-runtime/native-runtime-adapter.js";
import { openConfig } from "./native-runtime/native-codec.js";
import { nativeCoreErrorCode } from "./native-runtime/native-error-code.js";
import { encodeSchema } from "./native-runtime/schema-codec.js";
import { translateQuery } from "./query-adapter.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) });
const node = new Uint8Array(16).fill(41);
const author = testAuthorBytes("native-storage-admission");
const config = openConfig(node, author, 1, true);

function temporaryRoot() {
  const root = mkdtempSync(join(tmpdir(), "jazz-native-admission-"));
  onTestFinished(() => rmSync(root, { recursive: true, force: true }));
  return root;
}

// Record file bytes and entry types without following links. The refusal must
// leave both the supplied root and any linked external files untouched.
function snapshot(root: string): Record<string, string> {
  const result: Record<string, string> = {};
  const visit = (path: string, relative: string) => {
    const stat = lstatSync(path);
    if (stat.isSymbolicLink()) result[relative] = `link:${readlinkSync(path)}`;
    else if (stat.isDirectory()) {
      result[relative] = "directory";
      for (const name of readdirSync(path).sort()) visit(join(path, name), `${relative}/${name}`);
    } else result[relative] = createHash("sha256").update(readFileSync(path)).digest("hex");
  };
  visit(root, ".");
  return result;
}

for (const backend of [false, true]) {
  describe(`${backend ? "backend" : "client"} persistent root admission`, () => {
    function open(root: string) {
      return backend
        ? NapiDb.openPersistentAsBackend(root, encodeSchema(app.wasmSchema), config)
        : NapiDb.openPersistent(root, encodeSchema(app.wasmSchema), config);
    }

    async function refused(root: string) {
      let error: unknown;
      let db: ReturnType<typeof open> | undefined;
      try {
        db = open(root);
      } catch (thrown) {
        error = thrown;
      } finally {
        await db?.close();
      }
      expect(error).toBeInstanceOf(Error);
      expect(nativeCoreErrorCode(error)).toBe("unsupported_storage_codecs");
      expect((error as Error).message).toContain("jazz.sqlite.native-root.v1");
    }

    it.each([".DS_Store", "notes.txt", "jazz.sqlite.bak"])(
      "ignores a stray regular file %s and opens the store beside it",
      async (name) => {
        const root = temporaryRoot();
        writeFileSync(join(root, name), "Alice's unrelated file");
        const before = snapshot(root)[`./${name}`];
        const db = open(root);
        await db.close();
        expect(readdirSync(root)).toContain("jazz.sqlite");
        expect(snapshot(root)[`./${name}`]).toBe(before);
      },
    );

    it.each(["other.sqlite", "legacy.db", "legacy.db-wal", "CURRENT", "MANIFEST-000001"])(
      "refuses a foreign store file %s without creating a neighbouring empty store",
      async (name) => {
        const root = temporaryRoot();
        writeFileSync(join(root, name), "Alice's existing data");
        const before = snapshot(root);
        await refused(root);
        expect(snapshot(root)).toEqual(before);
      },
    );

    it("refuses a directory in the root without touching it", async () => {
      const root = temporaryRoot();
      mkdirSync(join(root, "attachments"));
      writeFileSync(join(root, "attachments", "retained"), "Alice's nested data");
      const before = snapshot(root);
      await refused(root);
      expect(snapshot(root)).toEqual(before);
    });

    it("refuses a directory occupying a recognised SQLite filename", async () => {
      const root = temporaryRoot();
      mkdirSync(join(root, "jazz.sqlite"));
      writeFileSync(join(root, "jazz.sqlite", "retained"), "Alice's nested data");
      const before = snapshot(root);
      await refused(root);
      expect(snapshot(root)).toEqual(before);
    });

    it
      .skipIf(process.platform === "win32")
      .each([
        "jazz.sqlite",
        "jazz.sqlite-wal",
        "jazz.sqlite-shm",
        "jazz.sqlite-journal",
        "notes.txt",
        ".DS_Store",
      ])("refuses a symlink at %s without touching its target", async (name) => {
      const parent = temporaryRoot();
      const root = join(parent, "alice");
      mkdirSync(root);
      const target = join(parent, "bob.data");
      writeFileSync(target, "Bob's external data");
      symlinkSync(target, join(root, name));
      const before = snapshot(parent);
      await refused(root);
      expect(snapshot(parent)).toEqual(before);
    });

    it("creates a missing root and preserves ordinary rows when reopened", async () => {
      const parent = temporaryRoot();
      const root = join(parent, "new", "database");
      const runtime = () =>
        new NativeRuntimeAdapter(NapiDb, app.wasmSchema, node, author, 1, true, {
          persistentPath: root,
          backendMode: backend,
        });
      const first = runtime();
      try {
        first.insert("notes", { text: { type: "Text", value: "Alice's saved note" } });
      } finally {
        await first.close();
      }
      expect(readdirSync(root)).toContain("jazz.sqlite");
      const reopened = runtime();
      try {
        const rows = await reopened.query(translateQuery(app.notes._build(), app.wasmSchema));
        expect(rows).toEqual([
          expect.objectContaining({
            values: [{ type: "Text", value: "Alice's saved note" }],
          }),
        ]);
      } finally {
        await reopened.close();
      }
    });
  });
}
