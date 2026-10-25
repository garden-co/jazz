import { mkdtemp, mkdir, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it, onTestFinished } from "vitest";
import type { NapiDb } from "jazz-napi";
import { encodeSchema } from "./native-runtime/native-runtime-adapter.js";
import { openConfig } from "./native-runtime/native-codec.js";
import { loadNapiModule } from "./testing/napi-runtime-test-utils.js";

// Independent byte fixture for the existing account-marker V1 grammar. The
// SQLite root layout changes; the account owner marker must remain identical.
const OWNER =
  '{"version":1,"appId":"native-root-fixture","env":"test","auth":{"kind":"account","account":"01010101-0101-0101-0101-010101010101","registry":"https://registry.example/accounts"}}';
const AUTHOR = '["01010101-0101-0101-0101-010101010101","https://issuer.example","alice"]';
const MARKER_V1 = Buffer.from(
  "4a415a5a2d4e4f44452d4143434f554e542d4f574e45520001000000b27b2276657273696f6e223a312c226170704964223a226e61746976652d726f6f742d66697874757265222c22656e76223a2274657374222c2261757468223a7b226b696e64223a226163636f756e74222c226163636f756e74223a2230313031303130312d303130312d303130312d303130312d303130313031303130313031222c227265676973747279223a2268747470733a2f2f72656769737472792e6578616d706c652f6163636f756e7473227d7d000000495b2230313031303130312d303130312d303130312d303130312d303130313031303130313031222c2268747470733a2f2f6973737565722e6578616d706c65222c22616c696365225d",
  "hex",
);
const TEMP_NAME = ".jazz-account-owner-019a0f00000070008000000000000001.tmp";
const schema = encodeSchema({
  notes: { columns: [{ name: "text", column_type: { type: "Text" }, nullable: false }] },
});
const config = openConfig(new Uint8Array(16).fill(1), new TextEncoder().encode(AUTHOR), 1, false);

async function fixture() {
  const { NapiDb } = await loadNapiModule();
  const root = await mkdtemp(join(tmpdir(), "jazz-native-root-"));
  const handles: NapiDb[] = [];
  onTestFinished(async () => {
    for (const handle of handles.reverse()) await handle.close();
    await rm(root, { recursive: true, force: true });
  });
  return {
    root,
    open(owner = OWNER) {
      const db = NapiDb.openPersistentAccountOwner(root, schema, config, owner);
      handles.push(db);
      return db;
    },
  };
}

it("pins the SQLite root layout and preserves the account-marker V1 bytes across reopen", async () => {
  const { root, open } = await fixture();
  await open().close();
  expect((await readFile(join(root, "jazz.sqlite"))).subarray(0, 16)).toEqual(
    Buffer.from("SQLite format 3\0"),
  );
  expect(await readFile(join(root, ".jazz-account-owner"))).toEqual(MARKER_V1);
  const names = await readdir(root);
  expect(names).toEqual(
    expect.arrayContaining([".jazz-account-owner", ".jazz-native-owner.lock", "jazz.sqlite"]),
  );
  expect(
    names.every((name) =>
      [
        ".jazz-account-owner",
        ".jazz-native-owner.lock",
        "jazz.sqlite",
        "jazz.sqlite-wal",
        "jazz.sqlite-shm",
        "jazz.sqlite-journal",
      ].includes(name),
    ),
  ).toBe(true);
  await open().close();
  expect(await readFile(join(root, ".jazz-account-owner"))).toEqual(MARKER_V1);
});

it("keeps the root locked until its owner closes and retires retained schema views", async () => {
  const { open } = await fixture();
  const owner = open();
  const view = owner.registerSchema(schema);
  await view.close();
  expect(() => open()).toThrow(/already open/);
  const retained = owner.registerSchema(schema);
  await owner.close();
  const reopened = open();
  expect(() => retained.localCurrentRow("notes", new Uint8Array(16))).toThrow();
  await retained.close();
  expect(() => open()).toThrow(/already open/);
  await reopened.close();
});

it.each([false, true])(
  "recovers a regular crash-left owner temp file (marker installed: %s)",
  async (installed) => {
    const { root, open } = await fixture();
    if (installed) await open().close();
    const temporary = join(root, TEMP_NAME);
    // Both partial bytes and a complete marker can survive a process crash.
    const bytes = installed ? MARKER_V1 : Buffer.from("partial marker");
    await writeFile(temporary, bytes);
    await open().close();
    expect(await readFile(join(root, ".jazz-account-owner"))).toEqual(MARKER_V1);
    expect(await readFile(temporary)).toEqual(bytes);
  },
);

it("does not let a temporary marker replace the installed owner", async () => {
  const { root, open } = await fixture();
  await open().close();
  await writeFile(join(root, TEMP_NAME), MARKER_V1);
  expect(() => open(OWNER.replace('"test"', '"other"'))).toThrow(/owner marker is mismatched/);
  expect(await readFile(join(root, ".jazz-account-owner"))).toEqual(MARKER_V1);
  await open().close();
});

it.each([
  "CURRENT",
  "MANIFEST-000001",
  ".jazz-account-owner-not-a-uuid.tmp",
  ".jazz-account-owner-019a0f00000040008000000000000001.tmp",
  ".jazz-account-owner-019A0F00000070008000000000000001.tmp",
])("refuses foreign root entry %s before installing an owner or opening SQLite", async (name) => {
  const { root, open } = await fixture();
  const bytes = Buffer.from("foreign storage must remain intact");
  await writeFile(join(root, name), bytes);
  expect(() => open()).toThrow(expect.objectContaining({ code: "unsupported_storage_codecs" }));
  expect(await readFile(join(root, name))).toEqual(bytes);
  expect(await readdir(root)).not.toContain("jazz.sqlite");
  expect(await readdir(root)).not.toContain(".jazz-account-owner");
});

it("refuses a directory masquerading as a crash temporary file", async () => {
  const { root, open } = await fixture();
  await mkdir(join(root, TEMP_NAME));
  expect(() => open()).toThrow(expect.objectContaining({ code: "unsupported_storage_codecs" }));
  expect(await readdir(root)).not.toContain("jazz.sqlite");
});

it.skipIf(process.platform === "win32")(
  "refuses symlinks in the native storage layout",
  async () => {
    const { root, open } = await fixture();
    const target = await mkdtemp(join(tmpdir(), "jazz-native-root-target-"));
    onTestFinished(() => rm(target, { recursive: true, force: true }));
    const targetFile = join(target, "marker");
    await writeFile(targetFile, MARKER_V1);
    for (const name of [TEMP_NAME, ".jazz-account-owner", "jazz.sqlite"]) {
      await symlink(targetFile, join(root, name));
      expect(() => open()).toThrow(expect.objectContaining({ code: "unsupported_storage_codecs" }));
      await rm(join(root, name));
    }
    expect(await readFile(targetFile)).toEqual(MARKER_V1);
    expect(await readdir(root)).not.toContain("jazz.sqlite");
  },
);
