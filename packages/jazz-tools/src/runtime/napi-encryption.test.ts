import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { queryObjects } from "node:v8";
import { compiledBindings } from "../encryption/build.js";
import { EncryptionRuntime, NapiDb } from "jazz-napi";
import { describe, expect, it } from "vitest";
import {
  Encryption,
  EncryptionNotDeclaredError,
  memoryPrivateStore,
  notDeclaredEncryption,
  servePrivateStore,
  type PrivateStore,
} from "../encryption/index.js";
import { encryptionStoreScope } from "../encryption/store-scope.js";
import { RecoveryPhrase } from "../local-first-auth/recovery-phrase.js";
import { testApp } from "../testing/test-app.js";
import { openConfig } from "./native-runtime/native-codec.js";
import { encodeSchema } from "./native-runtime/schema-codec.js";
import { testAccountId, testAuthorBytes } from "./testing/account-fixtures.js";

// The Node binding installs the end-to-end encryption runtime on the database
// of an app that declares encryption, over the private store the app
// supplies, and leaves a plain app alone.

const encryptedFile = {
  format: 1,
  tables: {
    projects: { columns: { name: "string", ownerId: "uuid" } },
    tasks: {
      columns: { projectId: "uuid", title: "bytes" },
      relations: { project: { rel: "projects", column: "projectId" } },
      encryption: { space: "projectId", scope: "projects", columns: { title: "string" } },
    },
  },
};
const encrypted = testApp(encryptedFile);
const plain = testApp({ tables: { notes: { columns: { text: "string" } } } });

function config(label: string): Uint8Array {
  return openConfig(new Uint8Array(16).fill(41), testAuthorBytes(label), 1, true);
}

/** A store that counts what the runtime asks of it. */
function counting(store: PrivateStore) {
  const calls: string[] = [];
  const wrapped = Object.fromEntries(
    (Object.keys(store) as (keyof PrivateStore)[]).map((name) => [
      name,
      (...args: unknown[]) => {
        calls.push(name);
        return (store[name] as (...a: unknown[]) => unknown)(...args);
      },
    ]),
  ) as unknown as PrivateStore;
  return { store: wrapped, calls };
}

async function openEncrypted(store: PrivateStore, label: string, path?: string) {
  const runtime = EncryptionRuntime.fromBindings(
    compiledBindings(encrypted),
    "napi-encryption-test",
    "test",
    testAccountId(label),
  );
  const building = servePrivateStore(runtime, store);
  try {
    await runtime.ready();
  } finally {
    building.stop();
  }
  const schema = encodeSchema(encrypted.wasmSchema);
  const db = path
    ? NapiDb.openPersistentEncrypted(path, schema, config(label), runtime)
    : NapiDb.openMemoryEncrypted(schema, config(label), runtime);
  const serving = servePrivateStore(runtime, store);
  return {
    db,
    encryption: new Encryption(db),
    stopServing: serving.stop,
    close: async () => {
      serving.stop();
      await db.close();
    },
  };
}

describe("encryption on the Node binding", () => {
  it("builds the runtime over the app's store, which keeps the device for the next open", async () => {
    const { store, calls } = counting(memoryPrivateStore());
    const first = await openEncrypted(store, "device-a");
    const id = await first.encryption.deviceId();
    expect(typeof id).toBe("string");
    expect(JSON.parse(first.db.encryptionState())).toEqual({
      status: null,
      readable: expect.any(Boolean),
      error: null,
      stage: expect.any(String),
    });
    expect(first.db.takeEncryptionReports()).toEqual([]);
    await first.close();
    // The device's keys went through the store's protection and its documents.
    expect(calls).toEqual(expect.arrayContaining(["openScope", "load", "save", "protect"]));
    const again = await openEncrypted(store, "device-a");
    expect(await again.encryption.deviceId()).toBe(id);
    await again.close();
    // The scope the runtime keeps the device in is the one a sign-out wipes
    // (`encryptionStoreScope`): wiping it makes the next open a new device.
    const scope = encryptionStoreScope("napi-encryption-test", "test", testAccountId("device-a"));
    expect(await store.list(scope)).toContain("device");
    await store.delete(scope);
    const wiped = await openEncrypted(store, "device-a");
    expect(await wiped.encryption.deviceId()).not.toBe(id);
    await wiped.close();
    // Another store is another device.
    const elsewhere = await openEncrypted(memoryPrivateStore(), "device-a");
    expect(await elsewhere.encryption.deviceId()).not.toBe(id);
    await elsewhere.close();
  });

  it.each(["close", "drop"])(
    "releases the encrypted owner's persistent store on %s",
    async (end) => {
      const path = await mkdtemp(`${tmpdir()}/encrypted-owner-`);
      try {
        const openAndRelease = async () => {
          const owner = await openEncrypted(memoryPrivateStore(), "alice", path);
          owner.stopServing();
          expect(await readdir(path)).toContain("jazz.sqlite-wal");
          if (end === "close") await owner.close();
        };
        await openAndRelease();
        // SQLite removes the WAL when the last connection to the store closes.
        if (end === "drop") {
          await expect
            .poll(async () => {
              // queryObjects runs a full GC. Give N-API a turn to finalise the wrapper.
              queryObjects(NapiDb);
              await new Promise<void>((resolve) => setImmediate(resolve));
              return readdir(path);
            })
            .not.toContain("jazz.sqlite-wal");
        } else {
          expect(await readdir(path)).not.toContain("jazz.sqlite-wal");
        }
      } finally {
        await rm(path, { force: true, recursive: true });
      }
    },
  );

  it("refuses an encrypted app opened as a plain database, and leaves a plain app alone", async () => {
    expect(() =>
      NapiDb.openMemory(encodeSchema(encrypted.wasmSchema), config("plain-open")),
    ).toThrow(/declares end-to-end encryption/);
    const db = NapiDb.openMemory(encodeSchema(plain.wasmSchema), config("plain"));
    try {
      expect(() => db.encryptionState()).toThrow(/no encryption runtime/);
    } finally {
      await db.close();
    }
  });

  it("fails to build for bindings that declare no encryption, and a store that fails fails the build", async () => {
    const undeclared = EncryptionRuntime.fromBindings(
      compiledBindings(plain),
      "napi-encryption-test",
      "test",
      testAccountId("undeclared"),
    );
    await expect(undeclared.ready()).rejects.toThrow(/declare no encryption/);
    const broken = memoryPrivateStore();
    broken.openScope = async () => {
      throw new Error("the disk is gone");
    };
    const runtime = EncryptionRuntime.fromBindings(
      compiledBindings(encrypted),
      "napi-encryption-test",
      "test",
      testAccountId("broken"),
    );
    const serving = servePrivateStore(runtime, broken);
    try {
      await expect(runtime.ready()).rejects.toThrow(/the disk is gone/);
    } finally {
      serving.stop();
    }
  });

  it("refuses a runtime built for another account than the session's", async () => {
    const runtime = EncryptionRuntime.fromBindings(
      compiledBindings(encrypted),
      "napi-encryption-test",
      "test",
      testAccountId("someone-else"),
    );
    const serving = servePrivateStore(runtime, memoryPrivateStore());
    try {
      await runtime.ready();
    } finally {
      serving.stop();
    }
    expect(() =>
      NapiDb.openMemoryEncrypted(encodeSchema(encrypted.wasmSchema), config("device-b"), runtime),
    ).toThrow(/another account/);
  });

  it("answers for an app without encryption with an error that says so", () => {
    expect(() => notDeclaredEncryption().devices).toThrow(EncryptionNotDeclaredError);
  });

  it("gives a recovery phrase of 24 words for 32 bytes and back, as local-first accounts do", () => {
    const bytes = new Uint8Array(32).map((_, index) => index);
    const phrase = RecoveryPhrase.fromBytes(bytes);
    expect(phrase.split(" ")).toHaveLength(24);
    expect(RecoveryPhrase.toBytes(phrase)).toEqual(bytes);
    expect(() => RecoveryPhrase.toBytes("abandon abandon")).toThrow(/Expected 24 words/);
  });
});
