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

async function openEncrypted(store: PrivateStore, label: string) {
  const runtime = new EncryptionRuntime(
    new TextEncoder().encode(JSON.stringify(encryptedFile)),
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
  const db = NapiDb.openMemoryEncrypted(encodeSchema(encrypted.wasmSchema), config(label), runtime);
  const serving = servePrivateStore(db, store);
  return {
    db,
    encryption: new Encryption(db),
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
    expect(JSON.parse(first.db.encryptionState())).toMatchObject({ status: null, error: null });
    expect(first.db.takeEncryptionReports()).toEqual([]);
    await first.close();
    // The device's keys went through the store's protection and its documents.
    expect(calls).toEqual(expect.arrayContaining(["openScope", "load", "save", "protect"]));
    const again = await openEncrypted(store, "device-a");
    expect(await again.encryption.deviceId()).toBe(id);
    await again.close();
    // Another store is another device.
    const elsewhere = await openEncrypted(memoryPrivateStore(), "device-a");
    expect(await elsewhere.encryption.deviceId()).not.toBe(id);
    await elsewhere.close();
  });

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

  it("fails to build for an app file that declares no encryption, and a store that fails fails the build", async () => {
    const undeclared = new EncryptionRuntime(
      new TextEncoder().encode(
        JSON.stringify({ format: 1, tables: { notes: { columns: { text: "string" } } } }),
      ),
      "napi-encryption-test",
      "test",
      testAccountId("undeclared"),
    );
    await expect(undeclared.ready()).rejects.toThrow(/declare no encryption/);
    const broken = memoryPrivateStore();
    broken.openScope = async () => {
      throw new Error("the disk is gone");
    };
    const runtime = new EncryptionRuntime(
      new TextEncoder().encode(JSON.stringify(encryptedFile)),
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
    const runtime = new EncryptionRuntime(
      new TextEncoder().encode(JSON.stringify(encryptedFile)),
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
