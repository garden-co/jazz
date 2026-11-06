import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import type { Db } from "../runtime/db.js";
import { deviceRequestSchema } from "./device-requests.js";
import { spaceSchema, type SpaceRoot } from "./spaces.js";
import { createBrowserCrypto } from "./browser.js";
import { DeviceKeyLifetime, type LocalDevice } from "./local-device.js";
import { RetainedContentCache, retainedContentRecordBytes } from "./retained-content-cache.js";
import fixture from "./fixtures/retained-content-context-v1.json";

// Positive observation fixtures model ONLY the private cache boundary. They do
// not qualify SQLite admission, complete history, membership or SELECT proof.
// Actual key envelopes and signatures use the real crypto adapter.
async function setup() {
  const app = s.defineApp(
    { ...deviceRequestSchema, ...spaceSchema, projects: s.table({ title: s.string() }, {}) },
    { migrations: "none" },
  );
  const crypto = await createBrowserCrypto();
  const encryption = await crypto.keyEnvelope.createKeyPair();
  const signing = await crypto.deviceSigner.createKeyPair();
  const id = globalThis.crypto.randomUUID();
  const scope = "private-test-application/alice";
  const lifetime = new DeviceKeyLifetime();
  let closed = false;
  const assertOpen = () => {
    if (closed) throw new Error("closed test owner");
  };
  const device = {
    id,
    async load() {
      assertOpen();
      return {
        id,
        publicKey: encryption.publicKey,
        privateKey: lifetime.own(encryption.privateKey.slice()),
        challenge: new Uint8Array(32),
        signing: {
          publicKey: signing.publicKey,
          privateKey: lifetime.own(signing.privateKey.slice()),
        },
      };
    },
    release(value: LocalDevice) {
      lifetime.release(value.privateKey);
      lifetime.release(value.signing.privateKey);
    },
  };
  const root: SpaceRoot = {
    id: globalThis.crypto.randomUUID(),
    scopeId: globalThis.crypto.randomUUID(),
    identifier: globalThis.crypto.randomUUID(),
    epochId: globalThis.crypto.randomUUID(),
    creatorAccountId: globalThis.crypto.randomUUID(),
    deviceId: id,
    accountEpochId: globalThis.crypto.randomUUID(),
    initialGrantId: globalThis.crypto.randomUUID(),
    mechanism: crypto.keyEnvelope.mechanism.id,
    version: crypto.keyEnvelope.mechanism.version,
    verification: Uint8Array.of(1),
    authorEnvelope: Uint8Array.of(2),
    signature: Uint8Array.of(3),
  };
  const publicKeyRow = {
    id: globalThis.crypto.randomUUID(),
    deviceId: id,
    publicKey: encryption.publicKey,
    signingPublicKey: signing.publicKey,
    mechanism: crypto.keyEnvelope.mechanism.id,
    version: crypto.keyEnvelope.mechanism.version,
    signingMechanism: crypto.deviceSigner.mechanism.id,
    signingVersion: crypto.deviceSigner.mechanism.version,
  };
  let missingRoot = false;
  let missingNamespace = false;
  let observationFailure: unknown;
  let originNamespace = "test-namespace-A";
  let provenance = "authority-read";
  const db = {
    async observeE2eeHistory(queries: { _table: string }[]) {
      if (observationFailure) throw observationFailure;
      if (missingNamespace)
        throw Object.assign(new Error("No admitted namespace"), { code: "not_observed" });
      return queries.map((query) => {
        const row =
          query._table === "__e2ee_device_keys" ? publicKeyRow : missingRoot ? undefined : root;
        return {
          rows: row ? [row] : [],
          through: "10",
          settlements: row
            ? [
                {
                  rowId: row.id,
                  state: { transactionId: originNamespace, position: "7" },
                  content: { transactionId: originNamespace, position: "7" },
                },
              ]
            : [],
          provenance: row ? [provenance] : [],
        };
      });
    },
  } as unknown as Db;
  let saved: string | null = null;
  const store = {
    async read() {
      return saved;
    },
    async update(change: (value: string | null) => string) {
      saved = change(saved);
    },
  };
  const cache = new RetainedContentCache(
    db,
    store,
    scope,
    app.wasmSchema,
    app,
    device,
    lifetime,
    crypto.keyEnvelope,
    crypto.deviceSigner,
    assertOpen,
  );
  const secret = new Uint8Array(32).fill(42);
  const tableId = globalThis.crypto.randomUUID(),
    columnId = globalThis.crypto.randomUUID();
  const retain = async (revision?: number) => {
    revision ??= await cache.revision();
    await cache.retainDevice();
    cache.rememberIdentities(root, "notes", "body", tableId, columnId);
    await cache.retainSpace("projects", root, secret, revision);
  };
  return {
    app,
    cache,
    root,
    secret,
    retain,
    tableId,
    columnId,
    store,
    removeRoot() {
      missingRoot = true;
    },
    changeNamespace() {
      originNamespace = "test-namespace-B";
    },
    reclassifyPositiveEvidence() {
      provenance = "local-submission";
    },
    removeNamespace() {
      missingNamespace = true;
    },
    failObservations(error: unknown) {
      observationFailure = error;
    },
    otherApplication() {
      return new RetainedContentCache(
        db,
        store,
        `${scope}/other-account`,
        app.wasmSchema,
        app,
        device,
        lifetime,
        crypto.keyEnvelope,
        crypto.deviceSigner,
        assertOpen,
      );
    },
    corruptSignature() {
      const value = JSON.parse(saved!);
      value.retainedContentV1.content[0].signature[0] ^= 1;
      saved = JSON.stringify(value);
    },
    close() {
      closed = true;
      lifetime.close();
      encryption.privateKey.fill(0);
      signing.privateKey.fill(0);
      secret.fill(0);
    },
  };
}

it("pins the versioned private context bytes independently of object property order", () => {
  const encoded = retainedContentRecordBytes(fixture.record);
  expect(Array.from(encoded, (byte) => byte.toString(16).padStart(2, "0")).join("")).toBe(
    fixture.hex,
  );
  expect(
    retainedContentRecordBytes({ binding: fixture.record.binding, format: fixture.record.format }),
  ).toEqual(encoded);
  expect(new TextDecoder().decode(retainedContentRecordBytes({ z: 0, A: 1, a: 2 }))).toBe(
    '["jazz-e2ee-retained-content-v1",{"A":1,"a":2,"z":0}]',
  );
});

it("reopens only a signed device-bound key and supplies its exact content identities", async () => {
  const f = await setup();
  try {
    expect(await f.cache.available()).toBe(false);
    await f.retain();
    expect(await f.cache.available()).toBe(true);
    f.reclassifyPositiveEvidence();
    expect(await f.cache.available()).toBe(true);
    let borrowed: Uint8Array | undefined;
    expect(
      await f.cache.withKeys(f.app.projects, f.root.identifier, async (secret, root) => {
        borrowed = secret;
        expect(secret).toEqual(f.secret);
        expect(root.epochId).toBe(f.root.epochId);
        expect(f.cache.identities(root, "notes", "body")).toEqual({
          table: f.tableId,
          column: f.columnId,
        });
      }),
    ).toBe(true);
    expect(borrowed).toEqual(new Uint8Array(32));
    // No positive root means unknown, even with the same valid private key.
    f.removeRoot();
    expect(
      await f.cache.withKeys(f.app.projects, f.root.identifier, async () => {
        throw new Error("Missing root must not supply a key");
      }),
    ).toBe(false);
  } finally {
    f.close();
  }
});

it("rejects changed store origins, schema bindings and tampered device signatures", async () => {
  const f = await setup();
  try {
    await f.retain();
    const other = s.defineApp(
      { projects: s.table({ different: s.string() }, {}) },
      { migrations: "none" },
    );
    expect(
      await f.cache.withKeys(other.projects, f.root.identifier, async () => {
        throw new Error("Foreign schema must not supply a key");
      }),
    ).toBe(false);
    f.corruptSignature();
    await expect(
      f.cache.withKeys(f.app.projects, f.root.identifier, async () => {}),
    ).rejects.toThrow("Invalid private retained-content record");
    // A copied private record cannot establish its binding in a different store.
    f.changeNamespace();
    expect(await f.cache.available()).toBe(false);
  } finally {
    f.close();
  }
});

it("does not resurrect a known denial with a late older online result", async () => {
  const f = await setup();
  try {
    await f.retain();
    const before = await f.cache.revision();
    await f.cache.denySpace("projects", f.root.identifier);
    await f.retain(before);
    expect(
      await f.cache.withKeys(f.app.projects, f.root.identifier, async () => {
        throw new Error("Stale result must not resurrect a key");
      }),
    ).toBe(false);
    await f.retain();
    expect(await f.cache.withKeys(f.app.projects, f.root.identifier, async () => {})).toBe(true);
    await f.cache.denyDevice();
    await f.retain();
    expect(await f.cache.available()).toBe(false);
  } finally {
    f.close();
  }
});

it("zeros an in-flight borrowed key and rejects completion when its owner closes", async () => {
  const f = await setup();
  try {
    await f.retain();
    await expect(
      f.cache.withKeys(f.app.projects, f.root.identifier, async (secret) => {
        f.close();
        expect(secret).toEqual(new Uint8Array(32));
      }),
    ).rejects.toThrow("closed test owner");
  } finally {
    f.close();
  }
});

it("treats a fresh store without an admitted namespace as missing positive evidence", async () => {
  const f = await setup();
  try {
    await f.retain();
    f.removeNamespace();
    expect(await f.cache.available()).toBe(false);
    expect(
      await f.cache.withKeys(f.app.projects, f.root.identifier, async () => {
        throw new Error("An unbound store must not supply a retained key");
      }),
    ).toBe(false);
  } finally {
    f.close();
  }
});

it("does not transfer private content across account/application bindings", async () => {
  const f = await setup();
  try {
    await f.retain();
    const other = f.otherApplication();
    expect(await other.available()).toBe(false);
    expect(
      await other.withKeys(f.app.projects, f.root.identifier, async () => {
        throw new Error("A foreign account must not borrow this key");
      }),
    ).toBe(false);
  } finally {
    f.close();
  }
});

it.each(["storage", "protocol"])(
  "does not suppress a %s failure as missing cache evidence",
  async (code) => {
    const f = await setup();
    try {
      await f.retain();
      const failure = Object.assign(new Error("Terminal observation failure"), { code });
      f.failObservations(failure);
      await expect(f.cache.available()).rejects.toBe(failure);
    } finally {
      f.close();
    }
  },
);
