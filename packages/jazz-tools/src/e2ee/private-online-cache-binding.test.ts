import { expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import type { Db } from "../runtime/db.js";
import { setDbInternalSession } from "../runtime/db-internal-session.js";
import { deviceRequestSchema } from "./device-requests.js";
import { spaceSchema, type SpaceRoot } from "./spaces.js";
import { createBrowserCrypto } from "./browser.js";
import { DeviceKeyLifetime, type LocalDevice } from "./local-device.js";
import { RetainedContentCache, retainedContentRecordBytes } from "./retained-content-cache.js";
import formatFixture from "./fixtures/private-online-cache-anchor-v1.json";

// Unit boundary fixtures exercise PRIVATE binding checks, not core admission.
// The original fresh-owner test separately executes the real accepted route.
const boundary = vi.hoisted(() => ({
  read: undefined as ((query: { _table: string }) => unknown) | undefined,
  wait: undefined as (() => void) | undefined,
}));
vi.mock("./accepted-membership.js", async (original) => ({
  ...(await original<typeof import("./accepted-membership.js")>()),
  acceptedDeviceTransaction: async (
    _db: unknown,
    callback: (tx: {
      readAccepted: (query: { _table: string }) => Promise<unknown>;
    }) => Promise<unknown>,
  ) => {
    const result = await callback({ readAccepted: async (query) => boundary.read!(query) });
    return {
      wait: async () => {
        boundary.wait?.();
        return result;
      },
    };
  },
}));
async function setup() {
  const app = s.defineApp(
    { ...deviceRequestSchema, ...spaceSchema, projects: s.table({ title: s.string() }, {}) },
    { migrations: "none" },
  );
  const crypto = await createBrowserCrypto();
  const encryption = await crypto.keyEnvelope.createKeyPair();
  const signing = await crypto.deviceSigner.createKeyPair();
  const lifetime = new DeviceKeyLifetime();
  const deviceId = globalThis.crypto.randomUUID(),
    accountId = globalThis.crypto.randomUUID();
  const root: SpaceRoot = {
    id: globalThis.crypto.randomUUID(),
    scopeId: globalThis.crypto.randomUUID(),
    identifier: globalThis.crypto.randomUUID(),
    epochId: globalThis.crypto.randomUUID(),
    creatorAccountId: accountId,
    deviceId,
    accountEpochId: globalThis.crypto.randomUUID(),
    initialGrantId: globalThis.crypto.randomUUID(),
    mechanism: crypto.keyEnvelope.mechanism.id,
    version: crypto.keyEnvelope.mechanism.version,
    verification: Uint8Array.of(1),
    authorEnvelope: Uint8Array.of(2),
    signature: Uint8Array.of(3),
  };
  const keys = {
    id: globalThis.crypto.randomUUID(),
    deviceId,
    publicKey: encryption.publicKey,
    signingPublicKey: signing.publicKey,
    mechanism: crypto.keyEnvelope.mechanism.id,
    version: crypto.keyEnvelope.mechanism.version,
    signingMechanism: crypto.deviceSigner.mechanism.id,
    signingVersion: crypto.deviceSigner.mechanism.version,
  };
  let cAdmitted = false;
  let cDeviceOnly = false;
  let closed = false,
    retired = false,
    saved: string | null = null;
  const assertOpen = () => {
    if (closed) throw new Error("closed private owner");
  };
  const device = {
    id: deviceId,
    async load() {
      assertOpen();
      return {
        id: deviceId,
        publicKey: encryption.publicKey,
        privateKey: lifetime.own(encryption.privateKey.slice()),
        challenge: new Uint8Array(32),
        signing: {
          publicKey: signing.publicKey,
          privateKey: lifetime.own(signing.privateKey.slice()),
        },
      };
    },
    release(loaded: LocalDevice) {
      lifetime.release(loaded.privateKey);
      lifetime.release(loaded.signing.privateKey);
    },
  };
  const ids = [globalThis.crypto.randomUUID(), globalThis.crypto.randomUUID()];
  const rowFor = (query: { _table: string }) =>
    query._table === "__e2ee_device_keys" ? keys : root;
  const db = {
    e2eeHasConnectedTransportForKeys: () => true,
    tableIdentity: async (query: { _table: string }) =>
      ids[query._table === "__e2ee_device_keys" ? 0 : 1]!,
    all: async (query: { _table: string }) => {
      assertOpen();
      return [structuredClone(rowFor(query))];
    },
    observeE2eeHistory: async (queries: { _table: string }[]) => {
      if (retired) throw Object.assign(new Error("retired native owner"), { code: "storage" });
      return cAdmitted
        ? queries.map((query) => {
            if (cDeviceOnly && query._table !== "__e2ee_device_keys")
              return { rows: [], through: "9", settlements: [], provenance: [] };
            const row = structuredClone(rowFor(query));
            return {
              rows: [row],
              through: "9",
              settlements: [
                {
                  rowId: row.id,
                  state: { transactionId: "state-id", position: "7" },
                  content: { transactionId: "content-id", position: "6" },
                },
              ],
              provenance: ["authority-read"],
            };
          })
        : [];
    },
  } as unknown as Db;
  const session = {
    account_id: accountId,
    issuer: "https://fixture.invalid",
    user_id: "actual-owner",
    claims: { role: "reader" },
    authMode: "external" as const,
  };
  setDbInternalSession(db, session);
  const schema = globalThis.crypto.randomUUID();
  boundary.wait = undefined;
  boundary.read = (query) => {
    const row = structuredClone(rowFor(query));
    return {
      rows: [row],
      schema,
      through: "9",
      evidence: [
        {
          rowId: row.id,
          state: { transactionId: "state-id", position: "7" },
          content: { transactionId: "content-id", position: "6" },
        },
      ],
    };
  };
  const store = {
    async read() {
      return saved;
    },
    async update(change: (value: string | null) => string) {
      saved = change(saved);
    },
  };
  const scope = JSON.stringify(["fixture-registry", "fixture-app", accountId]);
  const makeCache = () =>
    new RetainedContentCache(
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
  const cache = makeCache();
  const secret = new Uint8Array(32).fill(42);
  const retain = async () => {
    await cache.captureOnlineBinding(root);
    const revision = await cache.revision();
    await cache.retainDevice();
    await cache.retainSpace("projects", root, secret, revision, true);
  };
  return {
    app,
    cache,
    root,
    keys,
    secret,
    db,
    session,
    ids,
    retain,
    makeCache,
    admitC() {
      cAdmitted = true;
      cDeviceOnly = false;
    },
    admitCDeviceOnly() {
      cAdmitted = true;
      cDeviceOnly = true;
    },
    tamper(kind: "origin" | "envelope") {
      const value = JSON.parse(saved!);
      const record = value.retainedContentV1.content[0];
      if (kind === "origin") record.anchor.origin.content.transactionId = "changed-origin";
      else record.envelope[0] ^= 1;
      saved = JSON.stringify(value);
    },
    retire() {
      retired = true;
    },
    close() {
      closed = true;
      lifetime.close();
    },
  };
}
it("pins the versioned private anchor bytes without changing existing C record encodings", () => {
  expect(Buffer.from(retainedContentRecordBytes(formatFixture.record)).toString("hex")).toBe(
    formatFixture.hex,
  );
});
it("borrows only this owner's accepted-bound content/provisional key; a new owner cannot inherit its private proof", async () => {
  const f = await setup();
  try {
    await f.retain();
    expect(await f.cache.available()).toBe(true);
    expect(
      await f.cache.withProvisionalKey(
        f.app.projects,
        f.root.identifier,
        async (key) => {
          expect(key).toEqual(f.secret);
        },
        "warn",
      ),
    ).toBe("used");
    expect(await f.makeCache().available()).toBe(false);
  } finally {
    f.close();
  }
});
it.each(["account", "claims"])("refuses a changed actual %s binding", async (kind) => {
  const f = await setup();
  try {
    await f.retain();
    setDbInternalSession(
      f.db,
      kind === "account"
        ? { ...f.session, account_id: globalThis.crypto.randomUUID() }
        : { ...f.session, claims: { role: "other" } },
    );
    await expect(f.cache.available()).rejects.toThrow(
      /actual authenticated account|binding changed/,
    );
  } finally {
    f.close();
  }
});
it.each(["device", "scope", "paired-origin", "same-cut"])(
  "refuses an invalid online %s snapshot",
  async (kind) => {
    const f = await setup();
    try {
      const read = boundary.read!;
      boundary.read = (query) => {
        const snapshot = read(query) as {
          rows: Array<Record<string, unknown>>;
          through: string;
          evidence: Array<{ content: { position: string } }>;
        };
        if (query._table === "__e2ee_device_keys" && kind === "device")
          snapshot.rows[0]!.deviceId = globalThis.crypto.randomUUID();
        if (query._table === "__e2ee_spaces" && kind === "scope")
          snapshot.rows[0]!.identifier = globalThis.crypto.randomUUID();
        if (kind === "paired-origin") snapshot.evidence[0]!.content.position = "10";
        if (query._table === "__e2ee_spaces" && kind === "same-cut") snapshot.through = "10";
        return snapshot;
      };
      await expect(f.cache.captureOnlineBinding(f.root)).rejects.toThrow();
      expect(await f.cache.available()).toBe(false);
    } finally {
      f.close();
    }
  },
);
it("refuses exact-body tampering, native retirement and closed-owner use", async () => {
  const f = await setup();
  try {
    await f.retain();
    f.root.verification[0] = 9;
    expect(
      await f.cache.withKeys(f.app.projects, f.root.identifier, async () => {
        throw new Error("must not use changed content");
      }),
    ).toBe(false);
    f.retire();
    await expect(f.cache.available()).rejects.toThrow("retired native owner");
    f.close();
    await expect(f.cache.available()).rejects.toThrow("closed private owner");
  } finally {
    f.close();
  }
});
it("does not publish a private anchor if the genuine accepted wait refuses or the owner closes", async () => {
  const f = await setup();
  try {
    boundary.wait = () => {
      throw new Error("authority refused");
    };
    await expect(f.cache.captureOnlineBinding(f.root)).rejects.toThrow("authority refused");
    expect(await f.cache.available()).toBe(false);
    boundary.wait = () => f.close();
    await expect(f.cache.captureOnlineBinding(f.root)).rejects.toThrow("closed private owner");
  } finally {
    f.close();
  }
});

it.each(["origin", "envelope"] as const)("rejects signed-record %s tampering", async (kind) => {
  const f = await setup();
  try {
    await f.retain();
    f.tamper(kind);
    await expect(
      f.cache.withKeys(f.app.projects, f.root.identifier, async () => {}),
    ).rejects.toThrow("Invalid private retained-content record");
  } finally {
    f.close();
  }
});
it("refuses another scope and native retirement during an awaited key use", async () => {
  const f = await setup();
  let borrowed: Uint8Array | undefined;
  try {
    await f.retain();
    expect(
      await f.cache.withKeys(f.app.projects, crypto.randomUUID(), async () => {
        throw new Error("wrong scope must not receive a key");
      }),
    ).toBe(false);
    await expect(
      f.cache.withKeys(f.app.projects, f.root.identifier, async (key) => {
        borrowed = key;
        await Promise.resolve();
        f.retire();
      }),
    ).rejects.toThrow("retired native owner");
    expect(borrowed).toEqual(new Uint8Array(32));
  } finally {
    f.close();
  }
});

it("preserves existing C-backed records across reopen without manufacturing private proof", async () => {
  const f = await setup();
  try {
    f.admitC();
    boundary.read = () => {
      throw new Error("private fallback must not replace C");
    };
    await f.retain();
    expect(await f.makeCache().available()).toBe(true);
    expect(
      await f.makeCache().withProvisionalKey(
        f.app.projects,
        f.root.identifier,
        async (key) => {
          expect(key).toEqual(f.secret);
        },
        "warn",
      ),
    ).toBe("used");
  } finally {
    f.close();
  }
});
it("keeps an existing private record usable when C evidence arrives, without treating it as C", async () => {
  const f = await setup();
  try {
    await f.retain();
    f.admitC();
    expect(await f.cache.available()).toBe(true);
    expect(
      await f.cache.withProvisionalKey(
        f.app.projects,
        f.root.identifier,
        async (key) => {
          expect(key).toEqual(f.secret);
        },
        "warn",
      ),
    ).toBe("used");
    expect(await f.makeCache().available()).toBe(false);
  } finally {
    f.close();
  }
});

it("requires a new genuine accepted capture before refreshing a changed-claims binding", async () => {
  const f = await setup();
  try {
    await f.retain();
    setDbInternalSession(f.db, { ...f.session, claims: { role: "new-policy-reader" } });
    await expect(f.cache.available()).rejects.toThrow("binding changed");
    // This mock represents ONLY the accepted boundary. Production checks
    // real policy-bound reads and Global again before replacing the proof.
    await f.retain();
    expect(await f.cache.available()).toBe(true);
  } finally {
    f.close();
  }
});

it("keeps a C-bound device and private same-cut root separate without granting a new owner that root", async () => {
  const f = await setup();
  try {
    f.admitCDeviceOnly();
    await f.retain();
    expect(
      await f.cache.withProvisionalKey(
        f.app.projects,
        f.root.identifier,
        async (key) => {
          expect(key).toEqual(f.secret);
        },
        "warn",
      ),
    ).toBe("used");
    const newOwner = f.makeCache();
    expect(await newOwner.available()).toBe(true);
    expect(
      await newOwner.withKeys(f.app.projects, f.root.identifier, async () => {
        throw new Error("private root must not cross owners");
      }),
    ).toBe(false);
  } finally {
    f.close();
  }
});
