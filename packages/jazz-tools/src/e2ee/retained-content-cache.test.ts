import { expect, it, vi } from "vitest";
import { schema as s } from "../schema-namespace.js";
import type { Db } from "../runtime/db.js";
import { deviceRequestSchema } from "./device-requests.js";
import { spaceSchema, type SpaceRoot } from "./spaces.js";
import { createBrowserCrypto } from "./browser.js";
import { DeviceKeyLifetime, type LocalDevice } from "./local-device.js";
import {
  RetainedContentCache,
  retainedContentRecordBytes,
  provisionalWriteRecordBytes,
} from "./retained-content-cache.js";
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
  const retain = async (
    revision?: Awaited<ReturnType<RetainedContentCache["revision"]>>,
    provisionalWrite = false,
  ) => {
    revision ??= await cache.revision();
    await cache.retainDevice();
    cache.rememberIdentities(root, "notes", "body", tableId, columnId);
    await cache.retainSpace("projects", root, secret, revision, provisionalWrite);
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
    reopen() {
      return new RetainedContentCache(
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

it("pins the distinct provisional-key v1 signing context without changing content v1", () => {
  expect(
    new TextDecoder().decode(
      provisionalWriteRecordBytes({
        format: "jazz-e2ee-provisional-key-v1",
        identifier: "scope",
      }),
    ),
  ).toBe(
    '["jazz-e2ee-provisional-key-v1",{"format":"jazz-e2ee-provisional-key-v1","identifier":"scope"}]',
  );
});

it("requires an online-retained selector and borrows exactly one selected key", async () => {
  const f = await setup();
  try {
    await f.retain();
    let calls = 0;
    const use = async (secret: Uint8Array) => {
      calls++;
      expect(secret).toEqual(f.secret);
    };
    expect(await f.cache.withProvisionalKey(f.app.projects, f.root.identifier, use, "warn")).toBe(
      "unavailable",
    );
    expect(calls).toBe(0);
    await f.retain(undefined, true);
    // Appending a historical record must not select it just because it is last.
    await f.cache.retainSpace(
      "projects",
      { ...f.root, epochId: crypto.randomUUID() },
      f.secret,
      await f.cache.revision(),
    );
    expect(
      await f.cache.withProvisionalKey(
        f.app.projects,
        f.root.identifier,
        async (secret, root) => {
          await use(secret);
          expect(root.epochId).toBe(f.root.epochId);
          expect(f.cache.identities(root, "notes", "body")).toEqual({
            table: f.tableId,
            column: f.columnId,
          });
        },
        "warn",
      ),
    ).toBe("used");
    expect(calls).toBe(1);
  } finally {
    f.close();
  }
});

it("warns or refuses known-stale provisional keys and refuses known revoked scopes", async () => {
  const f = await setup();
  const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
  try {
    await f.retain(undefined, true);
    await f.cache.markStale("projects", f.root.identifier);
    let calls = 0;
    const use = async () => {
      calls++;
    };
    expect(await f.cache.withProvisionalKey(f.app.projects, f.root.identifier, use, "reject")).toBe(
      "stale",
    );
    expect(calls).toBe(0);
    expect(warning).not.toHaveBeenCalled();
    expect(await f.cache.withProvisionalKey(f.app.projects, f.root.identifier, use, "warn")).toBe(
      "used",
    );
    expect(calls).toBe(1);
    expect(warning).toHaveBeenCalledWith(
      "E2EE: writing offline with a known-stale encryption epoch; removed recipients may still decrypt this write.",
    );
    await f.cache.denySpace("projects", f.root.identifier);
    expect(await f.cache.withProvisionalKey(f.app.projects, f.root.identifier, use, "warn")).toBe(
      "unavailable",
    );
    expect(calls).toBe(1);
  } finally {
    warning.mockRestore();
    f.close();
  }
});

it.each(["selector", "namespace", "account"] as const)(
  "refuses a mismatched provisional key (%s)",
  async (change) => {
    const f = await setup();
    try {
      await f.retain(undefined, true);
      let used = false;
      const use = async () => {
        used = true;
      };
      if (change === "selector") {
        await f.store.update((value) => {
          const state = JSON.parse(value!);
          state.retainedContentV1.provisionalWritesV1[0].signature[0] ^= 1;
          return JSON.stringify(state);
        });
        await expect(
          f.cache.withProvisionalKey(f.app.projects, f.root.identifier, use, "warn"),
        ).rejects.toThrow("Invalid provisional-key record");
      } else if (change === "namespace") {
        f.changeNamespace();
        expect(
          await f.cache.withProvisionalKey(f.app.projects, f.root.identifier, use, "warn"),
        ).toBe("unavailable");
      } else {
        expect(
          await f
            .otherApplication()
            .withProvisionalKey(f.app.projects, f.root.identifier, use, "warn"),
        ).toBe("unavailable");
      }
      expect(used).toBe(false);
    } finally {
      f.close();
    }
  },
);

it("keeps optional retention failures owner-local without promising durable reopen", async () => {
  const f = await setup();
  try {
    f.store.update = async (change) => {
      change(await f.store.read());
      throw new Error("Cache persistence unavailable");
    };
    await f.retain(undefined, true);
    let calls = 0;
    expect(
      await f.cache.withProvisionalKey(
        f.app.projects,
        f.root.identifier,
        async () => {
          calls++;
        },
        "warn",
      ),
    ).toBe("used");
    expect(calls).toBe(1);
    expect(await f.reopen().available()).toBe(false);
  } finally {
    f.close();
  }
});

it.each(["device", "scope"] as const)(
  "revokes an in-flight provisional borrow even if %s denial cannot persist",
  async (target) => {
    const f = await setup();
    let borrowed: Uint8Array | undefined;
    try {
      await f.retain(undefined, true);
      f.store.update = async () => {
        throw new Error("Denial persistence unavailable");
      };
      await expect(
        f.cache.withProvisionalKey(
          f.app.projects,
          f.root.identifier,
          async (secret) => {
            borrowed = secret;
            await expect(
              target === "device"
                ? f.cache.denyDevice()
                : f.cache.denySpace("projects", f.root.identifier),
            ).rejects.toThrow("Denial persistence unavailable");
          },
          "warn",
        ),
      ).rejects.toThrow("invalidated during use");
      expect(borrowed).toEqual(new Uint8Array(32));
    } finally {
      f.close();
    }
  },
);

it("refuses a reject-policy provisional borrow if newly known staleness cannot persist", async () => {
  const f = await setup();
  try {
    await f.retain(undefined, true);
    f.store.update = async () => {
      throw new Error("Stale-state persistence unavailable");
    };
    await expect(
      f.cache.withProvisionalKey(
        f.app.projects,
        f.root.identifier,
        async () => {
          await expect(f.cache.markStale("projects", f.root.identifier)).rejects.toThrow(
            "Stale-state persistence unavailable",
          );
        },
        "reject",
      ),
    ).rejects.toThrow("known-stale during use");
  } finally {
    f.close();
  }
});

it("exposes retained structural identities only inside the verified owner borrow", async () => {
  const f = await setup();
  let borrowedRoot: Readonly<typeof f.root> | undefined;
  try {
    await f.retain();
    expect(f.cache.isBorrowing(f.root)).toBe(false);
    await expect(
      f.cache.withKeys(f.app.projects, f.root.identifier, async (_secret, root) => {
        borrowedRoot = root;
        expect(f.cache.isBorrowing(root)).toBe(true);
        expect(f.cache.identities(root, "notes", "body")).toEqual({
          table: f.tableId,
          column: f.columnId,
        });
      }),
    ).resolves.toBe(true);
    expect(f.cache.isBorrowing(borrowedRoot!)).toBe(false);
    expect(() => f.cache.identities(borrowedRoot!, "notes", "body")).toThrow(/catalogue binding/);
  } finally {
    f.close();
  }
});

it.each([
  { invalidation: "denial", persistence: "durable" },
  { invalidation: "denial", persistence: "volatile" },
  { invalidation: "stale", persistence: "durable" },
  { invalidation: "stale", persistence: "volatile" },
] as const)(
  "fences delayed $persistence retention after failed $invalidation persistence until fresh validation",
  async ({ invalidation, persistence }) => {
    const f = await setup();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let entered!: () => void;
    const waiting = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const originalUpdate = f.store.update.bind(f.store);
    let delayed: Promise<void> | undefined;
    try {
      await f.retain(undefined, true);
      // This receipt precedes the older result's already-completed validation.
      const before = await f.cache.revision();
      let updates = 0;
      const update = vi.spyOn(f.store, "update").mockImplementation(async (change) => {
        if (++updates === 1) {
          entered();
          await gate;
          if (persistence === "durable") return originalUpdate(change);
          change(await f.store.read());
          throw new Error("optional retention persistence unavailable");
        }
        // Transform ran, but the backing store did not publish the denial.
        change(await f.store.read());
        throw new Error("denial persistence unavailable");
      });
      delayed = f.cache.retainSpace("projects", f.root, f.secret, before, true);
      await waiting;
      await expect(
        invalidation === "denial"
          ? f.cache.denySpace("projects", f.root.identifier)
          : f.cache.markStale("projects", f.root.identifier),
      ).rejects.toThrow("denial persistence unavailable");
      release();
      await delayed;
      const use = vi.fn(async () => {});
      if (invalidation === "denial") {
        expect(await f.cache.withKeys(f.app.projects, f.root.identifier, use)).toBe(false);
        expect(
          await f.cache.withProvisionalKey(f.app.projects, f.root.identifier, use, "reject"),
        ).toBe("unavailable");
      } else {
        expect(f.cache.isKnownStale("projects", f.root.identifier)).toBe(true);
        expect(
          await f.cache.withProvisionalKey(f.app.projects, f.root.identifier, use, "reject"),
        ).toBe("stale");
      }
      expect(use).not.toHaveBeenCalled();
      update.mockRestore();
      // Only a fresh validation receipt may restore this same-owner binding.
      // This fixture models that private boundary; it is not authority proof.
      await f.retain(undefined, true);
      expect(await f.cache.withKeys(f.app.projects, f.root.identifier, use)).toBe(true);
      expect(
        await f.cache.withProvisionalKey(f.app.projects, f.root.identifier, use, "reject"),
      ).toBe("used");
      expect(use).toHaveBeenCalledTimes(2);
    } finally {
      release();
      await delayed?.catch(() => {});
      vi.restoreAllMocks();
      f.close();
    }
  },
);
