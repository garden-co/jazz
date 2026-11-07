import { describe, expect, it, vi } from "vitest";
import { encodeSchema, NativeRuntimeAdapter } from "./native-runtime-adapter.js";

const query = JSON.stringify({ table: "__e2ee_roots" });
const empty = (through = "1") => ({ rows: [0], through, evidence: [], provenance: [] });

// Explicit binding mocks: these test host shape/lifetime checks, not SQLite,
// WASM admission, a real worker, or persistent-browser durability.
function fixture(result: unknown) {
  const observeLocalBatch = vi.fn((..._args: unknown[]) => result);
  const db = {
    observeLocalBatch,
    all: vi.fn(() => Uint8Array.of(0)),
    setTickScheduler: vi.fn(),
    setNonDurableClient: vi.fn(),
    setSessionClaims: vi.fn(),
    close: vi.fn(),
    tick: vi.fn(),
  };
  const schema = {
    __e2ee_roots: {
      columns: [{ name: "title", column_type: { type: "Text" as const }, nullable: false }],
    },
  };
  const runtime = new NativeRuntimeAdapter(
    { openMemory: () => db } as never,
    schema,
    new Uint8Array(16),
    new TextEncoder().encode('["urn:jazz:test","alice"]'),
    1,
    true,
  );
  return { runtime, db, schema: encodeSchema(schema) };
}

describe("local observation adapter", () => {
  it("passes all queries to one core call and keeps the shared cut", async () => {
    const { runtime, db } = fixture([empty(), empty()]);
    try {
      const result = await runtime.observeLocalBatch([query, query]);
      expect(result).toEqual([
        { ...empty(), rows: [] },
        { ...empty(), rows: [] },
      ]);
      expect(db.observeLocalBatch).toHaveBeenCalledOnce();
      expect(db.observeLocalBatch.mock.calls[0]?.[0]).toHaveLength(2);
    } finally {
      await runtime.close();
    }
  });

  it("uses the admitted peer binding without forwarding a realm session token", async () => {
    const { runtime, db, schema } = fixture([empty()]);
    const observeLocalBatch = vi.fn(() => [empty()]);
    const peer = { observeLocalBatch };
    const claims = { scope: "admitted" };
    try {
      await expect(
        runtime.observeLocalBatchForPeer(peer as never, [query], claims, schema),
      ).resolves.toEqual([{ ...empty(), rows: [] }]);
      expect(observeLocalBatch).toHaveBeenCalledExactlyOnceWith([expect.any(Uint8Array)], claims);
      expect(db.observeLocalBatch).not.toHaveBeenCalled();
      await expect(
        runtime.observeLocalBatchForPeer(peer as never, [query], claims, Uint8Array.of(99)),
      ).rejects.toThrow("schema view");
      expect(observeLocalBatch).toHaveBeenCalledOnce();
      await runtime.close();
      await expect(
        runtime.observeLocalBatchForPeer(peer as never, [query], claims, schema),
      ).rejects.toThrow("closed");
      expect(observeLocalBatch).toHaveBeenCalledOnce();
    } finally {
      await runtime.close();
    }
  });

  it("rejects cuts that differ across queries", async () => {
    const { runtime } = fixture([empty("1"), empty("2")]);
    try {
      await expect(runtime.observeLocalBatch([query, query])).rejects.toThrow("one cut");
    } finally {
      await runtime.close();
    }
  });

  it("rejects unpaired evidence instead of treating a receipt as a row", async () => {
    const { runtime } = fixture([{ ...empty(), evidence: [{ rowId: "not-a-row" }] }]);
    try {
      await expect(runtime.observeLocalBatch([query])).rejects.toThrow(
        "Incomplete local observation evidence",
      );
    } finally {
      await runtime.close();
    }
  });

  it("does not select a caller-provided schema on the durable owner", async () => {
    const { runtime, db } = fixture([empty()]);
    try {
      await expect(
        runtime.observeLocalBatch([query], undefined, Uint8Array.of(99)),
      ).rejects.toThrow("schema view");
      expect(db.observeLocalBatch).not.toHaveBeenCalled();
    } finally {
      await runtime.close();
    }
  });

  it("revokes a foreground relay without falling back to its volatile replica", async () => {
    const { runtime, db } = fixture([empty()]);
    runtime.setNonDurableClient();
    const relay = vi.fn(async () => [{ ...empty(), rows: [] }]);
    const dispose = runtime.setLocalObservationRelay(relay);
    try {
      await runtime.observeLocalBatch([query]);
      expect(relay).toHaveBeenCalledOnce();
      expect(db.observeLocalBatch).not.toHaveBeenCalled();
      dispose();
      await expect(runtime.observeLocalBatch([query])).rejects.toThrow(
        "durable owner is unavailable",
      );
      expect(db.observeLocalBatch).not.toHaveBeenCalled();
    } finally {
      await runtime.close();
    }
  });
});

describe("authenticated durable-owner observation admission", () => {
  const session = (claims = { scope: "actual" }, user_id = "alice", account_id?: string) =>
    JSON.stringify({ issuer: "urn:jazz:test", user_id, account_id, authMode: "external", claims });

  function ownFixture() {
    const f = fixture([empty()]);
    const peer = { observeLocalBatch: vi.fn(() => [empty()]), close: vi.fn() };
    const admit = vi.fn(() => peer);
    Object.assign(f.db, { acceptOwnObservationSubscriber: admit });
    return { ...f, peer, admit };
  }

  it("reuses one native owner admission without reinstalling observation-call claims", async () => {
    const f = ownFixture();
    try {
      await f.runtime.query(query, session(), "local-only");
      f.db.setSessionClaims.mockClear();
      await f.runtime.observeLocalBatch([query], session());
      await f.runtime.observeLocalBatch([query], session());
      expect(f.admit).toHaveBeenCalledExactlyOnceWith();
      expect(f.peer.observeLocalBatch).toHaveBeenCalledTimes(2);
      expect(f.db.setSessionClaims).not.toHaveBeenCalled();
      expect(f.db.observeLocalBatch).not.toHaveBeenCalled();
      await f.runtime.close();
      expect(f.peer.close).toHaveBeenCalledOnce();
      await expect(f.runtime.observeLocalBatch([query], session())).rejects.toThrow("closed");
      expect(f.admit).toHaveBeenCalledOnce();
    } finally {
      await f.runtime.close();
    }
  });

  it.each(["identity", "account", "claims"])(
    "refuses a changed %s without updating native registration",
    async (kind) => {
      const f = ownFixture();
      try {
        await f.runtime.query(query, session(), "local-only");
        f.db.setSessionClaims.mockClear();
        const altered =
          kind === "identity"
            ? session(undefined, "bob")
            : kind === "account"
              ? session(undefined, "alice", "00000000-0000-4000-8000-000000000002")
              : session({ scope: "unadmitted" });
        await expect(f.runtime.observeLocalBatch([query], altered)).rejects.toThrow(
          /owner identity|installed owner claims/,
        );
        expect(f.admit).not.toHaveBeenCalled();
        expect(f.db.setSessionClaims).not.toHaveBeenCalled();
        expect(f.db.observeLocalBatch).not.toHaveBeenCalled();
      } finally {
        await f.runtime.close();
      }
    },
  );

  it.each(["before first observation", "after admission"])(
    "does not restore withdrawn native scope %s",
    async (when) => {
      const f = ownFixture();
      try {
        await f.runtime.query(query, session(), "local-only");
        if (when === "after admission") await f.runtime.observeLocalBatch([query], session());
        f.db.setSessionClaims.mockClear();
        f.peer.observeLocalBatch.mockImplementation(() => {
          throw new Error("local observation admission claims changed");
        });
        await expect(f.runtime.observeLocalBatch([query], session())).rejects.toThrow(
          "claims changed",
        );
        await expect(f.runtime.observeLocalBatch([query], session())).rejects.toThrow(
          "claims changed",
        );
        expect(f.admit).toHaveBeenCalledExactlyOnceWith();
        expect(f.db.setSessionClaims).not.toHaveBeenCalled();
        expect(f.db.observeLocalBatch).not.toHaveBeenCalled();
      } finally {
        await f.runtime.close();
      }
    },
  );
});
