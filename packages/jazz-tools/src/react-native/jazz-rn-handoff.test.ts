import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../drivers/types.js";

function makeFakeRnBinding() {
  return {
    onBatchedTickNeeded: vi.fn(),
    addClient: vi.fn(),
    addServer: vi.fn(),
    batchedTick: vi.fn(),
    close: vi.fn(),
    connect: vi.fn(),
    disconnect: vi.fn(),
    updateAuth: vi.fn(),
    onAuthFailure: vi.fn(),
    delete_: vi.fn(),
    flush: vi.fn(),
    getSchemaHash: vi.fn(() => "fake-hash"),
    insert: vi.fn(),
    onSyncMessageReceived: vi.fn(),
    onSyncMessageReceivedFromClient: vi.fn(),
    query: vi.fn(),
    removeServer: vi.fn(),
    setClientRole: vi.fn(),
    createSubscription: vi.fn(),
    executeSubscription: vi.fn(),
    subscribe: vi.fn(),
    unsubscribe: vi.fn(),
    update: vi.fn(),
  };
}

const RnRuntimeCtor = vi.fn(function (this: unknown) {
  return makeFakeRnBinding();
});
const mintLocalFirstToken = vi.fn(() => "minted-jwt");
const mintAnonymousToken = vi.fn(() => "minted-anon-jwt");

vi.mock("jazz-rn", () => ({
  default: {
    jazz_rn: {
      RnRuntime: RnRuntimeCtor as unknown as new (...args: unknown[]) => unknown,
      mintLocalFirstToken,
      mintAnonymousToken,
    },
  },
}));

beforeEach(() => {
  RnRuntimeCtor.mockClear();
  mintLocalFirstToken.mockClear();
  mintAnonymousToken.mockClear();
  vi.resetModules();
});

afterEach(() => {
  vi.restoreAllMocks();
});

function makeSchema(table: string): WasmSchema {
  return {
    [table]: { columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }] },
  };
}

describe("createJazzRnRuntime / loadJazzRn handoff", () => {
  it("RNHO-U01 createJazzRnRuntime succeeds after createDb has primed the loader", async () => {
    const { createDb } = await import("./db.js");
    const { createJazzRnRuntime } = await import("./create-jazz-rn-runtime.js");

    await createDb({ appId: "rn-handoff" });

    expect(() =>
      createJazzRnRuntime({ schema: makeSchema("todos"), appId: "rn-handoff" }),
    ).not.toThrow();
    expect(RnRuntimeCtor).toHaveBeenCalledTimes(1);
  });

  it("RNHO-U02 createJazzRnRuntime throws an actionable error when invoked without prior load", async () => {
    const { createJazzRnRuntime } = await import("./create-jazz-rn-runtime.js");

    expect(() => createJazzRnRuntime({ schema: makeSchema("todos"), appId: "rn-handoff" })).toThrow(
      /accessed before it was loaded/,
    );
  });

  it("RNHO-U03 Db#getLocalFirstIdentityProof lazy-loads jazz-rn and mints a token", async () => {
    const { createDb } = await import("./db.js");
    const db = await createDb({ appId: "rn-handoff", secret: "seed-32-bytes" });

    const proof = db.getLocalFirstIdentityProof({ ttlSeconds: 120, audience: "test-aud" });

    expect(proof).toBe("minted-jwt");
    expect(mintLocalFirstToken).toHaveBeenCalledWith("seed-32-bytes", "test-aud", BigInt(120));
  });

  it("RNHO-U04 Db#getLocalFirstIdentityProof returns null when no secret is configured", async () => {
    const { createDb } = await import("./db.js");
    const db = await createDb({ appId: "rn-handoff" });

    const proof = db.getLocalFirstIdentityProof();
    expect(proof).toBeNull();
    expect(mintLocalFirstToken).not.toHaveBeenCalled();
  });
});
