import { describe, it, expect, vi } from "vitest";
import { JazzClient } from "./client.js";
import type { AppContext } from "./context.js";

function makeFakeRuntime() {
  return {
    updateAuth: vi.fn(),
    onAuthFailure: vi.fn(),
    // Runtime interface stubs
    insert: vi.fn(),
    insertDurable: vi.fn(),
    update: vi.fn(),
    updateDurable: vi.fn(),
    delete: vi.fn(),
    deleteDurable: vi.fn(),
    query: vi.fn(),
    subscribe: vi.fn(),
    createSubscription: vi.fn(),
    executeSubscription: vi.fn(),
    unsubscribe: vi.fn(),
    onSyncMessageReceived: vi.fn(),
    addServer: vi.fn(),
    removeServer: vi.fn(),
    addClient: vi.fn().mockReturnValue("client-id"),
    getSchema: vi.fn().mockReturnValue({}),
    getSchemaHash: vi.fn().mockReturnValue("hash"),
    close: vi.fn(),
  };
}

function makeContext(): AppContext {
  return {
    appId: "test-app",
    schema: {},
    serverUrl: "https://example.test",
    jwtToken: "initial.jwt.token",
  };
}

describe("JazzClient.updateAuthToken", () => {
  it("forwards refreshed JWT to the Rust runtime via runtime.updateAuth", () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());

    client.updateAuthToken("new.jwt.token");

    expect(runtime.updateAuth).toHaveBeenCalledTimes(1);
    const arg = runtime.updateAuth.mock.calls[0][0] as string;
    expect(JSON.parse(arg)).toMatchObject({ jwt_token: "new.jwt.token" });
  });

  it("forwards undefined JWT (clear) as null jwt_token", () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());

    client.updateAuthToken(undefined);

    expect(runtime.updateAuth).toHaveBeenCalledTimes(1);
    const arg = runtime.updateAuth.mock.calls[0][0] as string;
    expect(JSON.parse(arg)).toMatchObject({ jwt_token: null });
  });
});
