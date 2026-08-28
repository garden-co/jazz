import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import {
  createDb,
  createJazzClient,
  REACT_NATIVE_AUTH_SECRET_STORE_REQUIRED_ERROR,
  REACT_NATIVE_NATIVE_RELAY_MEMORY_ONLY_ERROR,
  REACT_NATIVE_NATIVE_RELAY_REQUIRED_ERROR,
  REACT_NATIVE_PERSISTENT_RUNTIME_UNAVAILABLE_ERROR,
  REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR,
  type JazzClient,
  type ReactNativeSqliteStorageDriver,
  type NativeRelayCapability,
  useLocalFirstAuth,
} from "./index.js";

const app = s.defineApp({
  notes: s.table({ title: s.string() }),
});

const nativeRelayCapability = Uint8Array.from(
  { length: 32 },
  (_, index) => index,
) as NativeRelayCapability;

function nativeRelayReceipt() {
  const commands: number[] = [];
  const base64 = (bytes: number[]) => btoa(String.fromCharCode(...bytes));
  return {
    commands,
    config: {
      executor: {
        execute: async (request: string) => {
          const tag = atob(request).charCodeAt(0);
          commands.push(tag);
          // Open, Attach, Receive, CloseClient/CloseRelay, and frame work.
          return tag === 1
            ? base64([1, 9])
            : tag === 2
              ? base64([2, 7])
              : tag === 7
                ? base64([5, 0])
                : tag === 3 || tag === 4
                  ? base64([3, 1])
                  : base64([4]);
        },
      },
      capability: nativeRelayCapability,
    },
  };
}

describe("React Native binding scaffolding in the Node test runtime", () => {
  let client: JazzClient | undefined;

  afterEach(async () => {
    await client?.shutdown();
    client = undefined;
  });

  it("exports the exact installed-package persistence boundary messages", () => {
    expect(REACT_NATIVE_PERSISTENT_RUNTIME_UNAVAILABLE_ERROR).toBe(
      "React Native persistent storage is not available in this alpha; memory mode is unverified scaffolding, not device-supported persistence",
    );
    expect(REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR).toBe(
      "ReactNativeDbConfig.sqliteStorage is proposal-only and cannot be used by the v2 runtime; remove sqliteStorage (memory mode remains unverified scaffolding)",
    );
    expect(REACT_NATIVE_NATIVE_RELAY_REQUIRED_ERROR).toMatch(/JazzRelay native artifact/);
  });

  it("never falls back to browser localStorage for a React Native auth root", () => {
    expect(() => useLocalFirstAuth({} as never)).toThrow(
      REACT_NATIVE_AUTH_SECRET_STORE_REQUIRED_ERROR,
    );
  });

  it("rejects a server-only credential copied through a React Native client config", async () => {
    const serverConfig = {
      appId: "react-native-backend-secret-boundary",
      driver: { type: "memory" as const },
      backendSecret: "server-only",
    };
    const error = await createDb({ ...serverConfig } as never).catch((error: unknown) => error);

    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toMatch(/createJazzContext/);
  });

  it("routes explicit memory configuration through the Node WASM harness", async () => {
    client = await createJazzClient({
      appId: "react-native-memory-launch-test",
      driver: { type: "memory" },
    });

    await expect(client.db.all(app.notes)).resolves.toEqual([]);
  });

  it("rejects the default persistent configuration", async () => {
    const error = await createDb({ appId: "react-native-default-persistent-boundary-test" }).catch(
      (error: unknown) => error,
    );
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toBe(REACT_NATIVE_NATIVE_RELAY_REQUIRED_ERROR);
  });

  it("uses one foreground memory runtime connected to the platform-admitted persistent relay", async () => {
    const relay = nativeRelayReceipt();
    client = await createJazzClient({
      appId: "react-native-native-relay-startup-receipt",
      nativeRelay: relay.config,
    });

    await expect(client.db.all(app.notes)).resolves.toEqual([]);
    expect(relay.commands).toEqual(expect.arrayContaining([1, 2]));

    await client.shutdown();
    client = undefined;
    expect(relay.commands).toEqual(expect.arrayContaining([3, 4]));
  });

  it("replaces only the foreground peer alias across explicit offline/reconnect", async () => {
    const relay = nativeRelayReceipt();
    client = await createJazzClient({
      appId: "react-native-native-relay-reconnect-receipt",
      serverUrl: "wss://relay.example.test",
      nativeRelay: relay.config,
    });

    await client.db.all(app.notes, { tier: "local" });
    await client.db.disconnect();
    await client.db.reconnect();
    await client.db.all(app.notes, { tier: "local" });

    expect(relay.commands.filter((tag) => tag === 1)).toHaveLength(2);
    expect(relay.commands.filter((tag) => tag === 2)).toHaveLength(2);
    expect(relay.commands).toEqual(expect.arrayContaining([3, 4]));
  });

  it("names native artifact, admission, and ABI failures at persistent startup", async () => {
    const relay = nativeRelayReceipt();
    relay.config.executor.execute = async () => btoa(String.fromCharCode(9));
    client = await createJazzClient({
      appId: "react-native-native-relay-abi-receipt",
      nativeRelay: relay.config,
    });

    await expect(client.db.all(app.notes)).rejects.toThrow(
      /installed native artifact, platform admission capability, and relay command ABI/,
    );
  });

  it("rejects an opaque relay capability when memory mode would ignore it", async () => {
    const relay = nativeRelayReceipt();
    const error = await createDb({
      appId: "react-native-native-relay-memory-boundary",
      driver: { type: "memory" },
      nativeRelay: relay.config,
    }).catch((error: unknown) => error);

    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toBe(REACT_NATIVE_NATIVE_RELAY_MEMORY_ONLY_ERROR);
  });

  it("rejects an injected SQLite driver before opening it", async () => {
    const open = vi.fn();
    const sqliteStorage: ReactNativeSqliteStorageDriver = {
      type: "react-native-sqlite",
      open,
      deleteDatabase: vi.fn(),
    };

    const error = await createDb({
      appId: "react-native-persistent-boundary-test",
      sqliteStorage,
    }).catch((error: unknown) => error);
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toBe(REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR);
    expect(open).not.toHaveBeenCalled();
  });

  it("rejects rather than ignores sqliteStorage combined with memory mode", async () => {
    const open = vi.fn();
    const sqliteStorage: ReactNativeSqliteStorageDriver = {
      type: "react-native-sqlite",
      open,
      deleteDatabase: vi.fn(),
    };

    const error = await createDb({
      appId: "react-native-memory-sqlite-ambiguity-test",
      driver: { type: "memory" },
      sqliteStorage,
    }).catch((error: unknown) => error);
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toBe(REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR);
    expect(open).not.toHaveBeenCalled();
  });

  it("can shut down and reopen the Node-only memory scaffold", async () => {
    const config = {
      appId: "react-native-memory-reopen-test",
      driver: { type: "memory" as const },
    };
    client = await createJazzClient(config);
    await expect(client.db.all(app.notes)).resolves.toEqual([]);
    await client.shutdown();

    client = await createJazzClient(config);
    await expect(client.db.all(app.notes)).resolves.toEqual([]);
  });
});
