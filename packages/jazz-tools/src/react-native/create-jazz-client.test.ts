import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import {
  createDb,
  createJazzClient,
  REACT_NATIVE_AUTH_SECRET_STORE_REQUIRED_ERROR,
  REACT_NATIVE_PERSISTENT_RUNTIME_UNAVAILABLE_ERROR,
  REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR,
  type JazzClient,
  type ReactNativeSqliteStorageDriver,
  useLocalFirstAuth,
} from "./index.js";

const app = s.defineApp({
  notes: s.table({ title: s.string() }),
});

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
    expect((error as Error).message).toBe(REACT_NATIVE_PERSISTENT_RUNTIME_UNAVAILABLE_ERROR);
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
