import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { createJazzClient, type JazzClient } from "./create-jazz-client.js";
import { createDb } from "./create-db.js";
import {
  REACT_NATIVE_PERSISTENT_RUNTIME_UNAVAILABLE_ERROR,
  REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR,
  type ReactNativeSqliteStorageDriver,
} from "./storage.js";

const app = s.defineApp({
  notes: s.table({ title: s.string() }),
});

describe("React Native binding scaffolding in the Node test runtime", () => {
  let client: JazzClient | undefined;

  afterEach(async () => {
    await client?.shutdown();
    client = undefined;
  });

  it("routes explicit memory configuration through the Node WASM harness", async () => {
    client = await createJazzClient({
      appId: "react-native-memory-launch-test",
      driver: { type: "memory" },
    });

    await expect(client.db.all(app.notes)).resolves.toEqual([]);
  });

  it("rejects the default persistent configuration", async () => {
    await expect(
      createDb({ appId: "react-native-default-persistent-boundary-test" }),
    ).rejects.toThrow(REACT_NATIVE_PERSISTENT_RUNTIME_UNAVAILABLE_ERROR);
  });

  it("rejects an injected SQLite driver before opening it", async () => {
    const open = vi.fn();
    const sqliteStorage: ReactNativeSqliteStorageDriver = {
      type: "react-native-sqlite",
      open,
      deleteDatabase: vi.fn(),
    };

    await expect(
      createDb({ appId: "react-native-persistent-boundary-test", sqliteStorage }),
    ).rejects.toThrow(REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR);
    expect(open).not.toHaveBeenCalled();
  });

  it("rejects rather than ignores sqliteStorage combined with memory mode", async () => {
    const open = vi.fn();
    const sqliteStorage: ReactNativeSqliteStorageDriver = {
      type: "react-native-sqlite",
      open,
      deleteDatabase: vi.fn(),
    };

    await expect(
      createDb({
        appId: "react-native-memory-sqlite-ambiguity-test",
        driver: { type: "memory" },
        sqliteStorage,
      }),
    ).rejects.toThrow(REACT_NATIVE_SQLITE_STORAGE_REJECTED_ERROR);
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
