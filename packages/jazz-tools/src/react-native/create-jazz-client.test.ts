import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { createJazzClient, type JazzClient } from "./create-jazz-client.js";
import { createDb } from "./create-db.js";
import {
  REACT_NATIVE_PERSISTENT_RUNTIME_UNAVAILABLE_ERROR,
  type ReactNativeSqliteStorageDriver,
} from "./storage.js";

const app = s.defineApp({
  notes: s.table({ title: s.string() }),
});

describe("React Native client launch", () => {
  let client: JazzClient | undefined;

  afterEach(async () => {
    await client?.shutdown();
    client = undefined;
  });

  it("launches the public client with the in-memory driver and can query", async () => {
    client = await createJazzClient({
      appId: "react-native-memory-launch-test",
      driver: { type: "memory" },
    });

    await expect(client.db.all(app.notes)).resolves.toEqual([]);
  });

  it("rejects persistent startup without pretending an injected SQLite driver stores Jazz data", async () => {
    const open = vi.fn();
    const sqliteStorage: ReactNativeSqliteStorageDriver = {
      type: "react-native-sqlite",
      open,
      deleteDatabase: vi.fn(),
    };

    await expect(
      createDb({ appId: "react-native-persistent-boundary-test", sqliteStorage }),
    ).rejects.toThrow(REACT_NATIVE_PERSISTENT_RUNTIME_UNAVAILABLE_ERROR);
    expect(open).not.toHaveBeenCalled();
  });

  it("can shut down and reopen the supported in-memory runtime", async () => {
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
