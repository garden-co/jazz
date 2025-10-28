import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { CoJsonIDBTransaction } from "../CoJsonIDBTransaction";

const TEST_DB_NAME = "test-cojson-idb-transaction";

describe("CoJsonIDBTransaction", () => {
  let db: IDBDatabase;

  beforeEach(async () => {
    // Create test database
    await new Promise<void>((resolve, reject) => {
      const request = indexedDB.open(TEST_DB_NAME, 1);

      request.onerror = () => reject(request.error);

      request.onupgradeneeded = (event) => {
        const db = request.result;
        // Create test stores
        db.createObjectStore("coValues", { keyPath: "id" });
        const sessions = db.createObjectStore("sessions", { keyPath: "id" });
        sessions.createIndex("uniqueSessions", ["coValue", "sessionID"], {
          unique: true,
        });
        db.createObjectStore("transactions", { keyPath: "id" });
        db.createObjectStore("signatureAfter", { keyPath: "id" });
      };

      request.onsuccess = () => {
        db = request.result;
        resolve();
      };
    });
  });

  afterEach(async () => {
    // Close and delete test database
    db.close();
    await new Promise<void>((resolve, reject) => {
      const request = indexedDB.deleteDatabase(TEST_DB_NAME);
      request.onerror = () => reject(request.error);
      request.onsuccess = () => resolve();
    });
  });

  test("handles successful write and read operations", async () => {
    const tx = new CoJsonIDBTransaction(db);

    // Write test
    await tx.handleRequest((tx) =>
      tx.getObjectStore("coValues").put({
        id: "test1",
        value: "hello",
      }),
    );

    // Read test
    const readTx = new CoJsonIDBTransaction(db);
    const result = await readTx.handleRequest((tx) =>
      tx.getObjectStore("coValues").get("test1"),
    );

    expect(result).toEqual({
      id: "test1",
      value: "hello",
    });
  });

  test("handles multiple operations in single transaction", async () => {
    const tx = new CoJsonIDBTransaction(db);

    // Multiple writes
    await Promise.all([
      tx.handleRequest((tx) =>
        tx.getObjectStore("coValues").put({
          id: "test1",
          value: "hello",
        }),
      ),
      tx.handleRequest((tx) =>
        tx.getObjectStore("coValues").put({
          id: "test2",
          value: "world",
        }),
      ),
    ]);

    // Read results
    const readTx = new CoJsonIDBTransaction(db);
    const [result1, result2] = await Promise.all([
      readTx.handleRequest((tx) => tx.getObjectStore("coValues").get("test1")),
      readTx.handleRequest((tx) => tx.getObjectStore("coValues").get("test2")),
    ]);

    expect(result1).toEqual({
      id: "test1",
      value: "hello",
    });
    expect(result2).toEqual({
      id: "test2",
      value: "world",
    });
  });

  test("handles transaction across multiple stores", async () => {
    const tx = new CoJsonIDBTransaction(db);

    await Promise.all([
      tx.handleRequest((tx) =>
        tx.getObjectStore("coValues").put({
          id: "value1",
          data: "value data",
        }),
      ),
      tx.handleRequest((tx) =>
        tx.getObjectStore("sessions").put({
          id: "session1",
          data: "session data",
        }),
      ),
    ]);

    const readTx = new CoJsonIDBTransaction(db);
    const [valueResult, sessionResult] = await Promise.all([
      readTx.handleRequest((tx) => tx.getObjectStore("coValues").get("value1")),
      readTx.handleRequest((tx) =>
        tx.getObjectStore("sessions").get("session1"),
      ),
    ]);

    expect(valueResult).toEqual({
      id: "value1",
      data: "value data",
    });
    expect(sessionResult).toEqual({
      id: "session1",
      data: "session data",
    });
  });

  test("handles failed transactions", async () => {
    const tx = new CoJsonIDBTransaction(db);

    await expect(
      tx.handleRequest((tx) =>
        tx.getObjectStore("sessions").put({
          id: 1,
          coValue: "value1",
          sessionID: "session1",
          data: "session data",
        }),
      ),
    ).resolves.toBe(1);

    expect(tx.failed).toBe(false);

    const badTx = new CoJsonIDBTransaction(db);
    await expect(
      badTx.handleRequest((tx) =>
        tx.getObjectStore("sessions").put({
          id: 2,
          coValue: "value1",
          sessionID: "session1",
          data: "session data",
        }),
      ),
    ).rejects.toThrow();

    expect(badTx.failed).toBe(true);
  });
});
