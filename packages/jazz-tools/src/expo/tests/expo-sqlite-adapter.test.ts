import { describe, expect, it, vi } from "vitest";

vi.mock("expo-sqlite", () => ({
  openDatabaseAsync: vi.fn().mockResolvedValue({
    execAsync: vi.fn(),
  }),
  deleteDatabaseAsync: vi.fn(),
}));

import { ExpoSQLiteAdapter } from "../storage/expo-sqlite-adapter.js";

describe("ExpoSQLiteAdapter", () => {
  describe("getInstance", () => {
    it("returns the same instance for the same database name", async () => {
      const adapter1 = ExpoSQLiteAdapter.getInstance("test-db");
      const adapter2 = ExpoSQLiteAdapter.getInstance("test-db");

      expect(adapter1).toBe(adapter2);
    });

    it("returns different instances for different database names", async () => {
      const adapter1 = ExpoSQLiteAdapter.getInstance("test-db-a");
      const adapter2 = ExpoSQLiteAdapter.getInstance("test-db-b");

      expect(adapter1).not.toBe(adapter2);
    });

    it("initializes the database connection only once", async () => {
      const adapter1 = ExpoSQLiteAdapter.getInstance("test-db");
      const adapter2 = ExpoSQLiteAdapter.getInstance("test-db");
      const promise1 = adapter1.initialize();
      const promise2 = adapter2.initialize();

      await promise1;
      await promise2;

      // @ts-expect-error - db is private
      expect(adapter1.db?.execAsync).toHaveBeenCalledTimes(1);
    });
  });
});
