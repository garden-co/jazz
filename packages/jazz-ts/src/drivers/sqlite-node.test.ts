/**
 * Tests for SQLite Node.js driver.
 *
 * Note: Requires Node.js 22+ for node:sqlite support.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { SqliteNodeDriver } from "./sqlite-node.js";
import type { StorageRequest, Commit } from "./types.js";

// Check if node:sqlite is available (Node.js 22+)
// Use process.version to do a synchronous check
const nodeVersion = parseInt(process.version.slice(1).split(".")[0], 10);
const hasNodeSqlite = nodeVersion >= 22;

describe.skipIf(!hasNodeSqlite)("SqliteNodeDriver", () => {
  let driver: SqliteNodeDriver;

  beforeEach(async () => {
    driver = await SqliteNodeDriver.open(":memory:");
  });

  afterEach(async () => {
    await driver.close();
  });

  describe("Object operations", () => {
    it("creates an object", async () => {
      const requests: StorageRequest[] = [
        {
          type: "CreateObject",
          id: "550e8400-e29b-41d4-a716-446655440000",
          metadata: { type: "todo" },
        },
      ];

      const responses = await driver.process(requests);
      expect(responses).toHaveLength(1);
      expect(responses[0].type).toBe("CreateObject");
      expect((responses[0] as any).success).toBe(true);
    });

    it("creates multiple objects", async () => {
      const requests: StorageRequest[] = [
        {
          type: "CreateObject",
          id: "id-1",
          metadata: { type: "todo" },
        },
        {
          type: "CreateObject",
          id: "id-2",
          metadata: { type: "user" },
        },
      ];

      const responses = await driver.process(requests);
      expect(responses).toHaveLength(2);
      expect(responses.every((r) => (r as any).success)).toBe(true);
    });
  });

  describe("Commit operations", () => {
    const testCommit: Commit = {
      parents: [],
      content: new Uint8Array([1, 2, 3, 4]),
      timestamp: Date.now() * 1000,
      author: "550e8400-e29b-41d4-a716-446655440000",
      metadata: { message: "Initial commit" },
    };

    it("appends a commit", async () => {
      const requests: StorageRequest[] = [
        {
          type: "CreateObject",
          id: "obj-1",
          metadata: {},
        },
        {
          type: "AppendCommit",
          object_id: "obj-1",
          branch_name: "main",
          commit: testCommit,
        },
      ];

      const responses = await driver.process(requests);
      expect(responses).toHaveLength(2);
      expect(responses[1].type).toBe("AppendCommit");
      expect((responses[1] as any).success).toBe(true);
      expect((responses[1] as any).commit_id).toBeTruthy();
    });

    it("loads branch with TipIdsOnly", async () => {
      // First create and append
      await driver.process([
        { type: "CreateObject", id: "obj-1", metadata: {} },
        {
          type: "AppendCommit",
          object_id: "obj-1",
          branch_name: "main",
          commit: testCommit,
        },
      ]);

      // Then load
      const responses = await driver.process([
        {
          type: "LoadObjectBranch",
          object_id: "obj-1",
          branch_name: "main",
          depth: "TipIdsOnly",
        },
      ]);

      expect(responses).toHaveLength(1);
      expect(responses[0].type).toBe("LoadObjectBranch");

      const loadResp = responses[0] as any;
      expect(loadResp.branch).toBeDefined();
      expect(loadResp.branch.tips).toHaveLength(1);
      // TipIdsOnly should have empty commits
      expect(Object.keys(loadResp.branch.commits)).toHaveLength(0);
    });

    it("loads branch with AllCommits", async () => {
      await driver.process([
        { type: "CreateObject", id: "obj-1", metadata: {} },
        {
          type: "AppendCommit",
          object_id: "obj-1",
          branch_name: "main",
          commit: testCommit,
        },
      ]);

      const responses = await driver.process([
        {
          type: "LoadObjectBranch",
          object_id: "obj-1",
          branch_name: "main",
          depth: "AllCommits",
        },
      ]);

      const loadResp = responses[0] as any;
      expect(loadResp.branch).toBeDefined();
      expect(Object.keys(loadResp.branch.commits)).toHaveLength(1);
    });

    it("returns NotFound for non-existent branch", async () => {
      const responses = await driver.process([
        {
          type: "LoadObjectBranch",
          object_id: "non-existent",
          branch_name: "main",
          depth: "TipIdsOnly",
        },
      ]);

      expect(responses[0].type).toBe("LoadObjectBranch");
      expect((responses[0] as any).error).toBe("NotFound");
    });

    it("deletes a commit", async () => {
      // Create and append
      const appendResponses = await driver.process([
        { type: "CreateObject", id: "obj-1", metadata: {} },
        {
          type: "AppendCommit",
          object_id: "obj-1",
          branch_name: "main",
          commit: testCommit,
        },
      ]);

      const commitId = (appendResponses[1] as any).commit_id;

      // Delete
      const deleteResponses = await driver.process([
        {
          type: "DeleteCommit",
          object_id: "obj-1",
          branch_name: "main",
          commit_id: commitId,
        },
      ]);

      expect(deleteResponses[0].type).toBe("DeleteCommit");
      expect((deleteResponses[0] as any).success).toBe(true);
    });
  });

  describe("Blob operations", () => {
    const testData = new Uint8Array([72, 101, 108, 108, 111]); // "Hello"
    const testHash = "0".repeat(64);

    it("stores a blob", async () => {
      const responses = await driver.process([
        { type: "StoreBlob", content_hash: testHash, data: testData },
      ]);

      expect(responses[0].type).toBe("StoreBlob");
      expect((responses[0] as any).success).toBe(true);
    });

    it("loads a blob", async () => {
      await driver.process([{ type: "StoreBlob", content_hash: testHash, data: testData }]);

      const responses = await driver.process([{ type: "LoadBlob", content_hash: testHash }]);

      expect(responses[0].type).toBe("LoadBlob");
      expect((responses[0] as any).data).toEqual(testData);
    });

    it("returns NotFound for non-existent blob", async () => {
      const responses = await driver.process([{ type: "LoadBlob", content_hash: "nonexistent" }]);

      expect((responses[0] as any).error).toBe("NotFound");
    });

    it("associates and loads blob associations", async () => {
      await driver.process([
        { type: "StoreBlob", content_hash: testHash, data: testData },
        {
          type: "AssociateBlob",
          content_hash: testHash,
          object_id: "obj-1",
          branch_name: "main",
          commit_id: "commit-1",
        },
      ]);

      const responses = await driver.process([
        { type: "LoadBlobAssociations", content_hash: testHash },
      ]);

      expect(responses[0].type).toBe("LoadBlobAssociations");
      const assocs = (responses[0] as any).associations;
      expect(assocs).toHaveLength(1);
      expect(assocs[0].object_id).toBe("obj-1");
    });

    it("dissociates and deletes orphaned blob", async () => {
      await driver.process([
        { type: "StoreBlob", content_hash: testHash, data: testData },
        {
          type: "AssociateBlob",
          content_hash: testHash,
          object_id: "obj-1",
          branch_name: "main",
          commit_id: "commit-1",
        },
      ]);

      const responses = await driver.process([
        {
          type: "DissociateAndMaybeDeleteBlob",
          content_hash: testHash,
          object_id: "obj-1",
          branch_name: "main",
          commit_id: "commit-1",
        },
      ]);

      expect(responses[0].type).toBe("DissociateAndMaybeDeleteBlob");
      expect((responses[0] as any).blob_deleted).toBe(true);

      // Verify blob is gone
      const loadResponses = await driver.process([{ type: "LoadBlob", content_hash: testHash }]);
      expect((loadResponses[0] as any).error).toBe("NotFound");
    });
  });

  describe("Index operations", () => {
    const testData = new Uint8Array([1, 2, 3, 4, 5]);

    it("stores and loads index page", async () => {
      await driver.process([
        {
          type: "StoreIndexPage",
          table: "todos",
          column: "_id",
          page_id: 0,
          data: testData,
        },
      ]);

      const responses = await driver.process([
        { type: "LoadIndexPage", table: "todos", column: "_id", page_id: 0 },
      ]);

      expect(responses[0].type).toBe("LoadIndexPage");
      expect((responses[0] as any).data).toEqual(testData);
    });

    it("returns undefined for non-existent page", async () => {
      const responses = await driver.process([
        { type: "LoadIndexPage", table: "todos", column: "_id", page_id: 999 },
      ]);

      expect((responses[0] as any).data).toBeUndefined();
      expect((responses[0] as any).error).toBeUndefined();
    });

    it("deletes index page", async () => {
      await driver.process([
        {
          type: "StoreIndexPage",
          table: "todos",
          column: "_id",
          page_id: 0,
          data: testData,
        },
      ]);

      await driver.process([
        { type: "DeleteIndexPage", table: "todos", column: "_id", page_id: 0 },
      ]);

      const responses = await driver.process([
        { type: "LoadIndexPage", table: "todos", column: "_id", page_id: 0 },
      ]);

      expect((responses[0] as any).data).toBeUndefined();
    });

    it("stores and loads index meta", async () => {
      await driver.process([
        {
          type: "StoreIndexMeta",
          table: "todos",
          column: "_id",
          data: testData,
        },
      ]);

      const responses = await driver.process([
        { type: "LoadIndexMeta", table: "todos", column: "_id" },
      ]);

      expect(responses[0].type).toBe("LoadIndexMeta");
      expect((responses[0] as any).data).toEqual(testData);
    });
  });

  describe("Branch tails", () => {
    it("sets branch tails", async () => {
      // Create branch first
      await driver.process([
        { type: "CreateObject", id: "obj-1", metadata: {} },
        {
          type: "AppendCommit",
          object_id: "obj-1",
          branch_name: "main",
          commit: {
            parents: [],
            content: new Uint8Array([1]),
            timestamp: Date.now() * 1000,
            author: "author",
          },
        },
      ]);

      const responses = await driver.process([
        {
          type: "SetBranchTails",
          object_id: "obj-1",
          branch_name: "main",
          tails: ["tail-1", "tail-2"],
        },
      ]);

      expect(responses[0].type).toBe("SetBranchTails");
      expect((responses[0] as any).success).toBe(true);

      // Verify tails are set
      const loadResponses = await driver.process([
        {
          type: "LoadObjectBranch",
          object_id: "obj-1",
          branch_name: "main",
          depth: "TipIdsOnly",
        },
      ]);

      expect((loadResponses[0] as any).branch.tails).toEqual(["tail-1", "tail-2"]);
    });
  });
});
