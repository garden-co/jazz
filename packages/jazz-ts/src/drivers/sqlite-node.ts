/**
 * SQLite storage driver for Node.js using node:sqlite.
 *
 * Tables:
 * - objects: object_id, metadata_json
 * - commits: object_id, branch_name, commit_id, parents_json, content, timestamp, author, metadata_json, is_tip
 * - branch_tips: object_id, branch_name, tips_json, tails_json
 * - blobs: content_hash, data
 * - blob_associations: content_hash, object_id, branch_name, commit_id
 * - index_pages: table_name, column_name, page_id, data
 * - index_meta: table_name, column_name, data
 */

import { createHash } from "blake3";
import type {
  StorageDriver,
  StorageRequest,
  StorageResponse,
  Commit,
  LoadedBranch,
  BlobAssociation,
} from "./types.js";

// Type for node:sqlite Database (avoiding direct import for compatibility)
interface SqliteDatabase {
  exec(sql: string): void;
  prepare(sql: string): SqliteStatement;
  close(): void;
}

interface SqliteStatement {
  run(...params: unknown[]): { changes: number; lastInsertRowid: number };
  get(...params: unknown[]): unknown;
  all(...params: unknown[]): unknown[];
}

/**
 * SQLite storage driver using Node.js built-in sqlite module.
 */
export class SqliteNodeDriver implements StorageDriver {
  private db: SqliteDatabase;

  /**
   * Create a new SQLite driver.
   *
   * @param dbPath Path to SQLite database file, or ":memory:" for in-memory
   */
  constructor(db: SqliteDatabase) {
    this.db = db;
    this.initSchema();
  }

  /**
   * Create a driver from a database path.
   * Requires node:sqlite to be available (Node.js 22+).
   */
  static async open(dbPath: string): Promise<SqliteNodeDriver> {
    // Dynamic import to avoid bundling issues
    // node:sqlite is available in Node.js 22+
    // @ts-expect-error - node:sqlite types may not be available
    const { DatabaseSync } = await import("node:sqlite");
    const db = new DatabaseSync(dbPath) as unknown as SqliteDatabase;
    return new SqliteNodeDriver(db);
  }

  private initSchema(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS objects (
        object_id TEXT PRIMARY KEY,
        metadata_json TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS commits (
        object_id TEXT NOT NULL,
        branch_name TEXT NOT NULL,
        commit_id TEXT NOT NULL,
        parents_json TEXT NOT NULL,
        content BLOB NOT NULL,
        timestamp INTEGER NOT NULL,
        author TEXT NOT NULL,
        metadata_json TEXT,
        PRIMARY KEY (object_id, branch_name, commit_id)
      );

      CREATE TABLE IF NOT EXISTS branch_tips (
        object_id TEXT NOT NULL,
        branch_name TEXT NOT NULL,
        tips_json TEXT NOT NULL,
        tails_json TEXT,
        PRIMARY KEY (object_id, branch_name)
      );

      CREATE TABLE IF NOT EXISTS blobs (
        content_hash TEXT PRIMARY KEY,
        data BLOB NOT NULL
      );

      CREATE TABLE IF NOT EXISTS blob_associations (
        content_hash TEXT NOT NULL,
        object_id TEXT NOT NULL,
        branch_name TEXT NOT NULL,
        commit_id TEXT NOT NULL,
        PRIMARY KEY (content_hash, object_id, branch_name, commit_id)
      );

      CREATE TABLE IF NOT EXISTS index_pages (
        table_name TEXT NOT NULL,
        column_name TEXT NOT NULL,
        page_id INTEGER NOT NULL,
        data BLOB NOT NULL,
        PRIMARY KEY (table_name, column_name, page_id)
      );

      CREATE TABLE IF NOT EXISTS index_meta (
        table_name TEXT NOT NULL,
        column_name TEXT NOT NULL,
        data BLOB NOT NULL,
        PRIMARY KEY (table_name, column_name)
      );
    `);
  }

  async process(requests: StorageRequest[]): Promise<StorageResponse[]> {
    return requests.map((req) => this.processOne(req));
  }

  private processOne(req: StorageRequest): StorageResponse {
    switch (req.type) {
      case "CreateObject":
        return this.createObject(req.id, req.metadata);
      case "AppendCommit":
        return this.appendCommit(req.object_id, req.branch_name, req.commit);
      case "LoadObjectBranch":
        return this.loadObjectBranch(req.object_id, req.branch_name, req.depth);
      case "StoreBlob":
        return this.storeBlob(req.content_hash, req.data);
      case "LoadBlob":
        return this.loadBlob(req.content_hash);
      case "AssociateBlob":
        return this.associateBlob(req.content_hash, req.object_id, req.branch_name, req.commit_id);
      case "LoadBlobAssociations":
        return this.loadBlobAssociations(req.content_hash);
      case "DeleteCommit":
        return this.deleteCommit(req.object_id, req.branch_name, req.commit_id);
      case "DissociateAndMaybeDeleteBlob":
        return this.dissociateAndMaybeDeleteBlob(
          req.content_hash,
          req.object_id,
          req.branch_name,
          req.commit_id,
        );
      case "SetBranchTails":
        return this.setBranchTails(req.object_id, req.branch_name, req.tails);
      case "LoadIndexPage":
        return this.loadIndexPage(req.table, req.column, req.page_id);
      case "StoreIndexPage":
        return this.storeIndexPage(req.table, req.column, req.page_id, req.data);
      case "DeleteIndexPage":
        return this.deleteIndexPage(req.table, req.column, req.page_id);
      case "LoadIndexMeta":
        return this.loadIndexMeta(req.table, req.column);
      case "StoreIndexMeta":
        return this.storeIndexMeta(req.table, req.column, req.data);
    }
  }

  private createObject(id: string, metadata: Record<string, string>): StorageResponse {
    try {
      const stmt = this.db.prepare(
        "INSERT OR REPLACE INTO objects (object_id, metadata_json) VALUES (?, ?)",
      );
      stmt.run(id, JSON.stringify(metadata));
      return { type: "CreateObject", id, success: true };
    } catch (e) {
      return {
        type: "CreateObject",
        id,
        success: false,
        error: String(e),
      };
    }
  }

  private appendCommit(objectId: string, branchName: string, commit: Commit): StorageResponse {
    // Convert content to Uint8Array if it's a plain array (from WASM serialization)
    const content =
      commit.content instanceof Uint8Array
        ? commit.content
        : new Uint8Array(commit.content as unknown as number[]);
    try {
      // Compute commit ID using converted content
      const commitId = this.computeCommitId({ ...commit, content });

      // Insert commit
      const insertCommit = this.db.prepare(`
        INSERT OR REPLACE INTO commits
        (object_id, branch_name, commit_id, parents_json, content, timestamp, author, metadata_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `);
      insertCommit.run(
        objectId,
        branchName,
        commitId,
        JSON.stringify(commit.parents),
        content,
        commit.timestamp,
        commit.author,
        commit.metadata ? JSON.stringify(commit.metadata) : null,
      );

      // Update tips: remove parents, add new commit
      let tips = this.getBranchTips(objectId, branchName);
      for (const parent of commit.parents) {
        tips.delete(parent);
      }
      tips.add(commitId);
      this.saveBranchTips(objectId, branchName, tips);

      return {
        type: "AppendCommit",
        object_id: objectId,
        commit_id: commitId,
        success: true,
      };
    } catch (e) {
      return {
        type: "AppendCommit",
        object_id: objectId,
        commit_id: "",
        success: false,
        error: String(e),
      };
    }
  }

  private loadObjectBranch(
    objectId: string,
    branchName: string,
    depth: "TipIdsOnly" | "TipsOnly" | "AllCommits",
  ): StorageResponse {
    try {
      // Load branch tips/tails
      const tipsRow = this.db
        .prepare(
          "SELECT tips_json, tails_json FROM branch_tips WHERE object_id = ? AND branch_name = ?",
        )
        .get(objectId, branchName) as { tips_json: string; tails_json: string | null } | undefined;

      if (!tipsRow) {
        return {
          type: "LoadObjectBranch",
          object_id: objectId,
          branch_name: branchName,
          error: "NotFound",
        };
      }

      const tips: string[] = JSON.parse(tipsRow.tips_json);
      const tails: string[] | undefined = tipsRow.tails_json
        ? JSON.parse(tipsRow.tails_json)
        : undefined;

      let commits: Record<string, Commit> = {};

      if (depth === "TipsOnly") {
        // Load only tip commits
        for (const tipId of tips) {
          const commit = this.loadCommit(objectId, branchName, tipId);
          if (commit) {
            commits[tipId] = commit;
          }
        }
      } else if (depth === "AllCommits") {
        // Load all commits
        const rows = this.db
          .prepare(
            "SELECT commit_id, parents_json, content, timestamp, author, metadata_json FROM commits WHERE object_id = ? AND branch_name = ?",
          )
          .all(objectId, branchName) as Array<{
          commit_id: string;
          parents_json: string;
          content: Buffer;
          timestamp: number;
          author: string;
          metadata_json: string | null;
        }>;

        for (const row of rows) {
          commits[row.commit_id] = {
            parents: JSON.parse(row.parents_json),
            content: new Uint8Array(row.content),
            timestamp: row.timestamp,
            author: row.author,
            metadata: row.metadata_json ? JSON.parse(row.metadata_json) : undefined,
          };
        }
      }

      const branch: LoadedBranch = {
        tips,
        tails,
        commits,
      };

      return {
        type: "LoadObjectBranch",
        object_id: objectId,
        branch_name: branchName,
        branch,
      };
    } catch (e) {
      return {
        type: "LoadObjectBranch",
        object_id: objectId,
        branch_name: branchName,
        error: String(e),
      };
    }
  }

  private storeBlob(contentHash: string, data: Uint8Array): StorageResponse {
    try {
      const stmt = this.db.prepare(
        "INSERT OR REPLACE INTO blobs (content_hash, data) VALUES (?, ?)",
      );
      stmt.run(contentHash, data);
      return { type: "StoreBlob", content_hash: contentHash, success: true };
    } catch (e) {
      return {
        type: "StoreBlob",
        content_hash: contentHash,
        success: false,
        error: String(e),
      };
    }
  }

  private loadBlob(contentHash: string): StorageResponse {
    try {
      const row = this.db
        .prepare("SELECT data FROM blobs WHERE content_hash = ?")
        .get(contentHash) as { data: Buffer } | undefined;

      if (!row) {
        return {
          type: "LoadBlob",
          content_hash: contentHash,
          error: "NotFound",
        };
      }

      return {
        type: "LoadBlob",
        content_hash: contentHash,
        data: new Uint8Array(row.data),
      };
    } catch (e) {
      return {
        type: "LoadBlob",
        content_hash: contentHash,
        error: String(e),
      };
    }
  }

  private associateBlob(
    contentHash: string,
    objectId: string,
    branchName: string,
    commitId: string,
  ): StorageResponse {
    try {
      const stmt = this.db.prepare(
        "INSERT OR IGNORE INTO blob_associations (content_hash, object_id, branch_name, commit_id) VALUES (?, ?, ?, ?)",
      );
      stmt.run(contentHash, objectId, branchName, commitId);
      return { type: "AssociateBlob", content_hash: contentHash, success: true };
    } catch (e) {
      return {
        type: "AssociateBlob",
        content_hash: contentHash,
        success: false,
        error: String(e),
      };
    }
  }

  private loadBlobAssociations(contentHash: string): StorageResponse {
    try {
      const rows = this.db
        .prepare(
          "SELECT object_id, branch_name, commit_id FROM blob_associations WHERE content_hash = ?",
        )
        .all(contentHash) as Array<{
        object_id: string;
        branch_name: string;
        commit_id: string;
      }>;

      if (rows.length === 0) {
        return {
          type: "LoadBlobAssociations",
          content_hash: contentHash,
          error: "NotFound",
        };
      }

      const associations: BlobAssociation[] = rows.map((row) => ({
        object_id: row.object_id,
        branch_name: row.branch_name,
        commit_id: row.commit_id,
      }));

      return {
        type: "LoadBlobAssociations",
        content_hash: contentHash,
        associations,
      };
    } catch (e) {
      return {
        type: "LoadBlobAssociations",
        content_hash: contentHash,
        error: String(e),
      };
    }
  }

  private deleteCommit(objectId: string, branchName: string, commitId: string): StorageResponse {
    try {
      const stmt = this.db.prepare(
        "DELETE FROM commits WHERE object_id = ? AND branch_name = ? AND commit_id = ?",
      );
      stmt.run(objectId, branchName, commitId);

      // Remove from tips if present
      const tips = this.getBranchTips(objectId, branchName);
      tips.delete(commitId);
      this.saveBranchTips(objectId, branchName, tips);

      return {
        type: "DeleteCommit",
        object_id: objectId,
        branch_name: branchName,
        commit_id: commitId,
        success: true,
      };
    } catch (e) {
      return {
        type: "DeleteCommit",
        object_id: objectId,
        branch_name: branchName,
        commit_id: commitId,
        success: false,
        error: String(e),
      };
    }
  }

  private dissociateAndMaybeDeleteBlob(
    contentHash: string,
    objectId: string,
    branchName: string,
    commitId: string,
  ): StorageResponse {
    try {
      // Remove association
      this.db
        .prepare(
          "DELETE FROM blob_associations WHERE content_hash = ? AND object_id = ? AND branch_name = ? AND commit_id = ?",
        )
        .run(contentHash, objectId, branchName, commitId);

      // Check if any associations remain
      const count = this.db
        .prepare("SELECT COUNT(*) as count FROM blob_associations WHERE content_hash = ?")
        .get(contentHash) as { count: number };

      let blobDeleted = false;
      if (count.count === 0) {
        // No associations remain, delete the blob
        this.db.prepare("DELETE FROM blobs WHERE content_hash = ?").run(contentHash);
        blobDeleted = true;
      }

      return {
        type: "DissociateAndMaybeDeleteBlob",
        content_hash: contentHash,
        object_id: objectId,
        branch_name: branchName,
        commit_id: commitId,
        blob_deleted: blobDeleted,
      };
    } catch (e) {
      return {
        type: "DissociateAndMaybeDeleteBlob",
        content_hash: contentHash,
        object_id: objectId,
        branch_name: branchName,
        commit_id: commitId,
        error: String(e),
      };
    }
  }

  private setBranchTails(objectId: string, branchName: string, tails?: string[]): StorageResponse {
    try {
      const stmt = this.db.prepare(`
        UPDATE branch_tips SET tails_json = ? WHERE object_id = ? AND branch_name = ?
      `);
      stmt.run(tails ? JSON.stringify(tails) : null, objectId, branchName);
      return {
        type: "SetBranchTails",
        object_id: objectId,
        branch_name: branchName,
        success: true,
      };
    } catch (e) {
      return {
        type: "SetBranchTails",
        object_id: objectId,
        branch_name: branchName,
        success: false,
        error: String(e),
      };
    }
  }

  private loadIndexPage(table: string, column: string, pageId: number): StorageResponse {
    try {
      const row = this.db
        .prepare(
          "SELECT data FROM index_pages WHERE table_name = ? AND column_name = ? AND page_id = ?",
        )
        .get(table, column, pageId) as { data: Buffer } | undefined;

      return {
        type: "LoadIndexPage",
        table,
        column,
        page_id: pageId,
        data: row ? new Uint8Array(row.data) : undefined,
      };
    } catch (e) {
      return {
        type: "LoadIndexPage",
        table,
        column,
        page_id: pageId,
        error: String(e),
      };
    }
  }

  private storeIndexPage(
    table: string,
    column: string,
    pageId: number,
    data: Uint8Array,
  ): StorageResponse {
    try {
      const stmt = this.db.prepare(
        "INSERT OR REPLACE INTO index_pages (table_name, column_name, page_id, data) VALUES (?, ?, ?, ?)",
      );
      stmt.run(table, column, pageId, data);
      return {
        type: "StoreIndexPage",
        table,
        column,
        page_id: pageId,
        success: true,
      };
    } catch (e) {
      return {
        type: "StoreIndexPage",
        table,
        column,
        page_id: pageId,
        success: false,
        error: String(e),
      };
    }
  }

  private deleteIndexPage(table: string, column: string, pageId: number): StorageResponse {
    try {
      this.db
        .prepare("DELETE FROM index_pages WHERE table_name = ? AND column_name = ? AND page_id = ?")
        .run(table, column, pageId);
      return {
        type: "DeleteIndexPage",
        table,
        column,
        page_id: pageId,
        success: true,
      };
    } catch (e) {
      return {
        type: "DeleteIndexPage",
        table,
        column,
        page_id: pageId,
        success: false,
        error: String(e),
      };
    }
  }

  private loadIndexMeta(table: string, column: string): StorageResponse {
    try {
      const row = this.db
        .prepare("SELECT data FROM index_meta WHERE table_name = ? AND column_name = ?")
        .get(table, column) as { data: Buffer } | undefined;

      return {
        type: "LoadIndexMeta",
        table,
        column,
        data: row ? new Uint8Array(row.data) : undefined,
      };
    } catch (e) {
      return {
        type: "LoadIndexMeta",
        table,
        column,
        error: String(e),
      };
    }
  }

  private storeIndexMeta(table: string, column: string, data: Uint8Array): StorageResponse {
    try {
      const stmt = this.db.prepare(
        "INSERT OR REPLACE INTO index_meta (table_name, column_name, data) VALUES (?, ?, ?)",
      );
      stmt.run(table, column, data);
      return {
        type: "StoreIndexMeta",
        table,
        column,
        success: true,
      };
    } catch (e) {
      return {
        type: "StoreIndexMeta",
        table,
        column,
        success: false,
        error: String(e),
      };
    }
  }

  async close(): Promise<void> {
    this.db.close();
  }

  // Helper methods

  private getBranchTips(objectId: string, branchName: string): Set<string> {
    const row = this.db
      .prepare("SELECT tips_json FROM branch_tips WHERE object_id = ? AND branch_name = ?")
      .get(objectId, branchName) as { tips_json: string } | undefined;

    if (!row) {
      return new Set();
    }
    return new Set(JSON.parse(row.tips_json) as string[]);
  }

  private saveBranchTips(objectId: string, branchName: string, tips: Set<string>): void {
    const stmt = this.db.prepare(`
      INSERT OR REPLACE INTO branch_tips (object_id, branch_name, tips_json, tails_json)
      VALUES (?, ?, ?, (SELECT tails_json FROM branch_tips WHERE object_id = ? AND branch_name = ?))
    `);
    stmt.run(objectId, branchName, JSON.stringify([...tips]), objectId, branchName);
  }

  private loadCommit(objectId: string, branchName: string, commitId: string): Commit | undefined {
    const row = this.db
      .prepare(
        "SELECT parents_json, content, timestamp, author, metadata_json FROM commits WHERE object_id = ? AND branch_name = ? AND commit_id = ?",
      )
      .get(objectId, branchName, commitId) as
      | {
          parents_json: string;
          content: Buffer;
          timestamp: number;
          author: string;
          metadata_json: string | null;
        }
      | undefined;

    if (!row) {
      return undefined;
    }

    return {
      parents: JSON.parse(row.parents_json),
      content: new Uint8Array(row.content),
      timestamp: row.timestamp,
      author: row.author,
      metadata: row.metadata_json ? JSON.parse(row.metadata_json) : undefined,
    };
  }

  /**
   * Compute commit ID using BLAKE3, matching the Rust implementation.
   *
   * Serialization format (all integers are little-endian):
   * - parents count (u64)
   * - for each parent: 32 bytes
   * - content length (u64)
   * - content bytes
   * - timestamp (u64)
   * - author UUID (16 bytes)
   * - metadata presence marker (1 byte: 0 or 1)
   * - if metadata present:
   *   - entry count (u64)
   *   - for each entry (sorted by key):
   *     - key length (u64)
   *     - key bytes
   *     - value length (u64)
   *     - value bytes
   */
  private computeCommitId(commit: Commit): string {
    const hasher = createHash();

    // Helper to write u64 little-endian
    const writeU64 = (n: number | bigint) => {
      const buf = Buffer.alloc(8);
      buf.writeBigUInt64LE(BigInt(n));
      hasher.update(buf);
    };

    // Hash parents
    writeU64(commit.parents.length);
    for (const parent of commit.parents) {
      // Parent is a 64-char hex string representing 32 bytes
      hasher.update(Buffer.from(parent, "hex"));
    }

    // Hash content
    writeU64(commit.content.length);
    hasher.update(commit.content);

    // Hash timestamp
    writeU64(commit.timestamp);

    // Hash author (UUID as 16 bytes)
    // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    const authorHex = commit.author.replace(/-/g, "");
    hasher.update(Buffer.from(authorHex, "hex"));

    // Hash metadata
    if (commit.metadata && Object.keys(commit.metadata).length > 0) {
      hasher.update(Buffer.from([1])); // presence marker
      const entries = Object.entries(commit.metadata).sort(([a], [b]) => a.localeCompare(b));
      writeU64(entries.length);
      for (const [key, value] of entries) {
        const keyBytes = Buffer.from(key, "utf-8");
        writeU64(keyBytes.length);
        hasher.update(keyBytes);
        const valueBytes = Buffer.from(value, "utf-8");
        writeU64(valueBytes.length);
        hasher.update(valueBytes);
      }
    } else {
      hasher.update(Buffer.from([0])); // absence marker
    }

    // Finalize and return as hex string
    return hasher.digest("hex");
  }
}
