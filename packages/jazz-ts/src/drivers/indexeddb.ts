/**
 * IndexedDB storage driver for browser environments.
 *
 * Object stores:
 * - objects: { objectId, metadata }
 * - commits: { objectId, branchName, commitId, commit }
 * - branchTips: { objectId, branchName, tips, tails }
 * - blobs: { contentHash, data }
 * - blobAssociations: { contentHash, objectId, branchName, commitId }
 * - indexPages: { table, column, pageId, data }
 * - indexMeta: { table, column, data }
 */

import type {
  StorageDriver,
  StorageRequest,
  StorageResponse,
  Commit,
  LoadedBranch,
  BlobAssociation,
} from "./types.js";

const DB_NAME = "jazz-groove";
const DB_VERSION = 1;

const STORES = {
  OBJECTS: "objects",
  COMMITS: "commits",
  BRANCH_TIPS: "branchTips",
  BLOBS: "blobs",
  BLOB_ASSOCIATIONS: "blobAssociations",
  INDEX_PAGES: "indexPages",
  INDEX_META: "indexMeta",
} as const;

/**
 * IndexedDB storage driver for browser environments.
 */
export class IndexedDBDriver implements StorageDriver {
  private db: IDBDatabase | null = null;
  private dbName: string;

  private constructor(dbName: string) {
    this.dbName = dbName;
  }

  /**
   * Open an IndexedDB driver.
   *
   * @param dbName Database name (default: "jazz-groove")
   */
  static async open(dbName: string = DB_NAME): Promise<IndexedDBDriver> {
    const driver = new IndexedDBDriver(dbName);
    await driver.initDB();
    return driver;
  }

  private initDB(): Promise<void> {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(this.dbName, DB_VERSION);

      request.onerror = () => {
        reject(new Error(`Failed to open IndexedDB: ${request.error?.message}`));
      };

      request.onsuccess = () => {
        this.db = request.result;
        resolve();
      };

      request.onupgradeneeded = (event) => {
        const db = (event.target as IDBOpenDBRequest).result;

        // Objects store
        if (!db.objectStoreNames.contains(STORES.OBJECTS)) {
          db.createObjectStore(STORES.OBJECTS, { keyPath: "objectId" });
        }

        // Commits store with compound key
        if (!db.objectStoreNames.contains(STORES.COMMITS)) {
          const store = db.createObjectStore(STORES.COMMITS, {
            keyPath: ["objectId", "branchName", "commitId"],
          });
          store.createIndex("byObjectBranch", ["objectId", "branchName"]);
        }

        // Branch tips store
        if (!db.objectStoreNames.contains(STORES.BRANCH_TIPS)) {
          db.createObjectStore(STORES.BRANCH_TIPS, {
            keyPath: ["objectId", "branchName"],
          });
        }

        // Blobs store
        if (!db.objectStoreNames.contains(STORES.BLOBS)) {
          db.createObjectStore(STORES.BLOBS, { keyPath: "contentHash" });
        }

        // Blob associations store
        if (!db.objectStoreNames.contains(STORES.BLOB_ASSOCIATIONS)) {
          const store = db.createObjectStore(STORES.BLOB_ASSOCIATIONS, {
            keyPath: ["contentHash", "objectId", "branchName", "commitId"],
          });
          store.createIndex("byContentHash", "contentHash");
        }

        // Index pages store
        if (!db.objectStoreNames.contains(STORES.INDEX_PAGES)) {
          db.createObjectStore(STORES.INDEX_PAGES, {
            keyPath: ["table", "column", "pageId"],
          });
        }

        // Index meta store
        if (!db.objectStoreNames.contains(STORES.INDEX_META)) {
          db.createObjectStore(STORES.INDEX_META, {
            keyPath: ["table", "column"],
          });
        }
      };
    });
  }

  async process(requests: StorageRequest[]): Promise<StorageResponse[]> {
    const responses: StorageResponse[] = [];
    for (const req of requests) {
      const response = await this.processOne(req);
      responses.push(response);
    }
    return responses;
  }

  private async processOne(req: StorageRequest): Promise<StorageResponse> {
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

  private tx(storeNames: string | string[], mode: IDBTransactionMode = "readonly"): IDBTransaction {
    if (!this.db) {
      throw new Error("Database not initialized");
    }
    return this.db.transaction(storeNames, mode);
  }

  private promisify<T>(request: IDBRequest<T>): Promise<T> {
    return new Promise((resolve, reject) => {
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
  }

  private async createObject(
    id: string,
    metadata: Record<string, string>,
  ): Promise<StorageResponse> {
    try {
      const tx = this.tx(STORES.OBJECTS, "readwrite");
      const store = tx.objectStore(STORES.OBJECTS);
      await this.promisify(store.put({ objectId: id, metadata }));
      return { type: "CreateObject", id, success: true };
    } catch (e) {
      return { type: "CreateObject", id, success: false, error: String(e) };
    }
  }

  private async appendCommit(
    objectId: string,
    branchName: string,
    commit: Commit,
  ): Promise<StorageResponse> {
    try {
      const commitId = this.computeCommitId(commit);

      const tx = this.tx([STORES.COMMITS, STORES.BRANCH_TIPS], "readwrite");
      const commitStore = tx.objectStore(STORES.COMMITS);
      const tipsStore = tx.objectStore(STORES.BRANCH_TIPS);

      // Store commit
      await this.promisify(
        commitStore.put({
          objectId,
          branchName,
          commitId,
          commit,
        }),
      );

      // Update tips
      const tipsKey = [objectId, branchName];
      const existingTips = await this.promisify(tipsStore.get(tipsKey));
      let tips: Set<string> = new Set(existingTips?.tips ?? []);

      // Remove parents from tips
      for (const parent of commit.parents) {
        tips.delete(parent);
      }
      // Add new commit as tip
      tips.add(commitId);

      await this.promisify(
        tipsStore.put({
          objectId,
          branchName,
          tips: [...tips],
          tails: existingTips?.tails,
        }),
      );

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

  private async loadObjectBranch(
    objectId: string,
    branchName: string,
    depth: "TipIdsOnly" | "TipsOnly" | "AllCommits",
  ): Promise<StorageResponse> {
    try {
      const tx = this.tx([STORES.COMMITS, STORES.BRANCH_TIPS], "readonly");
      const tipsStore = tx.objectStore(STORES.BRANCH_TIPS);

      const tipsRecord = await this.promisify(tipsStore.get([objectId, branchName]));

      if (!tipsRecord) {
        return {
          type: "LoadObjectBranch",
          object_id: objectId,
          branch_name: branchName,
          error: "NotFound",
        };
      }

      const tips: string[] = tipsRecord.tips ?? [];
      const tails: string[] | undefined = tipsRecord.tails;
      let commits: Record<string, Commit> = {};

      if (depth === "TipsOnly") {
        const commitStore = tx.objectStore(STORES.COMMITS);
        for (const tipId of tips) {
          const record = await this.promisify(commitStore.get([objectId, branchName, tipId]));
          if (record) {
            commits[tipId] = record.commit;
          }
        }
      } else if (depth === "AllCommits") {
        const commitStore = tx.objectStore(STORES.COMMITS);
        const index = commitStore.index("byObjectBranch");
        const records = await this.promisify(index.getAll([objectId, branchName]));
        for (const record of records) {
          commits[record.commitId] = record.commit;
        }
      }

      const branch: LoadedBranch = { tips, tails, commits };

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

  private async storeBlob(contentHash: string, data: Uint8Array): Promise<StorageResponse> {
    try {
      const tx = this.tx(STORES.BLOBS, "readwrite");
      const store = tx.objectStore(STORES.BLOBS);
      await this.promisify(store.put({ contentHash, data }));
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

  private async loadBlob(contentHash: string): Promise<StorageResponse> {
    try {
      const tx = this.tx(STORES.BLOBS, "readonly");
      const store = tx.objectStore(STORES.BLOBS);
      const record = await this.promisify(store.get(contentHash));

      if (!record) {
        return {
          type: "LoadBlob",
          content_hash: contentHash,
          error: "NotFound",
        };
      }

      return {
        type: "LoadBlob",
        content_hash: contentHash,
        data: record.data,
      };
    } catch (e) {
      return {
        type: "LoadBlob",
        content_hash: contentHash,
        error: String(e),
      };
    }
  }

  private async associateBlob(
    contentHash: string,
    objectId: string,
    branchName: string,
    commitId: string,
  ): Promise<StorageResponse> {
    try {
      const tx = this.tx(STORES.BLOB_ASSOCIATIONS, "readwrite");
      const store = tx.objectStore(STORES.BLOB_ASSOCIATIONS);
      await this.promisify(store.put({ contentHash, objectId, branchName, commitId }));
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

  private async loadBlobAssociations(contentHash: string): Promise<StorageResponse> {
    try {
      const tx = this.tx(STORES.BLOB_ASSOCIATIONS, "readonly");
      const store = tx.objectStore(STORES.BLOB_ASSOCIATIONS);
      const index = store.index("byContentHash");
      const records = await this.promisify(index.getAll(contentHash));

      if (records.length === 0) {
        return {
          type: "LoadBlobAssociations",
          content_hash: contentHash,
          error: "NotFound",
        };
      }

      const associations: BlobAssociation[] = records.map((r) => ({
        object_id: r.objectId,
        branch_name: r.branchName,
        commit_id: r.commitId,
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

  private async deleteCommit(
    objectId: string,
    branchName: string,
    commitId: string,
  ): Promise<StorageResponse> {
    try {
      const tx = this.tx([STORES.COMMITS, STORES.BRANCH_TIPS], "readwrite");
      const commitStore = tx.objectStore(STORES.COMMITS);
      const tipsStore = tx.objectStore(STORES.BRANCH_TIPS);

      await this.promisify(commitStore.delete([objectId, branchName, commitId]));

      // Remove from tips if present
      const tipsRecord = await this.promisify(tipsStore.get([objectId, branchName]));
      if (tipsRecord) {
        const tips = new Set<string>(tipsRecord.tips ?? []);
        tips.delete(commitId);
        await this.promisify(
          tipsStore.put({
            ...tipsRecord,
            tips: [...tips],
          }),
        );
      }

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

  private async dissociateAndMaybeDeleteBlob(
    contentHash: string,
    objectId: string,
    branchName: string,
    commitId: string,
  ): Promise<StorageResponse> {
    try {
      const tx = this.tx([STORES.BLOB_ASSOCIATIONS, STORES.BLOBS], "readwrite");
      const assocStore = tx.objectStore(STORES.BLOB_ASSOCIATIONS);
      const blobStore = tx.objectStore(STORES.BLOBS);

      // Remove association
      await this.promisify(assocStore.delete([contentHash, objectId, branchName, commitId]));

      // Check if any associations remain
      const index = assocStore.index("byContentHash");
      const remaining = await this.promisify(index.count(contentHash));

      let blobDeleted = false;
      if (remaining === 0) {
        await this.promisify(blobStore.delete(contentHash));
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

  private async setBranchTails(
    objectId: string,
    branchName: string,
    tails?: string[],
  ): Promise<StorageResponse> {
    try {
      const tx = this.tx(STORES.BRANCH_TIPS, "readwrite");
      const store = tx.objectStore(STORES.BRANCH_TIPS);

      const existing = await this.promisify(store.get([objectId, branchName]));

      if (existing) {
        await this.promisify(
          store.put({
            ...existing,
            tails,
          }),
        );
      }

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

  private async loadIndexPage(
    table: string,
    column: string,
    pageId: number,
  ): Promise<StorageResponse> {
    try {
      const tx = this.tx(STORES.INDEX_PAGES, "readonly");
      const store = tx.objectStore(STORES.INDEX_PAGES);
      const record = await this.promisify(store.get([table, column, pageId]));

      return {
        type: "LoadIndexPage",
        table,
        column,
        page_id: pageId,
        data: record?.data,
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

  private async storeIndexPage(
    table: string,
    column: string,
    pageId: number,
    data: Uint8Array,
  ): Promise<StorageResponse> {
    try {
      const tx = this.tx(STORES.INDEX_PAGES, "readwrite");
      const store = tx.objectStore(STORES.INDEX_PAGES);
      await this.promisify(store.put({ table, column, pageId, data }));

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

  private async deleteIndexPage(
    table: string,
    column: string,
    pageId: number,
  ): Promise<StorageResponse> {
    try {
      const tx = this.tx(STORES.INDEX_PAGES, "readwrite");
      const store = tx.objectStore(STORES.INDEX_PAGES);
      await this.promisify(store.delete([table, column, pageId]));

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

  private async loadIndexMeta(table: string, column: string): Promise<StorageResponse> {
    try {
      const tx = this.tx(STORES.INDEX_META, "readonly");
      const store = tx.objectStore(STORES.INDEX_META);
      const record = await this.promisify(store.get([table, column]));

      return {
        type: "LoadIndexMeta",
        table,
        column,
        data: record?.data,
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

  private async storeIndexMeta(
    table: string,
    column: string,
    data: Uint8Array,
  ): Promise<StorageResponse> {
    try {
      const tx = this.tx(STORES.INDEX_META, "readwrite");
      const store = tx.objectStore(STORES.INDEX_META);
      await this.promisify(store.put({ table, column, data }));

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
    if (this.db) {
      this.db.close();
      this.db = null;
    }
  }

  private computeCommitId(commit: Commit): string {
    // Simplified commit ID computation
    // In the real implementation, this would match the Rust BLAKE3 hash
    const data = JSON.stringify({
      parents: commit.parents,
      content: Array.from(commit.content),
      timestamp: commit.timestamp,
      author: commit.author,
      metadata: commit.metadata,
    });

    // Simple hash for now - in production, use BLAKE3
    let hash = 0;
    for (let i = 0; i < data.length; i++) {
      const char = data.charCodeAt(i);
      hash = (hash << 5) - hash + char;
      hash = hash & hash;
    }

    return Math.abs(hash).toString(16).padStart(64, "0");
  }
}
