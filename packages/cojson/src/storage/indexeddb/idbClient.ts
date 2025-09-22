import {
  putIndexedDbStore,
  queryIndexedDbStore,
  queryLastValue,
} from "./CoJsonIDBTransaction.js";
import type { StoredCoValueChunk, FirstCoValueBlock } from "../storageIDB.js";

export type CoValuesStoredBlock = {
  id: string[];
  values: Record<string, FirstCoValueBlock>;
  size: number;
};

export class IDBClient {
  private db;

  activeTransaction: IDBTransaction | undefined;

  blocks = new Map<string, CoValuesStoredBlock>();
  freeBlock: CoValuesStoredBlock | undefined;

  constructor(db: IDBDatabase) {
    this.db = db;
  }

  async getChunk(
    coValueId: string,
    block: number,
  ): Promise<StoredCoValueChunk | undefined> {
    return queryIndexedDbStore(this.db, "chunks", (store) =>
      store.get([coValueId, block]),
    );
  }

  async getBlock(coValueId: string): Promise<CoValuesStoredBlock | undefined> {
    const cachedBlock = this.blocks.get(coValueId);
    if (cachedBlock) {
      return cachedBlock;
    }

    const block = await queryIndexedDbStore<CoValuesStoredBlock>(
      this.db,
      "coValueBlocks",
      (store) => store.index("coValueBlocksByID").get(coValueId),
    );

    if (!block) {
      return undefined;
    }

    for (const id of block.id) {
      this.blocks.set(id, block);
    }

    return block;
  }

  async getFreeBlock(): Promise<CoValuesStoredBlock> {
    const freeBlock =
      this.freeBlock ||
      (await queryLastValue<CoValuesStoredBlock>(this.db, "coValueBlocks"));
    if (freeBlock && freeBlock.size < 20 * 1024) {
      this.freeBlock = freeBlock;
      return freeBlock;
    }

    this.freeBlock = undefined;
    return {
      id: [],
      values: {},
      size: 0,
    };
  }

  async storeBlock(block: CoValuesStoredBlock): Promise<void> {
    await putIndexedDbStore(this.db, "coValueBlocks", block);
  }

  async storeChunk(block: StoredCoValueChunk): Promise<void> {
    await putIndexedDbStore(this.db, "chunks", block);
  }
}
