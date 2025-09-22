import {
  createContentMessage,
  exceedsRecommendedSize,
} from "../coValueContentMessage.js";
import { CoValueHeader } from "../coValueCore/verifiedState.js";
import {
  type CoValueCore,
  type RawCoID,
  type SessionID,
  type StorageAPI,
  logger,
} from "../exports.js";
import { CoValuePriority, getPriorityFromHeader } from "../priority.js";
import { Semaphore } from "../queue/Semaphore.js";
import { StoreQueue } from "../queue/StoreQueue.js";
import {
  CoValueKnownState,
  NewContentMessage,
  SessionNewContent,
  emptyKnownState,
} from "../sync.js";
import { CoValuesStoredBlock, IDBClient } from "./indexeddb/idbClient.js";
import { StorageKnownState } from "./knownState.js";
import {
  collectNewTxs,
  getDependedOnCoValues,
  getNewTransactionsSize,
} from "./syncUtils.js";
import type {
  CorrectionCallback,
  DBClientInterfaceAsync,
  SignatureAfterRow,
  StoredCoValueRow,
  StoredSessionRow,
} from "./types.js";

export interface FirstCoValueBlock {
  id: `co_z${string}`;
  header: CoValueHeader;
  new: { [sessionID: SessionID]: SessionNewContent };
  size: number;
  position: 0;
  sessions: { [sessionID: SessionID]: number };
  lastBlock: number;
  lastBlockSize: number;
}

export interface StoredCoValueChunk {
  id: `co_z${string}`;
  header?: CoValueHeader;
  new: { [sessionID: SessionID]: SessionNewContent };
  size: number;
  position: number;
  sessions?: { [sessionID: SessionID]: number };
  lastBlock?: number;
  lastBlockSize?: number;
}

export class IndexedDBStorageApi implements StorageAPI {
  private readonly dbClient: IDBClient;

  private loadedCoValues = new Set<RawCoID>();
  private semaphore = new Semaphore(10);

  constructor(dbClient: IDBClient) {
    this.dbClient = dbClient;
  }

  knwonStates = new StorageKnownState();

  getKnownState(id: string): CoValueKnownState {
    const knownState = this.knwonStates.getKnownState(id);

    if (!knownState) {
      return emptyKnownState(id as RawCoID);
    }

    return knownState;
  }

  load(
    id: string,
    callback: (data: NewContentMessage) => void,
    done: (found: boolean) => void,
  ) {
    // Limit the parallel loading of coValues to balance the load on the database
    // and the data processing
    this.semaphore.acquire(() => {
      this.loadCoValue(id, callback, (found) => {
        this.semaphore.release();
        done(found);
      });
    });
  }

  async loadCoValue(
    id: string,
    callback: (data: NewContentMessage) => void,
    done: (found: boolean) => void,
  ) {
    const firstBlock = await this.dbClient.getBlock(id);

    const entry = firstBlock?.values[id];

    if (!entry) {
      done?.(false);
      return;
    }

    const contentStreaming = entry.lastBlock > 0;
    const response: NewContentMessage = {
      id: entry.id,
      header: entry.header,
      new: entry.new,
      priority: getPriorityFromHeader(entry.header),
      action: "content",
    };

    if (contentStreaming) {
      response.expectContentUntil = entry.sessions;
    }

    await this.pushContentWithDependencies(entry.header, response, callback);

    if (contentStreaming) {
      for (let i = 1; i <= entry.lastBlock; i++) {
        const chunk = await this.dbClient.getChunk(id, i);

        if (!chunk) {
          throw new Error("Block not found");
        }

        await this.pushContentWithDependencies(
          entry.header,
          {
            id: chunk.id,
            header: chunk.header,
            new: chunk.new,
            priority: getPriorityFromHeader(chunk.header),
            action: "content",
          },
          callback,
        );
      }
    }

    this.knwonStates.handleUpdate(id, {
      id: id as RawCoID,
      header: true,
      sessions: entry.sessions,
    });
    done?.(true);
  }

  async pushContentWithDependencies(
    header: CoValueHeader,
    contentMessage: NewContentMessage,
    pushCallback: (data: NewContentMessage) => void,
  ) {
    const dependedOnCoValuesList = getDependedOnCoValues(
      header,
      contentMessage,
    );

    const promises = [];

    for (const dependedOnCoValue of dependedOnCoValuesList) {
      if (this.loadedCoValues.has(dependedOnCoValue)) {
        continue;
      }

      promises.push(
        new Promise((resolve) => {
          this.loadCoValue(dependedOnCoValue, pushCallback, resolve);
        }),
      );
    }

    if (promises.length > 0) {
      await Promise.all(promises);
    }

    pushCallback(contentMessage);
  }

  storeQueue = new StoreQueue();

  async store(msg: NewContentMessage, correctionCallback: CorrectionCallback) {
    /**
     * The store operations must be done one by one, because we can't start a new transaction when there
     * is already a transaction open.
     */
    this.storeQueue.push(msg, correctionCallback);

    this.storeQueue.processQueue(async (data, correctionCallback) => {
      await this.storeSingle(data, correctionCallback);
    });
  }

  /**
   * This function is called when the storage lacks the information required to store the incoming content.
   *
   * It triggers a `correctionCallback` to ask the syncManager to provide the missing information.
   *
   * The correction is applied immediately, to ensure that, when applicable, the dependent content in the queue won't require additional corrections.
   */
  private async handleCorrection(
    knownState: CoValueKnownState,
    correctionCallback: CorrectionCallback,
  ) {
    const correction = correctionCallback(knownState);

    if (!correction) {
      logger.error("Correction callback returned undefined", {
        knownState,
        correction: correction ?? null,
      });
      return false;
    }

    for (const msg of correction) {
      const success = await this.storeSingle(msg, (knownState) => {
        logger.error("Double correction requested", {
          msg,
          knownState,
        });
        return undefined;
      });

      if (!success) {
        return false;
      }
    }

    return true;
  }

  private async storeSingle(
    msg: NewContentMessage,
    correctionCallback: CorrectionCallback,
  ): Promise<boolean> {
    if (this.storeQueue.closed) {
      return false;
    }

    const loadedBlock = await this.dbClient.getBlock(msg.id);
    const entry = loadedBlock?.values[msg.id];

    if (!entry && !msg.header) {
      return this.handleCorrection(
        emptyKnownState(msg.id as RawCoID),
        correctionCallback,
      );
    }

    const content: { [sessionID: SessionID]: SessionNewContent } = {};
    let size = 0;
    let invalidAssumptions = false;

    const knownSessions = entry?.sessions || {};

    for (const [sessionID, sessionNewContent] of Object.entries(msg.new) as [
      SessionID,
      SessionNewContent,
    ][]) {
      const after = knownSessions[sessionID] || 0;

      if (sessionNewContent.after > after) {
        invalidAssumptions = true;
        continue;
      }

      const actuallyNewOffset = after - sessionNewContent.after;
      const actuallyNewTransactions =
        sessionNewContent.newTransactions.slice(actuallyNewOffset);

      size += getNewTransactionsSize(actuallyNewTransactions);
      knownSessions[sessionID] = after + actuallyNewTransactions.length;

      content[sessionID] = {
        after,
        newTransactions: actuallyNewTransactions,
        lastSignature: sessionNewContent.lastSignature,
      };
    }

    const block = loadedBlock
      ? loadedBlock
      : await this.dbClient.getFreeBlock();

    if (!entry) {
      block.values[msg.id] = {
        id: msg.id,
        header: msg.header!,
        new: content,
        size,
        position: 0,
        sessions: knownSessions,
        lastBlock: 0,
        lastBlockSize: size,
      };
      block.id.push(msg.id);
      block.size += size;
      await this.dbClient.storeBlock(block);
    } else if (exceedsRecommendedSize(entry.lastBlockSize, size)) {
      const chunk = {
        id: msg.id,
        header: msg.header,
        new: content,
        size,
        position: entry.lastBlock + 1,
      };
      entry.lastBlock += 1;
      entry.lastBlockSize = size;
      entry.sessions = knownSessions;
      block.values[msg.id] = entry;
      await this.dbClient.storeChunk(chunk);
      await this.dbClient.storeBlock(block);
    } else if (entry.lastBlock === 0) {
      for (const [sessionID, sessionNewContent] of Object.entries(content) as [
        SessionID,
        SessionNewContent,
      ][]) {
        const blockContent = entry.new[sessionID];

        if (blockContent) {
          for (const tx of sessionNewContent.newTransactions) {
            blockContent.newTransactions.push(tx);
          }
          blockContent.lastSignature = sessionNewContent.lastSignature;
        } else {
          entry.new[sessionID] = sessionNewContent;
        }
      }

      block.size = size;
      entry.lastBlockSize += size;
      entry.size += size;
      entry.sessions = knownSessions;
      block.size += size;
      block.values[msg.id] = entry;
      await this.dbClient.storeBlock(block);
    } else if (entry.lastBlock > 0) {
      const chunk = await this.dbClient.getChunk(msg.id, entry.lastBlock);

      if (!chunk) {
        throw new Error("Chunk not found");
      }

      for (const [sessionID, sessionNewContent] of Object.entries(content) as [
        SessionID,
        SessionNewContent,
      ][]) {
        const entryContent = entry.new[sessionID];

        if (entryContent) {
          for (const tx of sessionNewContent.newTransactions) {
            entryContent.newTransactions.push(tx);
          }
          entryContent.lastSignature = sessionNewContent.lastSignature;
        } else {
          entry.new[sessionID] = sessionNewContent;
        }
      }
      entry.lastBlockSize += size;
      entry.size += size;
      entry.sessions = knownSessions;
      block.values[msg.id] = entry;

      await this.dbClient.storeBlock(block);
      await this.dbClient.storeChunk(chunk);
    }

    this.knwonStates.handleUpdate(msg.id, {
      id: msg.id as RawCoID,
      header: Boolean(entry?.header),
      sessions: knownSessions,
    });

    if (invalidAssumptions) {
      return this.handleCorrection(
        {
          id: msg.id as RawCoID,
          header: Boolean(entry?.header),
          sessions: knownSessions,
        },
        correctionCallback,
      );
    }

    return true;
  }

  waitForSync(id: string, coValue: CoValueCore) {
    return this.knwonStates.waitForSync(id, coValue);
  }

  close() {
    return this.storeQueue.close();
  }
}
