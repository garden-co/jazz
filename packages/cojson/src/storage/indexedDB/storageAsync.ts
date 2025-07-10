import type { CoValueHeader } from "../../coValueCore/verifiedState.js";
import {
  type CoValueCore,
  type RawCoID,
  type SessionID,
  type StorageAPI,
  logger,
} from "../../exports.js";
import {
  type CoValueKnownState,
  type NewContentMessage,
  emptyKnownState,
} from "../../sync.js";
import { StoreQueue } from "../StoreQueue.js";
import { StorageKnownState } from "../knownState.js";
import { getDependedOnCoValues } from "../syncUtils.js";
import type { IDBDriver } from "./getIndexedDBStorage.js";

export class IndexedDBStorage implements StorageAPI {
  private readonly dbClient: IDBDriver;

  private loadedCoValues = new Set<RawCoID>();

  constructor(dbClient: IDBDriver) {
    this.dbClient = dbClient;
  }

  knwonStates = new StorageKnownState();

  getKnownState(id: string): CoValueKnownState {
    return this.knwonStates.getKnownState(id);
  }

  async load(
    id: string,
    callback: (data: NewContentMessage) => void,
    done: (found: boolean) => void,
  ) {
    await this.loadCoValue(id, callback, done);
  }

  async loadCoValue(
    id: string,
    callback: (data: NewContentMessage) => void,
    done: (found: boolean) => void,
  ) {
    const coValueContent = await this.dbClient.getCoValue(id, 0);

    if (!coValueContent) {
      done?.(false);
      return;
    }

    const contentStreaming = coValueContent.lastIndex !== undefined;

    const knownState =
      coValueContent.knownState || this.knwonStates.getKnownState(id);
    this.knwonStates.handleUpdate(coValueContent.id, knownState);

    this.loadedCoValues.add(coValueContent.id as RawCoID);

    const contentMessage = coValueContent.content;

    if (contentStreaming) {
      contentMessage.expectContentUntil = knownState.sessions;
    }

    const header = coValueContent.content.header;

    if (!header) {
      logger.error("CoValue header is missing", { id });
      done?.(false);
      return;
    }

    this.pushContentWithDependencies(header, contentMessage, callback);

    for (let idx = 1; idx <= (coValueContent.lastIndex ?? 0); idx++) {
      const data = await this.dbClient.getCoValue(id, idx);

      if (!data) {
        logger.error("CoValue content is missing", { id, idx });
        return;
      }

      const contentMessage = data.content;

      this.pushContentWithDependencies(header, contentMessage, callback);
    }

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

    await Promise.all(promises);

    pushCallback(contentMessage);
  }

  storeQueue = new StoreQueue();

  async store(
    msgs: NewContentMessage[],
    correctionCallback: (data: CoValueKnownState) => void,
  ) {
    /**
     * The store operations must be done one by one, because we can't start a new transaction when there
     * is already a transaction open.
     */
    this.storeQueue.push(msgs, correctionCallback);

    this.storeQueue.processQueue(async (data, correctionCallback) => {
      for (const msg of data) {
        const success = await this.storeSingle(msg, correctionCallback);

        if (!success) {
          // Stop processing the messages for this entry, because the data is out of sync with storage
          // and the other transactions will be rejected anyway.
          break;
        }
      }
    });
  }

  private async storeSingle(
    msg: NewContentMessage,
    correctionCallback: (data: CoValueKnownState) => void,
  ): Promise<boolean> {
    const id = msg.id;
    const coValueContent = await this.dbClient.getCoValue(id, 0);

    // We have no info about coValue header
    const invalidAssumptionOnHeaderPresence = !msg.header && !coValueContent;

    if (invalidAssumptionOnHeaderPresence) {
      const knownState = emptyKnownState(id as RawCoID);
      this.knwonStates.setKnownState(id, knownState);

      correctionCallback(knownState);
      return false;
    }

    const knownState =
      coValueContent?.knownState || this.knwonStates.getKnownState(id);
    knownState.header = true;
    const sessions = getSessionCounters(msg);

    if (!coValueContent) {
      for (const [sessionID, counter] of sessions) {
        if (counter > (knownState.sessions[sessionID] ?? 0)) {
          knownState.sessions[sessionID] = counter;
        }
      }

      await this.dbClient.storeCoValue({
        id,
        index: 0,
        content: msg,
        knownState,
      });

      this.knwonStates.handleUpdate(id, knownState);

      return true;
    }

    let invalidAssumptions = false;

    for (const sessionID of Object.keys(msg.new) as SessionID[]) {
      const session = msg.new[sessionID];

      if (!session) {
        continue;
      }

      const lastIdx = knownState.sessions[sessionID] ?? 0;

      if (lastIdx < session.after) {
        invalidAssumptions = true;
        continue;
      }

      const currentSessionContent = coValueContent.content.new[sessionID];

      // TODO Implement streaming!
      if (!currentSessionContent) {
        coValueContent.content.new[sessionID] = session;
      } else {
        const delta =
          currentSessionContent.after +
          currentSessionContent.newTransactions.length -
          session.after;

        const newTransactions = session.newTransactions.slice(delta);

        if (newTransactions.length === 0) {
          continue;
        }

        for (const newTransaction of newTransactions) {
          currentSessionContent.newTransactions.push(newTransaction);
        }
        currentSessionContent.lastSignature = session.lastSignature;
      }

      knownState.sessions[sessionID] =
        session.after + session.newTransactions.length;
    }

    this.knwonStates.handleUpdate(id, knownState);

    await this.dbClient.storeCoValue({
      id,
      index: coValueContent.index,
      content: coValueContent.content,
      knownState,
    });

    if (invalidAssumptions) {
      correctionCallback(knownState);
      return false;
    }

    return true;
  }

  waitForSync(id: string, coValue: CoValueCore) {
    return this.knwonStates.waitForSync(id, coValue);
  }

  close() {
    // Drain the store queue
    this.storeQueue.drain();
  }
}

function getSessionCounters(msg: NewContentMessage) {
  const sessions: [SessionID, number][] = [];

  for (const sessionID of Object.keys(msg.new) as SessionID[]) {
    const session = msg.new[sessionID];

    if (!session) {
      continue;
    }

    sessions.push([sessionID, session.after + session.newTransactions.length]);
  }

  return sessions;
}
