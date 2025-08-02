import { CorrectionCallback } from "../exports.js";
import { logger } from "../logger.js";
import { NewContentMessage } from "../sync.js";
import { LinkedList } from "./LinkedList.js";

type StoreQueueEntry = {
  data: NewContentMessage;
  correctionCallback: CorrectionCallback;
};

export class StoreQueue {
  private queue = new LinkedList<StoreQueueEntry>();
  closed = false;

  public push(data: NewContentMessage, correctionCallback: CorrectionCallback) {
    if (this.closed) {
      return;
    }

    this.queue.push({ data, correctionCallback });
  }

  public pull() {
    return this.queue.shift();
  }

  processing = false;
  lastCallback: Promise<unknown> | undefined;

  async processQueue(
    callback: (
      data: NewContentMessage,
      correctionCallback: CorrectionCallback,
    ) => Promise<unknown>,
  ) {
    if (this.processing) {
      return;
    }

    this.processing = true;

    let entry: StoreQueueEntry | undefined;

    while ((entry = this.pull())) {
      const { data, correctionCallback } = entry;

      try {
        this.lastCallback = callback(data, correctionCallback);
        await this.lastCallback;
      } catch (err) {
        logger.error("Error processing message in store queue", { err });
      }
    }

    this.lastCallback = undefined;
    this.processing = false;
  }

  close() {
    this.closed = true;

    while (this.pull()) {}

    return this.lastCallback;
  }
}
