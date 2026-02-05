import type { NewContentMessage } from "./sync.js";
import type { RawCoID } from "./ids.js";

/**
 * TransactionContext manages the state of an active atomic transaction.
 *
 * When a transaction is active, mutations are applied immediately to memory
 * but their NewContentMessages are buffered instead of being synced immediately.
 * After the transaction callback completes, all buffered messages are:
 * - Stored atomically in a single IndexedDB transaction
 * - Sent to sync servers in a single BatchMessage
 *
 * The presence of a TransactionContext is treated as the "active" signal.
 * The callback is synchronous, so no other code can run during the transaction window.
 */
export class TransactionContext {
  private pendingMessages: NewContentMessage[] = [];
  private pendingCoValues: Set<RawCoID> = new Set();

  /**
   * Buffer a NewContentMessage for later atomic persistence and sync.
   * Messages are stored in order and will be processed as a batch.
   */
  bufferMessage(msg: NewContentMessage): void {
    this.pendingMessages.push(msg);
    this.pendingCoValues.add(msg.id);
  }

  /**
   * Get all buffered messages in order.
   * The order is preserved per CoValue for correct replay.
   */
  getPendingMessages(): NewContentMessage[] {
    return this.pendingMessages;
  }

  /**
   * Get the set of unique CoValue IDs affected by this transaction.
   */
  getCoValueIds(): Set<RawCoID> {
    return this.pendingCoValues;
  }

  /**
   * Check if this transaction context is active.
   * A TransactionContext is always considered active when it exists.
   */
  isActive(): boolean {
    return true;
  }

  /**
   * Clear all buffered messages and CoValue IDs.
   * Called when the transaction is discarded (e.g., on error).
   */
  clear(): void {
    this.pendingMessages = [];
    this.pendingCoValues.clear();
  }

  /**
   * Get the count of buffered messages.
   */
  get messageCount(): number {
    return this.pendingMessages.length;
  }

  /**
   * Check if there are any buffered messages.
   */
  get isEmpty(): boolean {
    return this.pendingMessages.length === 0;
  }
}
