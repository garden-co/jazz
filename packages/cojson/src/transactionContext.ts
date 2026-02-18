import type { VerifiedState } from "./coValueCore/verifiedState.js";
import type { CoValueKnownState } from "./knownState.js";

/**
 * TransactionContext manages the state of an active atomic transaction.
 *
 * When a transaction is active, mutations are applied immediately to memory
 * but the information required to build their NewContentMessages is buffered
 * instead of being synced immediately.
 * After the transaction callback completes, all buffered messages are:
 * - Processed inside LocalTransactionsSyncQueue to build the NewContentMessages
 * - Stored atomically in a single storage transaction
 * - Sent to sync servers in a single BatchMessage
 *
 * The presence of a TransactionContext is treated as the "active" signal.
 * The callback is synchronous, so no other code can run during the transaction window.
 */
export class TransactionContext {
  private pendingMessages: [VerifiedState, CoValueKnownState][] = [];

  /**
   * Buffer the information required to build a NewContentMessage for later atomic persistence and sync.
   * Messages are stored in order and will be processed as a batch.
   */
  bufferMessage(
    coValue: VerifiedState,
    knownStateBefore: CoValueKnownState,
  ): void {
    this.pendingMessages.push([coValue, knownStateBefore]);
  }

  /**
   * Get all buffered messages in order.
   * The order is preserved per CoValue for correct replay.
   */
  getPendingMessages() {
    return this.pendingMessages;
  }

  /**
   * Clear all buffered messages and CoValue IDs.
   * Called when the transaction is discarded (e.g., on error).
   */
  clear(): void {
    this.pendingMessages = [];
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
