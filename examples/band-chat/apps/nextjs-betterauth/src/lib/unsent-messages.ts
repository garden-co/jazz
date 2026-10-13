import { writeRejectionReason } from "./write-rejection";

/** A message the server refused, kept until the person dismisses it. */
export interface UnsentMessage extends OutgoingMessage {
  id: number;
  reason: string;
}

export interface OutgoingMessage {
  roomId: string;
  roomName: string;
  /** The message text, which may be empty for an attachment. */
  text: string;
  attachmentName?: string;
}

/** The parts of a Jazz write handle this needs. */
export interface TrackedWrite {
  txId: PromiseLike<string>;
  wait(options: { tier: "global" }): Promise<unknown>;
}

/** The parts of a Jazz mutation error event this needs. */
export interface RejectedWrite {
  reason: string;
  transaction: { transactionId: string };
}

/**
 * Sent messages until the server accepts or rejects them, and the ones it
 * rejected.
 *
 * It lives above the rooms, not in a room's composer: a person removed from a
 * room loses the room from view as soon as their access is gone, which is
 * also when a queued message from them is rejected. A notice kept here
 * outlives the room.
 *
 * A write's `wait` reports a rejection only while it is waiting. When it
 * stops waiting for another reason, such as a long outage, the write is still
 * committed locally and may be rejected later. That rejection arrives through
 * `db.onMutationError`, and {@link reportMutationError} matches it back to the
 * message by transaction id.
 */
export class UnsentMessages {
  private readonly awaiting = new Map<string, OutgoingMessage>();
  private readonly listeners = new Set<() => void>();
  private readonly draftListeners = new Set<(message: UnsentMessage) => void>();
  private unsent: readonly UnsentMessage[] = [];
  private nextId = 1;

  track(write: TrackedWrite, message: OutgoingMessage): void {
    const txId = Promise.resolve(write.txId);
    const forget = () => void txId.then((id) => this.awaiting.delete(id), ignore);
    void txId.then((id) => this.awaiting.set(id, message), ignore);
    write.wait({ tier: "global" }).then(forget, (cause: unknown) => {
      const reason = writeRejectionReason(cause);
      // Any other failure leaves the write committed locally, still awaiting
      // the server's verdict.
      if (reason === undefined) return;
      forget();
      this.add(message, reason);
    });
  }

  /** Returns whether the rejected write was a tracked message. */
  reportMutationError(event: RejectedWrite): boolean {
    const id = event.transaction.transactionId;
    const message = this.awaiting.get(id);
    if (!message) return false;
    this.awaiting.delete(id);
    this.add(message, event.reason);
    return true;
  }

  dismiss(id: number): void {
    this.unsent = this.unsent.filter((message) => message.id !== id);
    this.emit();
  }

  /** The current notices; a new array whenever they change. */
  getSnapshot = (): readonly UnsentMessage[] => this.unsent;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  /** Called with each newly rejected message, e.g. to put it back in a draft. */
  onUnsent(listener: (message: UnsentMessage) => void): () => void {
    this.draftListeners.add(listener);
    return () => this.draftListeners.delete(listener);
  }

  private add(message: OutgoingMessage, reason: string) {
    const unsent = { ...message, reason, id: this.nextId++ };
    this.unsent = [...this.unsent, unsent];
    this.emit();
    for (const listener of this.draftListeners) listener(unsent);
  }

  private emit() {
    for (const listener of this.listeners) listener();
  }
}

function ignore() {}
