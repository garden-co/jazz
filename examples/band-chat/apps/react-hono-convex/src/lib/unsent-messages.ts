import { ConvexError } from "convex/values";

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

/** Why the server refused a mutation. */
function rejectionReason(cause: unknown): string {
  if (cause instanceof ConvexError && cause.data === "permission_denied")
    return "The server didn't allow it.";
  return cause instanceof Error ? cause.message : String(cause);
}

/**
 * Sent messages until the server accepts or rejects them, and the ones it
 * rejected.
 *
 * It lives above the rooms, not in a room's composer: a person removed from a
 * room loses the room from view as soon as their access is gone, which is
 * also when a queued message from them is rejected. A notice kept here
 * outlives the room.
 */
export class UnsentMessages {
  private readonly listeners = new Set<() => void>();
  private readonly draftListeners = new Set<(message: UnsentMessage) => void>();
  private unsent: readonly UnsentMessage[] = [];
  private nextId = 1;

  /** Watches a send: the Convex client retries it until the server answers. */
  track(send: Promise<unknown>, message: OutgoingMessage): void {
    send.catch((cause: unknown) => this.add(message, rejectionReason(cause)));
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
