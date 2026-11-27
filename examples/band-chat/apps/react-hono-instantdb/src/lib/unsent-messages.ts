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

/**
 * Sent messages the server rejected.
 *
 * It lives above the rooms, not in a room's composer: a person removed from a
 * room loses the room from view as soon as their access is gone, which is
 * also when a message from them is rejected. A notice kept here outlives the
 * room. Instant's `transact` promise rejects when the server refuses the
 * write (and rolls back its optimistic update), so that is all it tracks.
 */
export class UnsentMessages {
  private readonly listeners = new Set<() => void>();
  private readonly draftListeners = new Set<(message: UnsentMessage) => void>();
  private unsent: readonly UnsentMessage[] = [];
  private nextId = 1;

  track(write: Promise<unknown>, message: OutgoingMessage): void {
    write.catch((cause: unknown) =>
      this.add(message, cause instanceof Error ? cause.message : String(cause)),
    );
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
