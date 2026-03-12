export type MutationRejectCode =
  | "permission_denied"
  | "session_required"
  | "catalogue_write_denied";

export type RuntimeObjectOutcomeState =
  | { type: "pending"; mutationId: string }
  | { type: "accepted"; mutationId: string }
  | { type: "errored"; mutationId: string; code: MutationRejectCode; reason: string };

export interface RuntimeObjectOutcomeEvent {
  objectId: string;
  outcome: RuntimeObjectOutcomeState | null;
}

/**
 * App-facing object outcome event enriched with acknowledge() for rejected outcomes.
 */
export interface ObjectOutcomeEvent {
  objectId: string;
  outcome: ObjectOutcomeState | null;
}

/**
 * Current mutation outcome overlay for one visible object.
 *
 * This is attached to returned rows as `$outcome` and is also used by the
 * global object-outcome event stream.
 */
export type ObjectOutcomeState =
  | { type: "pending"; mutationId: string }
  | { type: "accepted"; mutationId: string }
  | {
      type: "errored";
      mutationId: string;
      code: MutationRejectCode;
      reason: string;
      acknowledge: () => Promise<void>;
    };

export interface ObjectOutcomeSource {
  getObjectOutcome(objectId: string): ObjectOutcomeState | null;
  subscribeObjectOutcomeEvents(listener: (events: ObjectOutcomeEvent[]) => void): () => void;
  acknowledgeMutationOutcome(mutationId: string): Promise<void>;
}

export interface RuntimeObjectOutcomeBindings {
  listObjectOutcomes?(): RuntimeObjectOutcomeEvent[];
  takeObjectOutcomeEvents?(): RuntimeObjectOutcomeEvent[];
  acknowledgeMutationOutcome?(mutationId: string): void | Promise<void>;
  setMutationJournalEnabled?(enabled: boolean): void;
}

type OutcomeListener = (events: ObjectOutcomeEvent[]) => void;

export class ObjectOutcomeMirror implements ObjectOutcomeSource {
  private readonly cache = new Map<string, RuntimeObjectOutcomeState>();
  private readonly listeners = new Set<OutcomeListener>();

  constructor(
    private readonly acknowledgeImpl: (mutationId: string) => Promise<void> | void = async () => {},
  ) {}

  replaceSnapshot(snapshot: RuntimeObjectOutcomeEvent[]): void {
    const next = new Map<string, RuntimeObjectOutcomeState>();
    for (const event of snapshot) {
      if (event.outcome) {
        next.set(event.objectId, event.outcome);
      }
    }

    const changedIds = new Set<string>([...this.cache.keys(), ...next.keys()]);
    const changedObjectIds: string[] = [];

    for (const objectId of changedIds) {
      const previous = this.cache.get(objectId) ?? null;
      const current = next.get(objectId) ?? null;
      if (!runtimeObjectOutcomeEquals(previous, current)) {
        changedObjectIds.push(objectId);
      }
    }

    this.cache.clear();
    for (const [objectId, outcome] of next) {
      this.cache.set(objectId, outcome);
    }

    this.emit(changedObjectIds);
  }

  applyEvents(events: RuntimeObjectOutcomeEvent[]): void {
    const changedObjectIds: string[] = [];

    for (const event of events) {
      const previous = this.cache.get(event.objectId) ?? null;
      if (runtimeObjectOutcomeEquals(previous, event.outcome)) {
        continue;
      }

      if (event.outcome) {
        this.cache.set(event.objectId, event.outcome);
      } else {
        this.cache.delete(event.objectId);
      }

      changedObjectIds.push(event.objectId);
    }

    this.emit(changedObjectIds);
  }

  getObjectOutcome(objectId: string): ObjectOutcomeState | null {
    const outcome = this.cache.get(objectId);
    if (!outcome) {
      return null;
    }

    if (outcome.type === "errored") {
      return {
        ...outcome,
        acknowledge: () => Promise.resolve(this.acknowledgeImpl(outcome.mutationId)),
      };
    }

    return outcome;
  }

  subscribeObjectOutcomeEvents(listener: OutcomeListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  async acknowledgeMutationOutcome(mutationId: string): Promise<void> {
    await this.acknowledgeImpl(mutationId);
  }

  dispose(): void {
    this.listeners.clear();
    this.cache.clear();
  }

  private emit(objectIds: string[]): void {
    if (objectIds.length === 0) {
      return;
    }

    const events = objectIds.map((objectId) => ({
      objectId,
      outcome: this.getObjectOutcome(objectId),
    }));

    for (const listener of this.listeners) {
      listener(events);
    }
  }
}

export class RuntimeObjectOutcomeSource extends ObjectOutcomeMirror {
  constructor(private readonly runtime: RuntimeObjectOutcomeBindings) {
    super(async (mutationId) => {
      if (!runtime.acknowledgeMutationOutcome) {
        return;
      }
      await runtime.acknowledgeMutationOutcome(mutationId);
      this.drain();
    });

    this.replaceSnapshot(runtime.listObjectOutcomes?.() ?? []);
  }

  drain(): void {
    this.applyEvents(this.runtime.takeObjectOutcomeEvents?.() ?? []);
  }
}

function runtimeObjectOutcomeEquals(
  left: RuntimeObjectOutcomeState | null,
  right: RuntimeObjectOutcomeState | null,
): boolean {
  if (left === right) {
    return true;
  }
  if (!left || !right) {
    return false;
  }
  if (left.type !== right.type || left.mutationId !== right.mutationId) {
    return false;
  }
  if (left.type !== "errored" || right.type !== "errored") {
    return true;
  }
  return left.code === right.code && left.reason === right.reason;
}
