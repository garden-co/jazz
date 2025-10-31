import { knownStateFromContent } from "../coValueContentMessage.js";
import { VerifiedState } from "../coValueCore/verifiedState.js";
import { RawCoID } from "../ids.js";
import { combineKnownStateSessions, CoValueKnownState } from "../knownState.js";
import { NewContentMessage } from "../sync.js";

/**
 * This queue is used to batch the sync of local transactions while preserving the order of updates between CoValues.
 *
 * We need to preserve the order of updates between CoValues to keep the state always consistent in case of shutdown in the middle of a sync.
 *
 * Examples:
 * 1. When we extend a Group we need to always ensure that the parent group is persisted before persisting the extension transaction.
 * 2. If we do multiple updates on the same CoMap, the updates will be batched because it's safe to do so.
 */
export class LocalTransactionsSyncQueue {
  private batch: NewContentMessage[] = [];
  private firstChunks = new Map<RawCoID, NewContentMessage>();
  private lastUpdatedValue: VerifiedState | undefined;
  private lastUpdatedValueKnownState: CoValueKnownState | undefined;

  constructor(private readonly sync: (content: NewContentMessage) => void) {}

  syncTransaction = (
    coValue: VerifiedState,
    knownStateBefore: CoValueKnownState,
  ) => {
    const lastUpdatedValue = this.lastUpdatedValue;
    const lastUpdatedValueKnownState = this.lastUpdatedValueKnownState;

    if (lastUpdatedValue && lastUpdatedValueKnownState) {
      if (lastUpdatedValue.id === coValue.id) {
        return;
      }

      this.addContentToBatch(lastUpdatedValue, lastUpdatedValueKnownState);
    }

    this.lastUpdatedValue = coValue;
    this.lastUpdatedValueKnownState = knownStateBefore;

    for (const trackingSet of this.dirtyCoValuesTrackingSets) {
      trackingSet.add(coValue.id);
    }

    this.scheduleNextBatch();
  };

  private addContentToBatch(
    coValue: VerifiedState,
    knownStateBefore: CoValueKnownState,
  ) {
    const content = coValue.newContentSince(knownStateBefore, {
      skipExpectContentUntil: true, // we need to calculate the streaming header considering the current batch
    });

    if (!content) {
      return;
    }

    let firstChunk = this.firstChunks.get(coValue.id);

    for (const piece of content) {
      this.batch.push(piece);

      // Check if the local content updates are in streaming, if so we need to add the info to the first chunk
      if (firstChunk) {
        if (!firstChunk.expectContentUntil) {
          firstChunk.expectContentUntil =
            knownStateFromContent(firstChunk).sessions;
        }
        combineKnownStateSessions(
          firstChunk.expectContentUntil,
          knownStateFromContent(piece).sessions,
        );
      } else {
        firstChunk = piece;
        this.firstChunks.set(coValue.id, firstChunk);
      }
    }
  }

  private nextBatchScheduled = false;
  scheduleNextBatch() {
    if (this.nextBatchScheduled) return;

    this.nextBatchScheduled = true;

    queueMicrotask(() => {
      if (this.lastUpdatedValue && this.lastUpdatedValueKnownState) {
        this.addContentToBatch(
          this.lastUpdatedValue,
          this.lastUpdatedValueKnownState,
        );
      }
      const batch = this.batch;

      this.lastUpdatedValue = undefined;
      this.lastUpdatedValueKnownState = undefined;
      this.firstChunks = new Map();
      this.batch = [];
      this.nextBatchScheduled = false;

      for (const content of batch) {
        this.sync(content);
      }
    });
  }

  private dirtyCoValuesTrackingSets: Set<Set<RawCoID>> = new Set();

  /**
   * It starts tracking all changed CoValues. Returns a `done()` function that returns a set of coValues' ids that have been modified since the start.
   *
   * @example
   * ```ts
   * const tracking = node.syncManager.trackDirtyCoValues();
   * // Any CoValue mutation
   * const tracked = tracking.done();
   * console.log("CoValue mutated: " Array.from(tracked))
   * ```
   */
  trackDirtyCoValues = () => {
    const trackingSet = new Set<RawCoID>();

    this.dirtyCoValuesTrackingSets.add(trackingSet);

    return {
      done: () => {
        this.dirtyCoValuesTrackingSets.delete(trackingSet);

        return trackingSet;
      },
    };
  };
}
