import {
  cojsonInternals,
  CoValueCore,
  isRawCoID,
  LocalNode,
  RawCoID,
  RawCoValue,
} from "cojson";
import type { BranchDefinition } from "./types.js";
import { CoValueLoadingState } from "./types.js";

/**
 * Manages subscriptions to CoValue cores, handling both direct subscriptions
 * and branch-based subscriptions with automatic loading and error handling.
 *
 * It tries to resolve the value immediately if already available in memory.
 */
export class CoValueCoreSubscription {
  private _unsubscribe: () => void = () => {};
  private unsubscribed = false;

  private branchOwnerId?: RawCoID;
  private branchName?: string;
  private source: CoValueCore;
  private localNode: LocalNode;
  private listener: (
    value: RawCoValue | typeof CoValueLoadingState.UNAVAILABLE,
  ) => void;
  private skipRetry?: boolean;

  constructor(
    localNode: LocalNode,
    id: string,
    listener: (
      value: RawCoValue | typeof CoValueLoadingState.UNAVAILABLE,
    ) => void,
    skipRetry?: boolean,
    branch?: BranchDefinition,
  ) {
    this.localNode = localNode;
    this.listener = listener;
    this.skipRetry = skipRetry;
    this.branchName = branch?.name;
    this.branchOwnerId = branch?.owner?.$jazz.raw.id;
    this.source = localNode.getCoValue(id as RawCoID);

    this.initializeSubscription();
  }

  /**
   * Rehydrates the subscription by resetting the unsubscribed flag and initializing the subscription again
   */
  pullValue() {
    if (!this.unsubscribed) {
      return;
    }

    // Reset the unsubscribed flag so we can initialize the subscription again
    this.unsubscribed = false;
    this.initializeSubscription();
    this.unsubscribe();
  }

  /**
   * Main entry point for subscription initialization.
   * Determines the subscription strategy based on current availability and branch requirements.
   */
  private initializeSubscription(): void {
    const source = this.source;

    // If the ID is not a valid raw CoID, we immediately emit an unavailable event
    if (!isRawCoID(source.id)) {
      this.emit(CoValueLoadingState.UNAVAILABLE);
      return;
    }

    // If we have a branch name, we handle branching
    if (this.branchName) {
      this.handleBranching(this.branchName, this.branchOwnerId);
      return;
    }

    // If we don't have a branch name, we subscribe to the source directly
    this.subscribe(this.source);
  }

  private handleBranching(branchName: string, branchOwnerId?: RawCoID) {
    const source = this.source;

    // If the source is not available, we wait for it to become available and then try to branch
    if (!source.isAvailable()) {
      this.waitForSourceToBecomeAvailable(branchName, branchOwnerId);
      return;
    }

    // If the source is not branchable (e.g. it is a group), we subscribe to it directly
    if (!cojsonInternals.canBeBranched(source)) {
      this.subscribe(source);
      return;
    }

    // Try to get the specific branch from the available source
    const branch = source.getBranch(branchName, branchOwnerId);

    // If the branch hasn't been created, we create it directly so we can syncronously subscribe to it
    if (!branch.isAvailable() && !source.hasBranch(branchName, branchOwnerId)) {
      try {
        source.createBranch(branchName, branchOwnerId);
      } catch (error) {
        // If the branch creation fails (provided group is not available), we emit an unavailable event
        console.error("error creating branch", error);
        this.emit(CoValueLoadingState.UNAVAILABLE);
        return;
      }
    }

    this.subscribe(branch);
  }

  /**
   * Loads a CoValue core and emits an unavailable event if it is still unavailable after the retries.
   */
  load(value: CoValueCore) {
    this.localNode
      .loadCoValueCore(value.id, undefined, this.skipRetry)
      .then(() => {
        // If after the retries the value is still unavailable, we emit an unavailable event
        if (!value.isAvailable()) {
          this.emit(CoValueLoadingState.UNAVAILABLE);
        }
      });
  }

  /**
   * Waits for the source to become available and then tries to branch.
   */
  private waitForSourceToBecomeAvailable(
    branchName: string,
    branchOwnerId?: RawCoID,
  ): void {
    const source = this.source;

    const handleStateChange = (
      _: CoValueCore,
      unsubFromStateChange: () => void,
    ) => {
      // We are waiting for the source to become available, it's ok to wait indefinitiely
      // until either this becomes available or we unsubscribe, because we have already
      // emitted an "unavailable" event.
      if (!source.isAvailable()) {
        return;
      }

      unsubFromStateChange();

      this.handleBranching(branchName, branchOwnerId);
    };

    // Subscribe to state changes and store the unsubscribe function
    this._unsubscribe = source.subscribe(handleStateChange);

    this.load(source);
  }

  /**
   * Subscribes to a specific CoValue and notifies the listener.
   * This is the final step where we actually start receiving updates.
   */
  private subscribe(value: CoValueCore): void {
    if (this.unsubscribed) return;

    // Subscribe to the value and store the unsubscribe function
    this._unsubscribe = value.subscribe((value) => {
      if (value.isAvailable()) {
        this.emit(value);
      }
    });

    if (!value.isAvailable()) {
      this.load(value);
    }
  }

  lastState: CoValueLoadingState | undefined;

  emit(value: CoValueCore | typeof CoValueLoadingState.UNAVAILABLE): void {
    if (this.unsubscribed) return;
    if (value === CoValueLoadingState.UNAVAILABLE) {
      this.listener(CoValueLoadingState.UNAVAILABLE);
    } else if (isCompletelyDownloaded(value)) {
      this.listener(value.getCurrentContent());
    }
  }

  /**
   * Unsubscribes from all active subscriptions and marks the instance as unsubscribed.
   * This prevents any further operations and ensures proper cleanup.
   */
  unsubscribe(): void {
    if (this.unsubscribed) return;
    this.unsubscribed = true;
    this._unsubscribe();
  }
}

/**
 * This is true if the value is unavailable, or if the value is a binary coValue or a completely downloaded coValue.
 */
function isCompletelyDownloaded(value: CoValueCore) {
  return (
    value.isDeleted ||
    value.verified?.header.meta?.type === "binary" ||
    value.isCompletelyDownloaded()
  );
}
