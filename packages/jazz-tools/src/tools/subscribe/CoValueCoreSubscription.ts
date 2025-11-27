import {
  cojsonInternals,
  CoValueCore,
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

    // If the CoValue is already available, handle it immediately
    if (source.isAvailable()) {
      this.handleAvailableSource();
      return;
    }

    // If a specific branch is requested while the source is not available, attempt to checkout that branch
    if (this.branchName) {
      this.handleBranchCheckout();
      return;
    }

    // If we don't have a branch requested, load the CoValue
    this.loadCoValue();
  }

  /**
   * Handles the case where the CoValue source is immediately available.
   * Either subscribes directly or attempts to get the requested branch.
   */
  private handleAvailableSource(): void {
    if (!this.branchName || !cojsonInternals.canBeBranched(this.source)) {
      this.subscribe(this.source.getCurrentContent());
      return;
    }

    // Try to get the specific branch from the available source
    const branch = this.source.getBranch(this.branchName, this.branchOwnerId);

    if (branch.isAvailable()) {
      // Branch is available, subscribe to it
      this.subscribe(branch.getCurrentContent());
      return;
      // If the branch hasn't been created, we create it directly so we can syncronously subscribe to it
    } else if (!this.source.hasBranch(this.branchName, this.branchOwnerId)) {
      this.source.createBranch(this.branchName, this.branchOwnerId);
      this.subscribe(branch.getCurrentContent());
    } else {
      // Branch not available, fall through to checkout logic
      this.handleBranchCheckout();
    }
  }

  /**
   * Attempts to checkout a specific branch of the CoValue.
   * This is called when the source isn't available but a branch is requested.
   */
  private handleBranchCheckout(): void {
    this.localNode
      .checkoutBranch(this.source.id, this.branchName!, this.branchOwnerId)
      .then((value) => {
        if (this.unsubscribed) return;

        if (value !== CoValueLoadingState.UNAVAILABLE) {
          // Branch checkout successful, subscribe to it
          this.subscribe(value);
        } else {
          // Branch checkout failed, handle the error
          this.handleUnavailableBranch();
        }
      })
      .catch((error) => {
        // Handle unexpected errors during branch checkout
        console.error(error);
        this.emit(CoValueLoadingState.UNAVAILABLE);
      });
  }

  /**
   * Handles the case where a branch checkout fails.
   * Determines whether to retry or report unavailability.
   */
  private handleUnavailableBranch(): void {
    const source = this.source;
    if (source.isAvailable()) {
      // This should be impossible - if source is available we can create the branch and it should be available
      throw new Error("Branch is unavailable");
    }

    // Source isn't available either, subscribe to state changes and report unavailability
    this.subscribeToUnavailableSource();
    this.emit(CoValueLoadingState.UNAVAILABLE);
  }

  /**
   * Loads the CoValue core from the network/storage.
   * This is the fallback strategy when immediate availability fails.
   */
  private loadCoValue(): void {
    this.localNode
      .loadCoValueCore(this.source.id, undefined, this.skipRetry)
      .then((value) => {
        if (this.unsubscribed) return;

        if (value.isAvailable()) {
          // Loading successful, subscribe to the loaded value
          this.subscribe(value.getCurrentContent());
        } else {
          // Loading failed, subscribe to state changes and report unavailability
          this.subscribeToUnavailableSource();
          this.emit(CoValueLoadingState.UNAVAILABLE);
        }
      })
      .catch((error) => {
        // Handle unexpected errors during loading
        console.error(error);
        this.emit(CoValueLoadingState.UNAVAILABLE);
      });
  }

  /**
   * Subscribes to state changes of an unavailable CoValue source.
   * This allows the subscription to become active when the source becomes available after a first loading attempt.
   */
  private subscribeToUnavailableSource(): void {
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

      if (this.branchName) {
        // Branch was requested, attempt checkout again
        this.handleBranchCheckout();
      } else {
        // No branch requested, subscribe directly and cleanup state subscription
        this.subscribe(source.getCurrentContent());
      }
    };

    // Subscribe to state changes and store the unsubscribe function
    this._unsubscribe = source.subscribe(handleStateChange);
  }

  /**
   * Subscribes to a specific CoValue and notifies the listener.
   * This is the final step where we actually start receiving updates.
   */
  private subscribe(value: RawCoValue): void {
    if (this.unsubscribed) return;

    // Subscribe to the value and store the unsubscribe function
    this._unsubscribe = value.subscribe((value) => {
      this.emit(value);
    });
  }

  emit(value: RawCoValue | typeof CoValueLoadingState.UNAVAILABLE): void {
    if (this.unsubscribed) return;
    if (!isReadyForEmit(value)) {
      return;
    }

    this.listener(value);
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
function isReadyForEmit(value: RawCoValue | "unavailable") {
  if (value === "unavailable") {
    return true;
  }

  return (
    value.core.verified?.header.meta?.type === "binary" ||
    value.core.isCompletelyDownloaded()
  );
}
