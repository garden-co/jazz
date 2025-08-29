import { CoValueCore, LocalNode, RawCoID, RawCoMap, RawCoValue } from "cojson";

/**
 * Manages subscriptions to CoValue cores, handling both direct subscriptions
 * and branch-based subscriptions with automatic loading and error handling.
 *
 * It tries to resolve the value immediately if already available in memory.
 */
export class CoValueCoreSubscription {
  private _unsubscribe: () => void = () => {};
  private unsubscribed = false;

  constructor(
    public node: LocalNode,
    public id: string,
    public listener: (value: RawCoValue | "unavailable") => void,
    public skipRetry?: boolean,
    public branch?: { name: string; ownerId?: string },
  ) {
    this.initializeSubscription();
  }

  /**
   * Main entry point for subscription initialization.
   * Determines the subscription strategy based on current availability and branch requirements.
   */
  private initializeSubscription(): void {
    const source = this.node.getCoValue(this.id as any);

    // If the CoValue is already available, handle it immediately
    if (source.isAvailable()) {
      this.handleAvailableSource(source);
      return;
    }

    // If a specific branch is requested while the source is not available, attempt to checkout that branch
    if (this.branch) {
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
  private handleAvailableSource(source: CoValueCore): void {
    if (!this.branch) {
      this.subscribe(source.getCurrentContent());
      return;
    }

    const branchName = this.branch.name;
    const branchOwnerId = this.branch.ownerId as RawCoID | undefined;

    // Try to get the specific branch from the available source
    const branch = source.getBranch(branchName, branchOwnerId);

    if (branch.isAvailable()) {
      // Branch is available, subscribe to it
      this.subscribe(branch.getCurrentContent());
      return;
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
    this.node
      .checkoutBranch(
        this.id as any,
        this.branch!.name,
        this.branch!.ownerId as RawCoID | undefined,
      )
      .then((value) => {
        if (this.unsubscribed) return;

        if (value !== "unavailable") {
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
        this.listener("unavailable");
      });
  }

  /**
   * Handles the case where a branch checkout fails.
   * Determines whether to retry or report unavailability.
   */
  private handleUnavailableBranch(): void {
    const source = this.node.getCoValue(this.id as any);
    if (source.isAvailable()) {
      // This should be impossible - if source is available, branch should be too
      throw new Error("Branch is unavailable");
    }

    // Source isn't available either, subscribe to state changes and report unavailability
    this.subscribeToUnavailableSource();
    this.listener("unavailable");
  }

  /**
   * Loads the CoValue core from the network/storage.
   * This is the fallback strategy when immediate availability fails.
   */
  private loadCoValue(): void {
    this.node
      .loadCoValueCore(this.id as RawCoID, undefined, this.skipRetry)
      .then((value) => {
        if (this.unsubscribed) return;

        if (value.isAvailable()) {
          // Loading successful, subscribe to the loaded value
          this.subscribe(value.getCurrentContent());
        } else {
          // Loading failed, subscribe to state changes and report unavailability
          this.subscribeToUnavailableSource();
          this.listener("unavailable");
        }
      })
      .catch((error) => {
        // Handle unexpected errors during loading
        console.error(error);
        this.listener("unavailable");
      });
  }

  /**
   * Subscribes to state changes of an unavailable CoValue source.
   * This allows the subscription to become active when the source becomes available after a first loading attempt.
   */
  private subscribeToUnavailableSource(): void {
    const source = this.node.getCoValue(this.id as any);

    const handleStateChange = (
      _: CoValueCore, // Unused parameter, but required by the subscription API
      unsubFromStateChange: () => void,
    ) => {
      if (this.unsubscribed) {
        unsubFromStateChange();
        return;
      }

      // Source became available, handle it appropriately
      if (source.isAvailable()) {
        unsubFromStateChange();

        if (this.branch) {
          // Branch was requested, attempt checkout again
          this.handleBranchCheckout();
        } else {
          // No branch requested, subscribe directly and cleanup state subscription
          this.subscribe(source.getCurrentContent());
        }
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
      this.listener(value);
    });
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
