import { LocalNode } from "cojson";
import type {
  CoValue,
  CoValueClassOrSchema,
  Loaded,
  RefEncoded,
  RefsToResolve,
  ResolveQuery,
} from "../internal.js";
import { coValueClassFromCoValueClassOrSchema } from "../internal.js";
import { SubscriptionScope } from "./SubscriptionScope.js";
import type { BranchDefinition } from "./types.js";
import { isEqualRefsToResolve } from "./utils.js";

function copyResolve(resolve: RefsToResolve<any>): RefsToResolve<any> {
  if (typeof resolve !== "object" || resolve === null) {
    return resolve;
  }

  return { ...resolve };
}

interface CacheEntry {
  subscriptionScope: SubscriptionScope<any>;
  schema: CoValueClassOrSchema;
  resolve: RefsToResolve<any>;
  branch?: BranchDefinition;
  subscriberCount: number;
  cleanupTimeoutId?: ReturnType<typeof setTimeout>;
  unsubscribeFromScope: () => void;
}

export class SubscriptionCache {
  // Nested cache: outer map keyed by id, inner set of CacheEntry
  private cache: Map<string, Set<CacheEntry>>;
  private cleanupTimeout: number;

  constructor(cleanupTimeout: number = 5000) {
    this.cache = new Map();
    this.cleanupTimeout = cleanupTimeout;
  }

  /**
   * Get the inner set for a given id (read-only access)
   */
  private getIdSet(id: string): Set<CacheEntry> | undefined {
    return this.cache.get(id);
  }

  /**
   * Get the inner set for a given id, creating it if it doesn't exist
   */
  private getIdSetOrCreate(id: string): Set<CacheEntry> {
    let idSet = this.cache.get(id);
    if (!idSet) {
      idSet = new Set();
      this.cache.set(id, idSet);
    }
    return idSet;
  }

  /**
   * Check if an entry matches the provided parameters
   */
  private matchesEntry(
    entry: CacheEntry,
    schema: CoValueClassOrSchema,
    resolve: RefsToResolve<any>,
    branch?: BranchDefinition,
  ): boolean {
    // Compare schema by object identity
    if (entry.schema !== schema) {
      return false;
    }

    // Compare resolve queries using isEqualRefsToResolve
    if (!isEqualRefsToResolve(entry.resolve, resolve)) {
      return false;
    }

    // Compare branch names by string equality
    const branchName = branch?.name;
    if (entry.branch?.name !== branchName) {
      return false;
    }

    // Compare branch owner ids by string equality
    const branchOwnerId = branch?.owner?.$jazz.id;
    if (entry.branch?.owner?.$jazz.id !== branchOwnerId) {
      return false;
    }

    return true;
  }

  /**
   * Find a matching cache entry by comparing against entry properties
   * Uses id-based nesting to quickly filter candidates
   */
  private findMatchingEntry(
    schema: CoValueClassOrSchema,
    id: string,
    resolve: RefsToResolve<any>,
    branch?: BranchDefinition,
  ): CacheEntry | undefined {
    // Get the inner set for this id (quick filter)
    const idSet = this.getIdSet(id);
    if (!idSet) {
      return undefined;
    }

    // Search only within entries for this id
    for (const entry of idSet) {
      if (this.matchesEntry(entry, schema, resolve, branch)) {
        return entry;
      }
    }

    return undefined;
  }

  /**
   * Handle subscriber count changes from SubscriptionScope
   */
  private handleSubscriberChange(entry: CacheEntry, count: number): void {
    entry.subscriberCount = count;

    if (count === 0) {
      // Schedule cleanup when subscriber count reaches zero
      this.scheduleCleanup(entry);
    } else {
      // Cancel cleanup if count increases from zero
      this.cancelCleanup(entry);
    }
  }

  /**
   * Schedule cleanup timeout for an entry
   */
  private scheduleCleanup(entry: CacheEntry): void {
    // Cancel any existing cleanup timeout
    this.cancelCleanup(entry);

    entry.cleanupTimeoutId = setTimeout(() => {
      this.destroyEntry(entry);
    }, this.cleanupTimeout);
  }

  /**
   * Cancel pending cleanup timeout for an entry
   */
  private cancelCleanup(entry: CacheEntry): void {
    if (entry.cleanupTimeoutId !== undefined) {
      clearTimeout(entry.cleanupTimeoutId);
      entry.cleanupTimeoutId = undefined;
    }
  }

  /**
   * Destroy a cache entry and its SubscriptionScope
   */
  private destroyEntry(entry: CacheEntry): void {
    // Cancel any pending cleanup
    this.cancelCleanup(entry);

    // Unsubscribe from subscriber changes
    entry.unsubscribeFromScope();

    // Destroy the SubscriptionScope
    try {
      entry.subscriptionScope.destroy();
    } catch (error) {
      // Log error but don't throw - we still want to remove the entry
      console.error("Error destroying SubscriptionScope:", error);
    }

    // Remove from nested cache structure
    const id = entry.subscriptionScope.id;
    const idSet = this.getIdSet(id);
    if (idSet) {
      idSet.delete(entry);
      // Clean up empty inner set to prevent memory leaks
      if (idSet.size === 0) {
        this.cache.delete(id);
      }
    }
  }

  /**
   * Get or create a SubscriptionScope from the cache
   */
  getOrCreate<S extends CoValueClassOrSchema>(
    node: LocalNode,
    schema: S,
    id: string,
    resolve: ResolveQuery<S>,
    skipRetry?: boolean,
    bestEffortResolution?: boolean,
    branch?: BranchDefinition,
  ): SubscriptionScope<Loaded<S, ResolveQuery<S>>> {
    // Handle undefined/null id case
    if (!id) {
      throw new Error("Cannot create subscription with undefined or null id");
    }

    // Search for matching entry
    const matchingEntry = this.findMatchingEntry(schema, id, resolve, branch);

    if (matchingEntry) {
      // Found existing entry - cancel any pending cleanup since we're reusing it
      this.cancelCleanup(matchingEntry);

      return matchingEntry.subscriptionScope as SubscriptionScope<
        Loaded<S, ResolveQuery<S>>
      >;
    }

    // Create new SubscriptionScope
    // Transform schema to RefEncoded format
    const refEncoded: RefEncoded<CoValue> = {
      ref: coValueClassFromCoValueClassOrSchema(schema) as any,
      optional: true,
    };

    // Create new SubscriptionScope with all required parameters
    const subscriptionScope = new SubscriptionScope<Loaded<S, ResolveQuery<S>>>(
      node,
      resolve,
      id,
      refEncoded,
      skipRetry ?? false,
      bestEffortResolution ?? false,
      branch,
    );

    const handleSubscriberChange = (count: number) => {
      const idSet = this.getIdSet(id);
      if (idSet && idSet.has(entry)) {
        this.handleSubscriberChange(entry, count);
      }
    };

    // Create cache entry with initial subscriber count (starts at 0)
    // Clone resolve to prevent mutation by SubscriptionScope.subscribeToKey from affecting cache lookups
    const entry: CacheEntry = {
      subscriptionScope,
      schema,
      resolve: copyResolve(resolve),
      branch,
      subscriberCount: subscriptionScope.subscribers.size,
      unsubscribeFromScope: subscriptionScope.onSubscriberChange(
        handleSubscriberChange,
      ),
    };

    // Store in nested cache structure
    const idSet = this.getIdSetOrCreate(id);
    idSet.add(entry);

    return subscriptionScope;
  }

  /**
   * Clear all cache entries and destroy all SubscriptionScope instances
   */
  clear(): void {
    // Collect all entries first to avoid iteration issues during deletion
    const entriesToDestroy: CacheEntry[] = [];
    for (const idSet of this.cache.values()) {
      for (const entry of idSet) {
        entriesToDestroy.push(entry);
      }
    }

    // Destroy all entries
    for (const entry of entriesToDestroy) {
      this.destroyEntry(entry);
    }

    // Clear the cache map (should already be empty, but ensure it)
    this.cache.clear();
  }
}
