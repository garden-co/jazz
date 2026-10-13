/**
 * Start on Jazz Lite, then hot-swap to full local-first Jazz without dropping
 * live subscriptions (experimental).
 *
 * The app talks to one facade. Until `upgrade()` it is backed by `LiteDb`;
 * `upgrade()` loads full Jazz (the caller's `loadFull`, normally a dynamic
 * `import("jazz-tools")`, so WASM and workers stay out of the initial bundle),
 * then moves every live subscription across:
 *
 * 1. Open the same query on full Jazz, with the app's own options.
 * 2. Keep delivering lite results while full Jazz syncs. A cold store's first
 *    full results are partial (often empty), so they are never shown.
 * 3. Switch on the first full result equal to the latest lite result, in one
 *    synchronous step, and unsubscribe lite. The views agree, so the app sees
 *    no extra render. If they never agree within `handoverServerWaitMs`, switch
 *    anyway (one visible jump) so the upgrade cannot hang.
 *
 * New writes go to full Jazz once it is ready. Lite writes and transactions
 * already in flight finish on lite; the server orders them, and full Jazz
 * receives them by sync. The lite socket closes when nothing uses it.
 */
import type { Db, QueryBuilder, QueryOptions, TableProxy } from "jazz-tools";
import type { LiteDb, LiteTransaction } from "./client.js";
import type { WriteTier } from "./protocol.js";

export type UpgradeMode = "lite" | "upgrading" | "full";

export interface UpgradableDbConfig {
  lite: LiteDb;
  /** Load and open full Jazz for the same account. Called once, by `upgrade()`. */
  loadFull: () => Promise<Db>;
  /**
   * How long a subscription may wait for full Jazz to agree with lite before
   * it is switched anyway. Defaults to 10 s.
   */
  handoverServerWaitMs?: number;
}

export interface UpgradableWriteOptions {
  /** Defaults to "global" on lite and "local" on full Jazz. */
  tier?: WriteTier;
}

/** Transaction surface shared by both backends; every method is async. */
export interface UpgradableTransaction {
  all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]>;
  one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null>;
  insert<T, Init>(table: TableProxy<T, Init>, data: Init): Promise<T>;
  update<T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): Promise<void>;
  upsert<T, Init>(table: TableProxy<T, Init>, id: string, data: Init): Promise<void>;
  delete<T, Init>(table: TableProxy<T, Init>, id: string): Promise<void>;
}

export interface UpgradeTimings {
  /** `loadFull()` resolved: code downloaded, compiled, Db open. */
  fullReadyMs: number;
  /** Every subscription live at upgrade time has handed over. */
  handedOverMs: number;
  subscriptions: number;
}

interface Subscription {
  query: QueryBuilder<any>;
  options: QueryOptions | undefined;
  onUpdate: (rows: any[]) => void;
  onError: ((error: Error) => void) | undefined;
  source: "lite" | "full";
  stop: () => void;
  lastJson: string | null;
  /** Set during handover: switch if full Jazz already shows the latest lite result. */
  converged?: () => void;
}

type Row = { id: string };

export class UpgradableDb {
  private modeValue: UpgradeMode = "lite";
  private full: Db | null = null;
  private upgrading: Promise<UpgradeTimings> | null = null;
  private readonly subscriptions = new Set<Subscription>();
  private readonly listeners = new Set<(mode: UpgradeMode) => void>();
  private liteInFlight = 0;
  /** Per moved subscription: whether the switch was forced, and how long it waited. */
  readonly handovers: Array<{ forced: boolean; waitedMs: number }> = [];

  constructor(private readonly config: UpgradableDbConfig) {}

  get mode(): UpgradeMode {
    return this.modeValue;
  }

  onModeChange(listener: (mode: UpgradeMode) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  // -- reads -----------------------------------------------------------------

  all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]> {
    return this.full
      ? this.full.all(query, options)
      : this.onLite(() => this.config.lite.all(query, options));
  }

  async one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null> {
    return this.full
      ? this.full.one(query, options)
      : this.onLite(() => this.config.lite.one(query, options));
  }

  subscribe<T extends Row>(
    query: QueryBuilder<T>,
    callbacks:
      | ((rows: T[]) => void)
      | { onUpdate: (rows: T[]) => void; onError?: (error: Error) => void },
    options?: QueryOptions,
  ): () => void {
    const { onUpdate, onError } =
      typeof callbacks === "function" ? { onUpdate: callbacks, onError: undefined } : callbacks;
    const sub: Subscription = {
      query,
      options,
      onUpdate: onUpdate as (rows: any[]) => void,
      onError,
      source: this.full ? "full" : "lite",
      stop: () => {},
      lastJson: null,
    };
    this.subscriptions.add(sub);
    if (this.full) {
      sub.stop = this.full.subscribe(
        query,
        { onUpdate: sub.onUpdate, onError: sub.onError } as never,
        options,
      );
    } else {
      sub.stop = this.config.lite.subscribe(
        query,
        {
          onUpdate: (rows) => {
            if (sub.source !== "lite") return;
            sub.lastJson = JSON.stringify(rows, jsonReplacer);
            sub.onUpdate(rows);
            sub.converged?.();
          },
          onError: (error) => sub.onError?.(error) ?? console.error(error),
        },
        options,
      );
      // A subscription opened while full Jazz is still loading is still in
      // `subscriptions` when the upgrade collects what to move.
    }
    return () => {
      this.subscriptions.delete(sub);
      sub.stop();
    };
  }

  // -- writes ----------------------------------------------------------------

  async insert<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    options?: UpgradableWriteOptions,
  ): Promise<T> {
    if (this.full) return this.full.insert(table, data).wait({ tier: options?.tier ?? "local" });
    return this.onLite(() => this.config.lite.insert(table, data, options));
  }

  async update<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: UpgradableWriteOptions,
  ): Promise<void> {
    if (this.full)
      return this.full.update(table, id, data).wait({ tier: options?.tier ?? "local" });
    return this.onLite(() => this.config.lite.update(table, id, data, options));
  }

  async upsert<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Init,
    options?: UpgradableWriteOptions,
  ): Promise<void> {
    if (this.full)
      return this.full.upsert(table, id, data).wait({ tier: options?.tier ?? "local" });
    return this.onLite(() => this.config.lite.upsert(table, id, data, options));
  }

  async delete<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    options?: UpgradableWriteOptions,
  ): Promise<void> {
    if (this.full) return this.full.delete(table, id).wait({ tier: options?.tier ?? "local" });
    return this.onLite(() => this.config.lite.delete(table, id, options));
  }

  async transaction<R>(
    callback: (tx: UpgradableTransaction) => Promise<R> | R,
    options?: UpgradableWriteOptions,
  ): Promise<R> {
    if (this.full) {
      const result = await this.full.transaction((tx) => callback(wrapFullTx(tx)));
      return result.wait({ tier: options?.tier ?? "local" });
    }
    return this.onLite(() => this.config.lite.transaction((tx) => callback(tx), options));
  }

  async exclusiveTransaction<R>(
    callback: (tx: UpgradableTransaction) => Promise<R> | R,
    options?: UpgradableWriteOptions,
  ): Promise<R> {
    if (this.full) {
      // Exclusive commits always wait for the authority's verdict.
      const result = await this.full.exclusiveTransaction((tx) => callback(wrapFullTx(tx)));
      return result.wait();
    }
    return this.onLite(() =>
      this.config.lite.exclusiveTransaction((tx: LiteTransaction) => callback(tx), options),
    );
  }

  // -- upgrade ---------------------------------------------------------------

  /** Load full Jazz and move every live subscription onto it. Idempotent. */
  upgrade(): Promise<UpgradeTimings> {
    this.upgrading ??= this.runUpgrade();
    return this.upgrading;
  }

  private async runUpgrade(): Promise<UpgradeTimings> {
    const start = performance.now();
    this.setMode("upgrading");
    const full = await this.config.loadFull();
    const fullReadyMs = performance.now() - start;
    // From here new reads, writes and subscriptions use full Jazz.
    this.full = full;
    const moving = [...this.subscriptions].filter((sub) => sub.source === "lite");
    await Promise.all(moving.map((sub) => this.handOver(sub)));
    this.setMode("full");
    this.closeLiteWhenIdle();
    return { fullReadyMs, handedOverMs: performance.now() - start, subscriptions: moving.length };
  }

  /**
   * Move one subscription once full Jazz has caught up with it: the switch
   * happens on the first full result equal to the latest lite result, so the
   * app never sees a result go backwards. Lite keeps delivering meanwhile; if
   * both keep changing (or never agree) the switch is forced after
   * `handoverServerWaitMs`, accepting one visible jump.
   */
  private handOver(sub: Subscription): Promise<void> {
    const full = this.full!;
    const waitMs = this.config.handoverServerWaitMs ?? 10_000;
    return new Promise<void>((resolve) => {
      let switched = false;
      let latest: unknown[] | null = null;
      const switchNow = (rows: unknown[] | null, forced: boolean) => {
        if (switched) return;
        switched = true;
        clearTimeout(timer);
        sub.converged = undefined;
        if (!this.subscriptions.has(sub)) {
          stopFull();
          resolve();
          return;
        }
        const stopLite = sub.stop;
        sub.source = "full";
        sub.stop = stopFull;
        stopLite();
        this.handovers.push({ forced, waitedMs: performance.now() - started });
        resolve();
        if (rows && JSON.stringify(rows, jsonReplacer) !== sub.lastJson) sub.onUpdate(rows);
      };
      const started = performance.now();
      const timer = setTimeout(() => switchNow(latest, true), waitMs);
      // A lite result arriving while waiting may be the one full Jazz already shows.
      sub.converged = () => {
        if (latest !== null && JSON.stringify(latest, jsonReplacer) === sub.lastJson)
          switchNow(null, false);
      };
      const stopFull = full.subscribe(
        sub.query,
        {
          onUpdate: (rows: unknown[]) => {
            if (switched) {
              sub.onUpdate(rows);
              return;
            }
            latest = rows;
            if (JSON.stringify(rows, jsonReplacer) === sub.lastJson) switchNow(null, false);
          },
          onError: (error: Error) => {
            if (switched) {
              if (sub.onError) sub.onError(error);
              else console.error(error);
              return;
            }
            // Keep the subscription on lite; the error belongs to the swap, not the app.
            switched = true;
            clearTimeout(timer);
            sub.converged = undefined;
            stopFull();
            console.error("Jazz Lite: subscription could not move to full Jazz", error);
            resolve();
          },
        } as never,
        sub.options as never,
      );
    });
  }

  private async onLite<R>(work: () => Promise<R>): Promise<R> {
    this.liteInFlight++;
    try {
      return await work();
    } finally {
      this.liteInFlight--;
      if (this.modeValue === "full") this.closeLiteWhenIdle();
    }
  }

  private closeLiteWhenIdle(): void {
    const liteSubscriptions = [...this.subscriptions].some((sub) => sub.source === "lite");
    if (this.liteInFlight === 0 && !liteSubscriptions) this.config.lite.close();
  }

  private setMode(mode: UpgradeMode): void {
    this.modeValue = mode;
    for (const listener of this.listeners) listener(mode);
  }
}

function wrapFullTx(tx: any): UpgradableTransaction {
  return {
    all: (query, options) => tx.all(query, options),
    one: async (query, options) => (await tx.all(query, options))[0] ?? null,
    insert: async (table, data) => tx.insert(table, data),
    update: async (table, id, data) => void tx.update(table, id, data),
    upsert: async (table, id, data) => void tx.upsert(table, id, data),
    delete: async (table, id) => void tx.delete(table, id),
  };
}

function jsonReplacer(_key: string, value: unknown): unknown {
  if (typeof value === "bigint") return `${value}n`;
  if (value instanceof Uint8Array) return Array.from(value);
  return value;
}

export function createUpgradableDb(config: UpgradableDbConfig): UpgradableDb {
  return new UpgradableDb(config);
}
