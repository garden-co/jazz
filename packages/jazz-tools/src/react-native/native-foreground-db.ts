type ForegroundCommand =
  | "tick"
  | "close"
  | { type: "prepareQuery"; query: Uint8Array }
  | { type: "all"; query: number }
  | { type: "subscribe"; query: number }
  | { type: "drainSubscription"; subscription: number }
  | { type: "unsubscribe"; subscription: number };

type ForegroundEvent =
  | { type: "delta"; reset: boolean; settled: boolean; tier: string; delta: Uint8Array }
  | { type: "rejected"; reason: string }
  | { type: "closed" };

type ForegroundResponse =
  | { type: "ticked" }
  | { type: "preparedQuery"; query: number }
  | { type: "rows"; rows: Uint8Array }
  | { type: "subscribed"; subscription: number }
  | { type: "subscriptionEvents"; events: ForegroundEvent[] }
  | { type: "unsubscribed"; closed: boolean }
  | { type: "closed"; closed: boolean };

export type NativeForegroundRuntime = {
  execute(command: Uint8Array): Uint8Array;
  tick(): void;
  close(): boolean;
};

export type NativeForegroundModule = {
  installNativeForegroundRuntime(): NativeForegroundFactory;
  encodeNativeForegroundCommand(command: ForegroundCommand): Uint8Array;
  decodeNativeForegroundResponse(bytes: Uint8Array): ForegroundResponse;
};

export type NativeForegroundFactory = {
  readonly abiVersion: number;
  openAttached(capability: Uint8Array): NativeForegroundRuntime;
};

export const REACT_NATIVE_READ_ONLY_UNSUPPORTED_ERROR =
  "React Native experimental native foreground currently supports local read queries and subscriptions only";

/**
 * Narrow NativeDb consumer for the first JSI foreground slice.
 *
 * Rows and subscription deltas stay in the ordinary binding byte format, so
 * NativeRuntimeAdapter remains the sole schema-aware decoder. This is not a
 * second object-shaped database facade and intentionally grows only as the
 * shared Rust command ABI grows.
 */
export class NativeForegroundReadDb {
  private closed = false;

  constructor(
    private readonly runtime: NativeForegroundRuntime,
    private readonly commands: NativeForegroundModule,
  ) {}

  setTickScheduler(_callback: unknown): void {}

  onMutationError(_callback: unknown): void {}

  prepareQuery(query: Uint8Array): object {
    const response = this.execute({ type: "prepareQuery", query });
    if (response.type !== "preparedQuery") return unexpected("prepareQuery", response.type);
    return { nativeForegroundQuery: response.query };
  }

  all(query: object, opts: unknown): Uint8Array {
    assertLocalReadOptions(opts);
    // An attached foreground is an ordinary peer of the native relay. One
    // bounded relay turn admits already-persisted rows before materializing a
    // local read; without it a newly opened foreground can only observe rows
    // after some unrelated caller happens to tick the host.
    this.tick();
    const response = this.execute({ type: "all", query: queryHandle(query) });
    if (response.type !== "rows") return unexpected("all", response.type);
    return response.rows;
  }

  allAsync(query: object, opts: unknown): Uint8Array {
    return this.all(query, opts);
  }

  allForIdentity(): never {
    return unsupported("trusted-serving reads");
  }

  subscribe(query: object, opts: unknown): NativeForegroundSubscription {
    assertLocalReadOptions(opts);
    this.tick();
    const response = this.execute({ type: "subscribe", query: queryHandle(query) });
    if (response.type !== "subscribed") return unexpected("subscribe", response.type);
    return new NativeForegroundSubscription(response.subscription, this);
  }

  tick(): void {
    this.assertOpen();
    this.runtime.tick();
  }

  close(): boolean {
    if (this.closed) return false;
    this.closed = true;
    try {
      const response = this.executeAllowClosed("close");
      if (response.type !== "closed") return unexpected("close", response.type);
      return response.closed;
    } finally {
      // The command owns logical Db closure. The HostObject close releases the
      // JSI handle and is deliberately safe to call after that transition.
      // It must also run when command execution fails (for example, because a
      // platform logout revoked the capability just before this call).
      this.runtime.close();
    }
  }

  rejectAuthUpdate(): never {
    try {
      this.close();
    } catch {
      // A platform revocation may have already closed the opaque native
      // handle. It is still essential that this JS facade remains closed.
    }
    throw new Error(
      "React Native experimental native read-only runtime cannot rotate authentication in place; revoke the old native capability and create a new Db for the newly admitted session",
    );
  }

  drain(subscription: number): unknown[] {
    const response = this.execute({ type: "drainSubscription", subscription });
    if (response.type !== "subscriptionEvents") {
      return unexpected("drainSubscription", response.type);
    }
    return response.events.map((event) => {
      if (event.type === "delta") return event;
      if (event.type === "closed") return event;
      return { type: "rejected", reason: { type: "ServerFailure", code: event.reason } };
    });
  }

  unsubscribe(subscription: number): boolean {
    if (this.closed) return false;
    const response = this.execute({ type: "unsubscribe", subscription });
    if (response.type !== "unsubscribed") return unexpected("unsubscribe", response.type);
    // Native unsubscribe retires the stream handle synchronously but queues
    // its core cleanup for the following ordinary turn. Drive that turn here:
    // after the JS reader stops polling there may be no unrelated operation
    // to do it for us.
    if (response.closed) this.tick();
    return response.closed;
  }

  private execute(command: ForegroundCommand): ForegroundResponse {
    this.assertOpen();
    return this.executeAllowClosed(command);
  }

  private executeAllowClosed(command: ForegroundCommand): ForegroundResponse {
    return this.commands.decodeNativeForegroundResponse(
      this.runtime.execute(this.commands.encodeNativeForegroundCommand(command)),
    );
  }

  private assertOpen(): void {
    if (this.closed) throw new Error("React Native native foreground runtime is closed");
  }

  // Explicit members make accidental mutation fail at the NativeDb boundary,
  // rather than later as an opaque missing-method TypeError.
  insertEncoded(): never {
    return unsupported("insert");
  }
  updateEncoded(): never {
    return unsupported("update");
  }
  upsertEncoded(): never {
    return unsupported("upsert");
  }
  deleteEncoded(): never {
    return unsupported("delete");
  }
  restoreEncoded(): never {
    return unsupported("restore");
  }
  beginTransaction(): never {
    return unsupported("beginTransaction");
  }
  commitTransaction(): never {
    return unsupported("commitTransaction");
  }
  rollbackTransaction(): never {
    return unsupported("rollbackTransaction");
  }
  attachMergeableTx(): never {
    return unsupported("attachMergeableTx");
  }
  mergeableTx(): never {
    return unsupported("mergeableTx");
  }
  registerSchema(): never {
    return unsupported("registerSchema");
  }
  connectUpstream(): never {
    return unsupported("JavaScript upstream transport");
  }
}

class NativeForegroundSubscription {
  private closed = false;

  constructor(
    private readonly handle: number,
    private readonly db: NativeForegroundReadDb,
  ) {}

  readAll(): unknown[] | { retryAfterMs(): number } {
    if (this.closed) return [];
    // There is no native-to-JS wake callback in this first slice. Polling must
    // therefore also drive one fair relay turn before observing the stream;
    // Drain alone only consumes events which some other caller has advanced.
    this.db.tick();
    const events = this.db.drain(this.handle);
    // The first command slice has no native-to-JS wake callback yet. Keep the
    // normal NativeRuntimeAdapter subscription reader suspended on its
    // existing retry contract so later native deltas are observable without
    // a second subscription implementation. This polling seam is deliberately
    // capability-gated and can become a wake-driven pending batch later.
    return events.length === 0 ? { retryAfterMs: () => 50 } : events;
  }

  close(): boolean {
    if (this.closed) return false;
    this.closed = true;
    return this.db.unsubscribe(this.handle);
  }
}

function queryHandle(query: object): number {
  const handle = (query as { nativeForegroundQuery?: unknown }).nativeForegroundQuery;
  if (!Number.isSafeInteger(handle) || (handle as number) < 0) {
    throw new Error("React Native native foreground received an invalid prepared query handle");
  }
  return handle as number;
}

function assertLocalReadOptions(opts: unknown): void {
  if (!opts || typeof opts !== "object") throw new Error(REACT_NATIVE_READ_ONLY_UNSUPPORTED_ERROR);
  const entries = Object.entries(opts as Record<string, unknown>);
  if (entries.some(([key, value]) => key !== "tier" || value !== "local")) {
    throw new Error(
      `${REACT_NATIVE_READ_ONLY_UNSUPPORTED_ERROR}; remote tiers, historical views, and include-deleted reads are not implemented`,
    );
  }
}

function unsupported(operation: string): never {
  throw new Error(`${REACT_NATIVE_READ_ONLY_UNSUPPORTED_ERROR}; ${operation} is unavailable`);
}

function unexpected(operation: string, response: string): never {
  throw new Error(
    `React Native native foreground ${operation} returned unexpected ${response} response`,
  );
}
