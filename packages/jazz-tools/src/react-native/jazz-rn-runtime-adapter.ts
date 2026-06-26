import type { InsertValues, Value, WasmSchema } from "../drivers/types.js";
import type {
  BatchMode,
  DirectInsertResult,
  DirectMutationResult,
  MutationErrorEvent,
  Runtime,
} from "../runtime/client.js";
import { encodeFFIRecordToJson } from "../runtime/ffi-value.js";

export type JazzRnErrorTag =
  | "InvalidJson"
  | "InvalidUuid"
  | "InvalidTier"
  | "Schema"
  | "Runtime"
  | "Internal";

export type JazzRnNormalizedError = Error & {
  tag: JazzRnErrorTag;
  cause?: unknown;
};

export interface JazzRnRuntimeBinding {
  batchedTick(): void;
  close(): void;
  connect(url: string, authJson: string): void;
  disconnect(): void;
  updateAuth(authJson: string): void;
  onAuthFailure(callback: { onFailure(reason: string): void }): void;
  composeBranchName?(userBranch: string): string;
  delete_(objectId: string, writeContextJson: string | undefined): string;
  getSchemaHash(): string;
  insert(
    table: string,
    valuesJson: string,
    writeContextJson: string | undefined,
    objectId: string | undefined,
  ): string;
  restore(
    table: string,
    objectId: string,
    valuesJson: string,
    writeContextJson: string | undefined,
  ): string;
  upsert(
    table: string,
    objectId: string,
    valuesJson: string,
    writeContextJson: string | undefined,
  ): string;
  beginBatch(batchMode: BatchMode): string;
  rollbackBatch(batchId: string): boolean;
  waitForBatch(batchId: string, tier: string): Promise<void>;
  onMutationError(callback: { onError(eventJson: string): void }): void;
  onBatchedTickNeeded(
    callback:
      | {
          requestBatchedTick(): void;
        }
      | undefined,
  ): void;
  query(
    queryJson: string,
    sessionJson: string | undefined,
    tier: string | undefined,
    optionsJson: string | undefined,
  ): Promise<string>;
  createSubscription(
    queryJson: string,
    sessionJson: string | undefined,
    tier: string | undefined,
  ): bigint;
  executeSubscription(handle: bigint, callback: { onUpdate(deltaJson: string): void }): void;
  unsubscribe(handle: bigint): void;
  update(objectId: string, valuesJson: string, writeContextJson: string | undefined): string;
  commitBatch(batchId: string): void;
  uniffiDestroy?(): void;
}

function swallowCallbackError(context: string, error: unknown): void {
  // Callback exceptions crossing the UniFFI boundary can panic Rust and fail writes.
  // Keep runtime alive and surface the real JS error in logs.
  try {
    // eslint-disable-next-line no-console
    console.error(`[jazz-rn] ${context} callback failed`, error);
  } catch {
    // Ignore logging failures.
  }
}

function isJazzRnErrorLike(
  error: unknown,
): error is { tag: string; inner?: { message?: unknown } } {
  if (!error || typeof error !== "object") {
    return false;
  }
  const candidate = error as { tag?: unknown; inner?: unknown };
  return typeof candidate.tag === "string";
}

function normalizeJazzRnError(error: unknown): Error {
  if (!isJazzRnErrorLike(error)) {
    return error instanceof Error ? error : new Error(String(error));
  }

  const message =
    typeof error.inner?.message === "string" && error.inner.message.length > 0
      ? error.inner.message
      : String(error);
  const tag = error.tag as JazzRnErrorTag;
  const normalized = createErrorWithCause(message, error);
  normalized.name = `JazzRn${tag}Error`;
  Object.defineProperty(normalized, "tag", {
    value: tag,
    enumerable: false,
    configurable: true,
    writable: true,
  });
  return normalized as JazzRnNormalizedError;
}

function createErrorWithCause(message: string, cause: unknown): Error {
  try {
    return new Error(message, { cause });
  } catch {
    const fallback = new Error(message) as Error & { cause?: unknown };
    Object.defineProperty(fallback, "cause", {
      value: cause,
      enumerable: false,
      configurable: true,
      writable: true,
    });
    return fallback;
  }
}

export class JazzRnRuntimeAdapter implements Runtime {
  private readonly handleMap = new Map<number, bigint>();
  private closed = false;

  constructor(
    private readonly binding: JazzRnRuntimeBinding,
    private readonly schema: WasmSchema,
  ) {
    this.binding.onBatchedTickNeeded({
      requestBatchedTick: () => {
        // Avoid re-entering Rust while the originating call still holds its mutex.
        Promise.resolve()
          .then(() => {
            if (!this.closed) {
              this.binding.batchedTick();
            }
          })
          .catch(() => {
            // Ignore callback failures from deferred ticks.
          });
      },
    });
  }

  async waitForBatch(batch_id: string, tier: string): Promise<void> {
    try {
      await this.binding.waitForBatch(batch_id, tier);
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  beginBatch(batch_mode: BatchMode): string {
    try {
      return this.binding.beginBatch(batch_mode);
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  insert(
    table: string,
    values: InsertValues,
    write_context_json?: string | null,
    object_id?: string | null,
  ): DirectInsertResult {
    try {
      const rowJson = this.binding.insert(
        table,
        encodeFFIRecordToJson(values),
        write_context_json ?? undefined,
        object_id ?? undefined,
      );
      return JSON.parse(rowJson) as DirectInsertResult;
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  restore(
    table: string,
    object_id: string,
    values: InsertValues,
    write_context_json?: string | null,
  ): DirectInsertResult {
    try {
      const rowJson = this.binding.restore(
        table,
        object_id,
        encodeFFIRecordToJson(values),
        write_context_json ?? undefined,
      );
      return JSON.parse(rowJson) as DirectInsertResult;
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  update(
    object_id: string,
    values: Record<string, Value>,
    write_context_json?: string | null,
  ): DirectMutationResult {
    try {
      const resultJson = this.binding.update(
        object_id,
        encodeFFIRecordToJson(values),
        write_context_json ?? undefined,
      );
      return JSON.parse(resultJson) as DirectMutationResult;
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  upsert(
    table: string,
    object_id: string,
    values: InsertValues,
    write_context_json?: string | null,
  ): DirectMutationResult {
    try {
      const resultJson = this.binding.upsert(
        table,
        object_id,
        encodeFFIRecordToJson(values),
        write_context_json ?? undefined,
      );
      return JSON.parse(resultJson) as DirectMutationResult;
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  delete(object_id: string, write_context_json?: string | null): DirectMutationResult {
    try {
      const resultJson = this.binding.delete_(object_id, write_context_json ?? undefined);
      return JSON.parse(resultJson) as DirectMutationResult;
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  async query(
    query_json: string,
    session_json?: string | null,
    tier?: string | null,
    options_json?: string | null,
  ): Promise<any> {
    try {
      const rowsJson = await this.binding.query(
        query_json,
        session_json ?? undefined,
        tier ?? undefined,
        options_json ?? undefined,
      );
      return JSON.parse(rowsJson);
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  createSubscription(
    query_json: string,
    session_json?: string | null,
    tier?: string | null,
  ): number {
    const handle = this.binding.createSubscription(
      query_json,
      session_json ?? undefined,
      tier ?? undefined,
    );

    const numericHandle = Number(handle);
    if (!Number.isSafeInteger(numericHandle)) {
      throw new Error(`Subscription handle ${handle.toString()} is outside safe integer range`);
    }
    this.handleMap.set(numericHandle, handle);
    return numericHandle;
  }

  executeSubscription(handle: number, on_update: Function): void {
    const nativeHandle = this.handleMap.get(handle) ?? BigInt(handle);
    this.binding.executeSubscription(nativeHandle, {
      onUpdate: (deltaJson: string) => {
        try {
          const parsed = JSON.parse(deltaJson) as unknown;
          on_update(parsed);
        } catch (error) {
          swallowCallbackError("subscription", error);
        }
      },
    });
  }

  unsubscribe(handle: number): void {
    const nativeHandle = this.handleMap.get(handle) ?? BigInt(handle);
    this.binding.unsubscribe(nativeHandle);
    this.handleMap.delete(handle);
  }

  // No outbox-target attachment on RN — server sync is handled by the
  // Rust-owned WebSocket transport (runtime.connect()), and there is no
  // worker `postMessage` channel.

  connect(url: string, authJson: string): void {
    if (this.closed) return;
    this.binding.connect(url, authJson);
  }

  disconnect(): void {
    if (this.closed) return;
    this.binding.disconnect();
  }

  updateAuth(authJson: string): void {
    if (this.closed) return;
    this.binding.updateAuth(authJson);
  }

  onAuthFailure(callback: (reason: string) => void): void {
    if (this.closed) return;
    this.binding.onAuthFailure({
      onFailure: (reason: string) => {
        try {
          callback(reason);
        } catch (error) {
          swallowCallbackError("onAuthFailure", error);
        }
      },
    });
  }

  onMutationError(callback: (event: MutationErrorEvent) => void): void {
    if (this.closed) return;
    this.binding.onMutationError({
      onError: (eventJson: string) => {
        try {
          callback(JSON.parse(eventJson) as MutationErrorEvent);
        } catch (error) {
          swallowCallbackError("onMutationError", error);
        }
      },
    });
  }

  commitBatch(batch_id: string): void {
    try {
      this.binding.commitBatch(batch_id);
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  rollbackBatch(batch_id: string): boolean {
    try {
      return this.binding.rollbackBatch(batch_id);
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  getSchema(): any {
    return this.schema;
  }

  getSchemaHash(): string {
    return this.binding.getSchemaHash();
  }

  composeBranchName(userBranch: string): string {
    if (!this.binding.composeBranchName) {
      throw new Error("Branch composition is not available in this React Native runtime");
    }
    return this.binding.composeBranchName(userBranch);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.binding.onBatchedTickNeeded(undefined);
    this.handleMap.clear();
    try {
      this.binding.close();
    } catch {
      // Ignore close failures on teardown.
    }
    this.binding.uniffiDestroy?.();
  }
}
