import type { WasmSchema } from "../drivers/types.js";
import type { Runtime } from "../runtime/client.js";
import { OutboxDestinationKind } from "../runtime/sync-transport.js";

export interface JazzRnRuntimeBinding {
  addClient(): string;
  addServer(): void;
  batchedTick(): void;
  close(): void;
  delete_(objectId: string): void;
  flush(): void;
  getSchemaHash(): string;
  insert(table: string, valuesJson: string): string;
  onBatchedTickNeeded(
    callback:
      | {
          requestBatchedTick(): void;
        }
      | undefined,
  ): void;
  onSyncMessageReceived(messageJson: string): void;
  onSyncMessageReceivedFromClient(clientId: string, messageJson: string): void;
  onSyncMessageToSend(
    callback:
      | {
          onSyncMessage(
            destinationKind: OutboxDestinationKind,
            destinationId: string,
            payloadJson: string,
            isCatalogue: boolean,
          ): void;
        }
      | undefined,
  ): void;
  query(queryJson: string, sessionJson: string | undefined, tier: string | undefined): string;
  removeServer(): void;
  setClientRole(clientId: string, role: string): void;
  subscribe(
    queryJson: string,
    callback: { onUpdate(deltaJson: string): void },
    sessionJson: string | undefined,
    tier: string | undefined,
  ): bigint;
  unsubscribe(handle: bigint): void;
  update(objectId: string, valuesJson: string): void;
  uniffiDestroy?(): void;
}

function assertWorkerTier(tier: string): void {
  if (tier !== "worker") {
    throw new Error(
      `jazz-rn runtime adapter currently supports only 'worker' tier for persisted mutations (received '${tier}')`,
    );
  }
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

function isObjectNotFoundError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const maybeInner = (error as { inner?: { message?: unknown } }).inner;
  const innerMessage =
    maybeInner && typeof maybeInner === "object" ? maybeInner.message : undefined;
  if (typeof innerMessage === "string" && innerMessage.includes("ObjectNotFound(")) {
    return true;
  }
  const message = String(error);
  return message.includes("ObjectNotFound(");
}

function swallowMissingObjectMutation(context: string, error: unknown): boolean {
  if (!isObjectNotFoundError(error)) return false;
  try {
    // eslint-disable-next-line no-console
    console.warn(`[jazz-rn] ${context}: object already missing, ignoring`, error);
  } catch {
    // Ignore logging failures.
  }
  return true;
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

  insert(table: string, values: any): string {
    return this.binding.insert(table, JSON.stringify(values));
  }

  update(object_id: string, values: any): void {
    try {
      this.binding.update(object_id, JSON.stringify(values));
    } catch (error) {
      if (swallowMissingObjectMutation("update", error)) return;
      throw error;
    }
  }

  delete(object_id: string): void {
    try {
      this.binding.delete_(object_id);
    } catch (error) {
      if (swallowMissingObjectMutation("delete", error)) return;
      throw error;
    }
  }

  async query(
    query_json: string,
    session_json?: string | null,
    tier?: string | null,
  ): Promise<any> {
    const rowsJson = this.binding.query(query_json, session_json ?? undefined, tier ?? undefined);
    return JSON.parse(rowsJson);
  }

  subscribe(
    query_json: string,
    on_update: Function,
    session_json?: string | null,
    tier?: string | null,
  ): number {
    const handle = this.binding.subscribe(
      query_json,
      {
        onUpdate: (deltaJson: string) => {
          try {
            const parsed = JSON.parse(deltaJson) as unknown;
            on_update(parsed);
          } catch (error) {
            swallowCallbackError("subscription", error);
          }
        },
      },
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

  unsubscribe(handle: number): void {
    const nativeHandle = this.handleMap.get(handle) ?? BigInt(handle);
    this.binding.unsubscribe(nativeHandle);
    this.handleMap.delete(handle);
  }

  insertDurable(table: string, values: any, tier: string): Promise<string> {
    assertWorkerTier(tier);
    const id = this.insert(table, values);
    this.binding.flush();
    return Promise.resolve(id);
  }

  updateDurable(object_id: string, values: any, tier: string): Promise<void> {
    assertWorkerTier(tier);
    this.update(object_id, values);
    this.binding.flush();
    return Promise.resolve();
  }

  deleteDurable(object_id: string, tier: string): Promise<void> {
    assertWorkerTier(tier);
    this.delete(object_id);
    this.binding.flush();
    return Promise.resolve();
  }

  onSyncMessageReceived(message_json: string): void {
    if (this.closed) return;
    this.binding.onSyncMessageReceived(message_json);
  }

  onSyncMessageToSend(callback: Function): void {
    this.binding.onSyncMessageToSend({
      onSyncMessage: (
        destinationKind: OutboxDestinationKind,
        destinationId: string,
        payloadJson: string,
        isCatalogue: boolean,
      ) => {
        try {
          callback(destinationKind, destinationId, payloadJson, isCatalogue);
        } catch (error) {
          swallowCallbackError("sync message", error);
        }
      },
    });
  }

  addServer(): void {
    if (this.closed) return;
    this.binding.addServer();
  }

  removeServer(): void {
    if (this.closed) return;
    this.binding.removeServer();
  }

  addClient(): string {
    return this.binding.addClient();
  }

  getSchema(): any {
    return this.schema;
  }

  getSchemaHash(): string {
    return this.binding.getSchemaHash();
  }

  setClientRole(client_id: string, role: string): void {
    this.binding.setClientRole(client_id, role);
  }

  onSyncMessageReceivedFromClient(client_id: string, message_json: string): void {
    if (this.closed) return;
    this.binding.onSyncMessageReceivedFromClient(client_id, message_json);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.binding.onSyncMessageToSend(undefined);
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
