import type { OpenTransactionId, TxId, RuntimeWriteWaitOptions } from "../runtime/client.js";
import type { RuntimeSubscriptionDelta, Value, WasmSchema } from "../drivers/types.js";
import type { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import {
  deserializeBrowserRelayError,
  type BrowserFollowerPortEvent,
  type InspectorAttachmentBinding,
  type InspectorStagedEdit,
} from "../runtime/native-runtime/browser-worker-protocol.js";

// A runtime source can expose several schema views through the same port.
// Keep correlation IDs unique across those views and across their lifetimes.
const requestIds = new WeakMap<MessagePort, number>();
function nextRequestId(port: MessagePort): number {
  const id = requestIds.get(port) ?? -2;
  if (!Number.isSafeInteger(id)) throw new Error("Inspector request identifiers exhausted");
  requestIds.set(port, id - 1);
  return id;
}

/** Private MessagePort reads and edit batches execute in the authenticated
 * storage owner, so patches preserve the same cached preimage the UI reads.
 * They never ask another peer node to disable query propagation. */
export function attachInspectorCacheRuntime(
  runtime: NativeRuntimeAdapter,
  port: MessagePort,
  binding: InspectorAttachmentBinding,
  schema: WasmSchema,
): NativeRuntimeAdapter {
  let closed = false;
  const staged = new Map<OpenTransactionId, InspectorStagedEdit[]>();
  const ownedWrites = new Set<TxId>();
  const reads = new Map<number, { resolve(value: unknown): void; reject(error: Error): void }>();
  const subscriptions = new Map<
    number,
    {
      query: string;
      options?: string | null;
      callback?: (value: RuntimeSubscriptionDelta | Error) => void;
      started: boolean;
    }
  >();
  const receive = ({ data }: MessageEvent<BrowserFollowerPortEvent>) => {
    if (
      data.type === "error" ||
      data.type === "auth-failure" ||
      data.type === "storage-invalidated" ||
      data.type === "storage-reset"
    ) {
      dispose();
      return;
    }
    if (data.type !== "inspector-query-result" && data.type !== "result") return;
    if (data.id >= -1) return;
    const error = data.error && deserializeBrowserRelayError(data.error);
    const read = reads.get(data.id);
    if (read) {
      reads.delete(data.id);
      if (error) read.reject(error);
      else if (data.type === "inspector-query-result") read.resolve(data.value);
      else read.reject(new Error("Unexpected Inspector query response"));
      return;
    }
    const subscription = subscriptions.get(data.id);
    if (subscription && (error || data.type === "inspector-query-result")) {
      subscription.callback?.(error ?? (data as { value: RuntimeSubscriptionDelta }).value);
    }
  };
  const dispose = () => {
    if (closed) return;
    closed = true;
    port.removeEventListener("message", receive);
    port.removeEventListener("messageerror", failed);
    const error = new Error("Inspector cache connection closed");
    for (const read of reads.values()) read.reject(error);
    reads.clear();
    staged.clear();
    ownedWrites.clear();
    for (const [id, subscription] of subscriptions) {
      try {
        if (subscription.started) port.postMessage({ type: "inspect-unsubscribe", id, binding });
      } catch {
        /* Worker close also releases this attachment's subscriptions. */
      }
      try {
        subscription.callback?.(error);
      } catch (callbackError) {
        console.error("Inspector subscription close callback failed", callbackError);
      }
    }
    subscriptions.clear();
  };
  const failed = () => dispose();
  port.addEventListener("message", receive);
  port.addEventListener("messageerror", failed);
  const cacheOnly = (tier?: string | null, options?: string | null) =>
    tier === "local" && options != null && JSON.parse(options).propagation === "local-only";
  const request = (message: Record<string, unknown>): Promise<unknown> => {
    if (closed) return Promise.reject(new Error("Inspector cache connection closed"));
    return new Promise((resolve, reject) => {
      const id = nextRequestId(port);
      reads.set(id, { resolve, reject });
      try {
        port.postMessage({ ...message, id, binding });
      } catch (error) {
        reads.delete(id);
        reject(error);
      }
    });
  };
  return new Proxy(runtime, {
    get(target, property) {
      if (property === "beginTransaction")
        return (kind: string, id: OpenTransactionId) => {
          if (closed) throw new Error("Inspector cache connection closed");
          if (kind !== "mergeable")
            throw new Error("Inspector edits require a mergeable transaction");
          if (staged.has(id)) throw new Error("Inspector transaction already exists");
          staged.set(id, []);
          return id;
        };
      if (property === "rollbackTransaction")
        return async (id: OpenTransactionId) => staged.delete(id);
      if (property === "commitTransaction")
        return async (id: OpenTransactionId) => {
          const edits = staged.get(id);
          if (!edits) throw new Error("Inspector transaction is not open");
          staged.delete(id);
          const txId = (await request({ type: "inspect-commit", edits })) as TxId;
          ownedWrites.add(txId);
          return txId;
        };
      if (property === "waitForTransaction")
        return async (
          pending: TxId | Promise<TxId>,
          tier: string,
          options?: RuntimeWriteWaitOptions,
        ) => {
          const txId = await pending;
          if (!ownedWrites.has(txId)) return target.waitForTransaction(txId, tier, options);
          await Promise.all([options?.ready, request({ type: "inspect-wait", txId, tier })]);
        };
      if (property === "insert" || property === "update" || property === "delete")
        return (...args: unknown[]) => {
          const table = args[0] as string;
          const insert = property === "insert";
          const context = JSON.parse(
            (args[insert ? 2 : property === "update" ? 3 : 2] as string | null) ?? "{}",
          );
          const id = context.transaction_id as OpenTransactionId;
          const edits = staged.get(id);
          if (!edits) throw new Error("Inspector writes require an open transaction");
          if (context.branch_view || context.target_branch_name || context.attribution)
            throw new Error("Inspector edits do not support branch or attributed writes");
          const rowId = (insert ? (args[3] ?? crypto.randomUUID()) : args[1]) as string;
          const values = (insert ? args[1] : args[2]) as Record<string, Value>;
          edits.push(
            structuredClone({
              operation: property,
              table,
              rowId,
              ...(property !== "delete" ? { values } : {}),
              updatedAt: context.updated_at,
            }),
          );
          const receipt = { kind: "staged", openTransactionId: id };
          if (!insert) return receipt;
          const columns = schema[table]?.columns;
          if (!columns) throw new Error(`Unknown Inspector table ${table}`);
          return {
            ...receipt,
            id: rowId,
            values: columns
              .filter((column) => !column.name.startsWith("$"))
              .map((column) => values[column.name] ?? column.default ?? { type: "Null" }),
          };
        };
      if (
        property === "upsert" ||
        property === "restore" ||
        property === "updateLargeValues" ||
        property === "streamingMutation"
      )
        return () => {
          throw new Error("This mutation is not supported by the Inspector");
        };

      if (property === "query")
        return (
          query: string,
          session?: string | null,
          tier?: string | null,
          options?: string | null,
        ) => {
          if (!cacheOnly(tier, options)) return target.query(query, session, tier, options);
          if (closed) return Promise.reject(new Error("Inspector cache connection closed"));
          return request({ type: "inspect-query", query, options });
        };
      if (property === "createSubscription")
        return (
          query: string,
          session?: string | null,
          tier?: string | null,
          options?: string | null,
        ) => {
          if (!cacheOnly(tier, options))
            return target.createSubscription(query, session, tier, options);
          if (closed) throw new Error("Inspector cache connection closed");
          const id = nextRequestId(port);
          subscriptions.set(id, { query, options, started: false });
          return id;
        };
      if (property === "executeSubscription")
        return (id: number, callback: (value: RuntimeSubscriptionDelta | Error) => void) => {
          const subscription = subscriptions.get(id);
          if (!subscription) return target.executeSubscription(id, callback);
          subscription.callback = callback;
          if (!subscription.started) {
            subscription.started = true;
            try {
              port.postMessage({
                type: "inspect-subscribe",
                id,
                binding,
                query: subscription.query,
                options: subscription.options,
              });
            } catch (error) {
              subscriptions.delete(id);
              subscription.started = false;
              throw error;
            }
          }
        };
      if (property === "unsubscribe")
        return (id: number) => {
          const subscription = subscriptions.get(id);
          if (!subscription) return target.unsubscribe(id);
          subscriptions.delete(id);
          if (subscription.started) port.postMessage({ type: "inspect-unsubscribe", id, binding });
        };
      if (property === "close")
        return () => {
          dispose();
          return target.close();
        };
      if (property === "discard")
        return () => {
          dispose();
          return target.discard();
        };
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}
