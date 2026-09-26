import type { OpenTransactionId, TransactionalRuntime, TxId, WriteReceipt } from "../client.js";
import { serializeBrowserRelayError } from "./browser-worker-protocol.js";
import {
  CLIENT_BINDING_VERSION,
  deltaForPort,
  queryRowsForPort,
  transactionInContext,
  type ClientBindingCall,
  type ClientBindingEvent,
  type ClientBindingRequest,
} from "./browser-client-binding-protocol.js";

/**
 * One application port admitted by its host to one ordinary client runtime.
 * Owns only this port's handles. It neither creates a replica nor grants server
 * authority. The admitting host remains responsible for auth, storage and life.
 */
export class BrowserClientBindingHost {
  private lastRequest = 0;
  private closed = false;
  private readonly subscriptions = new Map<number, number>();
  private readonly transactions = new Map<OpenTransactionId, { error?: Error }>();
  private readonly writes = new Set<TxId>();
  private closing: Promise<void> | null = null;

  constructor(
    private readonly runtime: TransactionalRuntime,
    private readonly port: MessagePort,
  ) {
    port.addEventListener("message", this.onMessage);
    port.addEventListener("messageerror", this.onMessageError);
    port.start();
  }

  private readonly onMessage = (event: MessageEvent<ClientBindingRequest>): void => {
    if (this.closed) return;
    const message = event.data;
    if (
      !message ||
      message.version !== CLIENT_BINDING_VERSION ||
      !Number.isSafeInteger(message.id) ||
      message.id <= this.lastRequest
    ) {
      // A repeated write request must never execute a second time. Revoke the
      // port, rather than report a fictitious outcome for its previous call.
      void this.revoke(
        new Error("Invalid or repeated client binding request; write outcomes are unknown"),
      ).catch(() => undefined);
      return;
    }
    this.lastRequest = message.id;
    // Invoke immediately in MessagePort order, but do not await one request
    // before admitting the next. A remote query or durability wait must not
    // block an unrelated resident Local query (or the write that will settle it).
    try {
      if (message.type === "client-close") {
        void this.close().then(
          () => this.reply(message.id),
          (error) => this.reply(message.id, undefined, error),
        );
      } else if (message.type === "client-unsubscribe") {
        const handle = this.subscriptions.get(message.handle);
        this.subscriptions.delete(message.handle);
        if (handle !== undefined) this.runtime.unsubscribe(handle);
        this.reply(message.id);
      } else if (message.type === "client-subscribe") {
        this.assertTransactionContext(message.args[3]);
        const handle = this.runtime.createSubscription(...message.args);
        this.subscriptions.set(message.id, handle);
        this.runtime.executeSubscription(handle, (value) => {
          if (this.closed || this.subscriptions.get(message.id) !== handle) return;
          this.send(
            value instanceof Error
              ? { type: "client-delta", id: message.id, error: serializeBrowserRelayError(value) }
              : { type: "client-delta", id: message.id, value: deltaForPort(value) },
          );
        });
      } else if (message.type === "client-call") {
        const result = this.call(message.call);
        void Promise.resolve(result).then(
          (value) => {
            if (!this.closed) this.reply(message.id, value);
          },
          (error) => {
            if (!this.closed) this.reply(message.id, undefined, error);
          },
        );
      } else {
        throw new Error("Unknown client binding request");
      }
    } catch (error) {
      if (message.type === "client-subscribe") {
        this.send({
          type: "client-delta",
          id: message.id,
          error: serializeBrowserRelayError(error),
        });
      } else this.reply(message.id, undefined, error);
    }
  };

  private call(call: ClientBindingCall): unknown {
    switch (call.method) {
      case "query": {
        this.assertTransactionContext(call.args[3]);
        return this.runtime.query(...call.args).then(queryRowsForPort);
      }
      case "beginTransaction": {
        const [kind, id, session] = call.args;
        if (this.transactions.has(id)) throw new Error("Transaction already opened on this port");
        this.runtime.beginTransaction(kind, id, session);
        this.transactions.set(id, {});
        return id;
      }
      case "commitTransaction": {
        const [id] = call.args;
        const state = this.requireTransaction(id);
        if (state.error) {
          const failure = state.error;
          const rollback = this.runtime.rollbackTransaction(id);
          this.transactions.delete(id);
          return Promise.resolve(rollback).then(() => {
            throw failure;
          });
        }
        // Commit is admitted in message order, without yielding before the
        // native call. Earlier staged failures therefore cannot be overtaken.
        const receipt = this.runtime.commitTransaction(id);
        this.transactions.delete(id);
        return Promise.resolve(receipt).then((txId) => {
          if (!this.closed) this.writes.add(txId);
          return txId;
        });
      }
      case "rollbackTransaction": {
        const [id] = call.args;
        this.requireTransaction(id);
        const receipt = this.runtime.rollbackTransaction(id);
        this.transactions.delete(id);
        return receipt;
      }
      case "waitForTransaction": {
        const [id, tier, options] = call.args;
        if (typeof id !== "string" || !this.writes.has(id))
          throw new Error("Write does not belong to this port");
        if (options?.ready !== undefined)
          throw new Error("Host readiness cannot cross the client binding");
        return this.runtime.waitForTransaction(id, tier, options);
      }
      case "insert":
        return this.mutation(call.args[2], () => this.runtime.insert(...call.args));
      case "restore":
        return this.mutation(call.args[3], () => this.runtime.restore(...call.args));
      case "update":
        return this.mutation(call.args[3], () => this.runtime.update(...call.args));
      case "upsert":
        return this.mutation(call.args[3], () => this.runtime.upsert(...call.args));
      case "delete":
        return this.mutation(call.args[2], () => this.runtime.delete(...call.args));
      case "updateLargeValues":
        return this.mutation(call.args[4], () => {
          if (!this.runtime.updateLargeValues)
            throw new Error("Runtime does not support large-value edits");
          return this.runtime.updateLargeValues(...call.args);
        });
      case "streamingMutation": {
        this.assertTransactionContext(call.args[5]);
        if (!this.runtime.streamingMutation)
          throw new Error("Runtime does not support streaming writes");
        return this.runtime.streamingMutation(...call.args).then((value) => this.receipt(value));
      }
      case "requestInsertPermissionAdvice":
        return this.runtime.requestInsertPermissionAdvice?.(...call.args) ?? "unknown";
      case "requestReadPermissionAdvice":
        return this.runtime.requestReadPermissionAdvice?.(...call.args) ?? "unknown";
      case "requestUpdatePermissionAdvice":
        return this.runtime.requestUpdatePermissionAdvice?.(...call.args) ?? "unknown";
      case "requestDeletePermissionAdvice":
        return this.runtime.requestDeletePermissionAdvice?.(...call.args) ?? "unknown";
      default:
        throw new Error("Unknown client binding method");
    }
  }

  private mutation(
    context: string | null | undefined,
    run: () => WriteReceipt,
  ): Promise<WriteReceipt> {
    const state = this.assertTransactionContext(context);
    if (state?.error) throw state.error;
    try {
      return this.receipt(run());
    } catch (error) {
      if (state) state.error ??= error instanceof Error ? error : new Error(String(error));
      throw error;
    }
  }

  private async receipt<T extends WriteReceipt>(value: T): Promise<T> {
    if (value.kind !== "committed") return value;
    const txId = await value.txId;
    if (!this.closed) this.writes.add(txId);
    return { ...value, txId };
  }

  private requireTransaction(id: OpenTransactionId): { error?: Error } {
    const state = this.transactions.get(id);
    if (!state) throw new Error("Transaction does not belong to this port or is no longer open");
    return state;
  }
  private assertTransactionContext(json?: string | null): { error?: Error } | undefined {
    const id = transactionInContext(json);
    return id === undefined ? undefined : this.requireTransaction(id as OpenTransactionId);
  }

  /** Called by the admitting host on auth/storage invalidation or port retirement. */
  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.closed = true;
    this.port.removeEventListener("message", this.onMessage);
    this.port.removeEventListener("messageerror", this.onMessageError);
    for (const handle of this.subscriptions.values()) this.runtime.unsubscribe(handle);
    this.subscriptions.clear();
    const transactions = [...this.transactions.keys()];
    this.transactions.clear();
    this.writes.clear();
    this.closing = Promise.all(
      transactions.map((id) => Promise.resolve().then(() => this.runtime.rollbackTransaction(id))),
    ).then(() => undefined);
    return this.closing;
  }

  /** Fence the binding before an admitting host changes auth/storage lifetime. */
  revoke(error: Error): Promise<void> {
    if (!this.closed) {
      try {
        this.port.postMessage({
          version: CLIENT_BINDING_VERSION,
          type: "client-failed",
          id: 0,
          error: serializeBrowserRelayError(error),
        } satisfies ClientBindingEvent);
      } catch {
        /* The containing connection also owns liveness failure delivery. */
      }
    }
    return this.close();
  }

  private readonly onMessageError = (): void => {
    void this.revoke(new Error("Client binding message error; write outcomes are unknown")).catch(
      () => undefined,
    );
  };
  private reply(id: number, value?: unknown, error?: unknown): void {
    this.send({
      type: "client-result",
      id,
      ...(error === undefined ? { value } : { error: serializeBrowserRelayError(error) }),
    });
  }
  private send(message: Omit<ClientBindingEvent, "version">): void {
    try {
      this.port.postMessage({ ...message, version: CLIENT_BINDING_VERSION });
    } catch (error) {
      void this.revoke(error instanceof Error ? error : new Error(String(error))).catch(
        () => undefined,
      );
    }
  }
}
