import type { OpenTransactionId, TxId } from "../client.js";
import type { BrowserInitializationCommand } from "./browser-worker-protocol.js";
import {
  createInitializationSeal,
  type InitializationSeal,
} from "../provisional-initialization.js";

type WithHandle = Extract<BrowserInitializationCommand, { handle: string }>;
export type DurableBatchCommand = WithHandle extends infer C
  ? C extends WithHandle
    ? Omit<C, "handle">
    : never
  : never;
export type DurableOwnerRequest = (command: BrowserInitializationCommand) => Promise<unknown>;

/** One transport facade for a batch opened in the real worker from the outset. */
export class RemoteDurableTransaction {
  readonly ready: Promise<void>;
  private readonly opening: Promise<string>;
  private tail: Promise<void>;
  private failed = false;
  private failure: unknown;
  private accepting = true;
  private retired = false;
  private sealed = false;
  private cancelling?: Promise<void>;
  private submitted = false;
  private publicationFailed = false;
  private readonly waitIntents = new Map<
    string,
    { tier: "local" | "global"; signal: AbortSignal; submitted?: boolean }
  >();

  constructor(
    readonly id: OpenTransactionId,
    command: Extract<BrowserInitializationCommand, { operation: "begin" }>,
    private readonly request: DurableOwnerRequest,
    private readonly assertCurrent: () => void,
  ) {
    // Clone at admission, before any await: caller-owned schema/claims/bytes
    // cannot later change the actual opening request or a queued operation.
    const captured = structuredClone(command);
    this.opening = Promise.resolve().then(async () => {
      this.assertCurrent();
      const handle = await request(captured);
      if (typeof handle !== "string" || !handle)
        throw new Error("Durable owner omitted its batch handle");
      // A cancelled opening must retain this handle so cancel() can retire it.
      return handle;
    });
    this.ready = this.opening.then(() => {
      this.check();
    });
    this.tail = this.ready;
    void this.tail.catch(() => {});
  }

  run(command: DurableBatchCommand): Promise<unknown> {
    if (!this.accepting || this.retired)
      throw new Error("Durable batch no longer accepts operations");
    this.check();
    const captured = structuredClone(command);
    const result = this.tail
      .then(async () => {
        this.check();
        if (this.failed) throw this.failure;
        return this.request({ ...captured, handle: await this.opening } as WithHandle);
      })
      .then((value) => {
        this.check();
        return value;
      });
    this.tail = result.then(
      () => {},
      (error) => {
        this.failed = true;
        this.failure = error;
      },
    );
    void result.catch(() => {});
    return result;
  }

  enqueue(command: DurableBatchCommand): void {
    void this.run(command);
  }

  async seal(): Promise<InitializationSeal> {
    if (!this.accepting) throw new Error("Durable batch seal already requested");
    this.accepting = false;
    await this.tail;
    this.check();
    if (this.failed) throw this.failure;
    const reservation = await this.request({ operation: "seal", handle: await this.opening });
    if (typeof reservation !== "string" || !reservation)
      throw new Error("Durable owner omitted the reserved transaction identity");
    this.sealed = true;
    this.check();
    // This is only a page-side identity facade. The native token stays in
    // DurableInitializationOwner's private worker registry.
    return createInitializationSeal(reservation);
  }

  async commit(): Promise<TxId> {
    if (!this.accepting) throw new Error("Durable batch no longer accepts commit");
    this.accepting = false;
    await this.tail;
    this.check();
    if (this.failed) throw this.failure;
    return this.finish("commit");
  }

  async publish(): Promise<TxId> {
    this.check();
    if (!this.sealed) throw new Error("Durable batch has not been sealed");
    this.sealed = false;
    return this.finish("publish");
  }

  registerWait(waitId: string, tier: "local" | "global", signal: AbortSignal): boolean {
    if (this.retired || this.submitted || signal.aborted) return false;
    this.waitIntents.set(waitId, { tier, signal });
    return true;
  }

  waitWasSubmitted(waitId: string): boolean {
    return this.waitIntents.get(waitId)?.submitted === true;
  }
  forgetWait(waitId: string): void {
    this.waitIntents.delete(waitId);
  }

  private async finish(operation: "publish" | "commit"): Promise<TxId> {
    try {
      const handle = await this.opening;
      this.check();
      this.submitted = true;
      const waitIntents = [...this.waitIntents]
        .filter(([, intent]) => !intent.signal.aborted)
        .map(([waitId, intent]) => {
          intent.submitted = true;
          return { waitId, tier: intent.tier };
        });
      const tx = await this.request({ operation, handle, waitIntents });
      this.check();
      if (typeof tx !== "string" || !tx)
        throw new Error("Durable owner omitted the committed transaction identity");
      this.retired = true;
      return tx as TxId;
    } catch (error) {
      // A refused command can leave its actual native batch open. Permit
      // exact-handle cleanup once the request has settled, never replay or
      // claim that an already-started publication was rolled back.
      this.publicationFailed = true;
      throw error;
    }
  }

  cancel(): Promise<void> {
    if (this.cancelling) return this.cancelling;
    if (this.retired) return Promise.resolve();
    if (this.submitted && !this.publicationFailed)
      return Promise.reject(
        new Error("Durable publication already started; recover its exact status"),
      );
    this.retired = true;
    this.accepting = false;
    // Do not await tail: it may be a pending native read which only this
    // cancellation can unwind. The worker owns exact rollback and token cleanup.
    this.cancelling = this.opening.then(async (handle) => {
      await this.request({ operation: "cancel", handle });
    });
    void this.cancelling.catch(() => {});
    return this.cancelling;
  }

  private check(): void {
    this.assertCurrent();
    if (this.retired) throw new Error("Durable batch owner retired");
  }
}
