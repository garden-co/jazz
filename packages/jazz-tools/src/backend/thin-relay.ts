/**
 * Thin sessions' transactions, served by the backend (SPEC 8.14).
 *
 * A thin client's socket is relayed to the core as any client's is, except
 * that its transaction requests stay here: the native router splits the
 * client's frames, in the client's order, and the session's own client
 * serves the transaction ones. That client lives in this process, in
 * memory, linked to the core as the socket's session (the same vouched
 * session, guest token or trusted backend link), so the core reads and
 * checks for it exactly as for the session, and only ever accepts finished
 * transactions from it. The core never holds a transaction open between
 * calls.
 *
 * The client starts on the session's first transaction request and ends
 * with the socket, rolling back what it left open.
 */
import { NapiThinRouter } from "jazz-napi";

/** How long a transaction request waits for the core before it is answered without it. */
export const THIN_REQUEST_TIMEOUT_MS = 30_000;

/**
 * How long a thin socket's requests may wait on an answer, from the core or
 * the session's client, since the last one came, before the socket is
 * closed (1011): the answer is lost, and the client reconnects.
 */
export const THIN_ANSWER_DEADLINE_MS = 60_000;

/** The code of a commit whose outcome the backend could not learn: it may have applied. */
export const COMMIT_OUTCOME_UNKNOWN = "commit_outcome_unknown";

/** The session's own client, as the relay uses it. */
export interface ThinTransactionClient {
  /** Serve a transaction request's frame; the answer is its response frame. */
  serve(request: Uint8Array): Promise<Uint8Array>;
  /** End the session's open transactions, then the client. */
  close(): Promise<void>;
}

export interface ThinRelayOptions {
  /**
   * Open the session's client for the schema the thin client bound (its
   * public schema JSON), or refuse with why. `null` when the backend serves
   * as many sessions' transactions as it may.
   */
  openClient(schema: Uint8Array): ThinTransactionClient | null | { refused: string };
  /** Send a message to the core; false once it went away. */
  toCore(message: Uint8Array): boolean | Promise<boolean>;
  /** Send a message to the client. */
  toClient(message: Uint8Array): void;
  /** Close the client's socket. */
  close(code: number, reason: string): void;
  requestTimeoutMs?: number;
  answerDeadlineMs?: number;
}

/** One thin client's socket, past its Hello. */
export class ThinRelay {
  private readonly router = new NapiThinRouter();
  private client: ThinTransactionClient | undefined;
  /** The session's transaction requests, served one at a time, in order. */
  private serving: Promise<void> = Promise.resolve();
  /** Messages to the core, sent in order. */
  private sending: Promise<boolean> = Promise.resolve(true);
  private deadline: ReturnType<typeof setTimeout> | undefined;
  private ended = false;

  constructor(private readonly options: ThinRelayOptions) {}

  /** Take one client message. False when the socket is broken. */
  fromClient(message: Uint8Array): Promise<boolean> {
    if (this.ended) return Promise.resolve(false);
    try {
      this.router.fromClient(message);
    } catch {
      this.options.close(1008, "undecodable thin frame");
      this.end();
      return Promise.resolve(false);
    }
    return this.release();
  }

  /** Note one message the core sent the client (the caller relays it). */
  fromCore(message: Uint8Array): void {
    if (this.ended) return;
    if (this.router.fromCore(message)) this.progressed();
    void this.release();
  }

  /** The socket ended: what the session left open rolls back. */
  end(): void {
    if (this.ended) return;
    this.ended = true;
    clearTimeout(this.deadline);
    const client = this.client;
    this.client = undefined;
    // At once, not after the request it may be serving: one the core never
    // answers would otherwise hold the client (and its place) for good.
    // Closing drops that request, and the transactions it left open roll
    // back with the client.
    if (client) void client.close().catch(() => undefined);
  }

  /** An answer came: the wait for the next goes on from now. */
  private progressed(): void {
    clearTimeout(this.deadline);
    this.deadline = undefined;
    this.watch();
  }

  /** Close the socket if its requests wait on an answer that doesn't come. */
  private watch(): void {
    if (this.ended || this.deadline !== undefined || !this.router.isWaiting()) return;
    this.deadline = setTimeout(() => {
      this.deadline = undefined;
      if (this.ended || !this.router.isWaiting()) return;
      this.options.close(1011, "an answer never came");
      this.end();
    }, this.options.answerDeadlineMs ?? THIN_ANSWER_DEADLINE_MS);
    (this.deadline as { unref?: () => void }).unref?.();
  }

  /** Move what the router releases: core messages in order, transaction requests to the client. */
  private release(): Promise<boolean> {
    for (const step of this.router.ready()) {
      if (step.core) {
        const message = step.core;
        this.sending = this.sending.then(async (ok) => ok && (await this.options.toCore(message)));
      } else if (step.backend && step.id !== undefined && step.id !== null) {
        this.serve(step.id, step.backend, step.commit === true);
      }
    }
    this.watch();
    return this.sending;
  }

  private serve(id: number, request: Uint8Array, commit: boolean): void {
    const client = this.sessionClient(id);
    if (!client) return;
    const timeout = this.options.requestTimeoutMs ?? THIN_REQUEST_TIMEOUT_MS;
    this.serving = this.serving.then(async () => {
      if (this.ended) return;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timedOut = new Promise<null>((resolve) => {
        timer = setTimeout(() => resolve(null), timeout);
      });
      // A request the core keeps waiting is answered at the timeout, but
      // the session's client serves one request at a time: the next one
      // waits for this one to end (the answer deadline closes a socket
      // whose requests wait too long).
      const call = client.serve(request);
      let answer: Uint8Array | null | Error;
      try {
        answer = await Promise.race([call, timedOut]);
      } catch (error) {
        answer = error instanceof Error ? error : new Error(String(error));
      } finally {
        clearTimeout(timer);
      }
      const ended =
        answer === null
          ? call.then(
              () => undefined,
              () => undefined,
            )
          : undefined;
      if (this.ended) return;
      let messages: Uint8Array[];
      if (answer instanceof Uint8Array) {
        messages = this.router.answer(answer);
      } else if (answer === null) {
        // A commit that timed out may still reach the core: its outcome is
        // unknown, never a failure the client may retry.
        messages = commit
          ? this.router.refuse(
              id,
              COMMIT_OUTCOME_UNKNOWN,
              `the core did not confirm the commit within ${timeout / 1000}s: it may have applied, or may still`,
            )
          : this.router.refuse(
              id,
              "not_observed",
              `the core did not answer within ${timeout / 1000}s`,
            );
      } else {
        messages = this.router.refuse(id, "storage", answer.message);
      }
      for (const message of messages) this.options.toClient(message);
      this.progressed();
      void this.release();
      await ended;
    });
  }

  /** The session's client, started on its first transaction request. */
  private sessionClient(id: number): ThinTransactionClient | undefined {
    if (this.client) return this.client;
    const schema = this.router.schema();
    const refuse = (code: string, message: string) => {
      for (const answer of this.router.refuse(id, code, message)) this.options.toClient(answer);
      this.progressed();
      void this.release();
    };
    if (!schema) {
      refuse("Argument", "bind the connection to a schema first");
      return undefined;
    }
    let opened: ReturnType<ThinRelayOptions["openClient"]>;
    try {
      opened = this.options.openClient(schema);
    } catch (error) {
      refuse(
        "storage",
        `the backend could not open the session's client: ${error instanceof Error ? error.message : String(error)}`,
      );
      return undefined;
    }
    if (opened === null) {
      refuse(
        "backpressure",
        "the backend serves as many sessions' transactions as it may; try again later",
      );
      return undefined;
    }
    if ("refused" in opened) {
      refuse("Argument", opened.refused);
      return undefined;
    }
    this.client = opened;
    return opened;
  }
}

/** Whether a client's Hello asks for thin results: a thin client, whose transactions its backend serves. */
export function asksThinResults(message: Uint8Array): boolean {
  return NapiThinRouter.asksThinResults(message);
}
