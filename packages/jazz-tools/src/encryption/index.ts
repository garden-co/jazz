/**
 * End-to-end encryption for an app on Node: the handle an app uses
 * (`db.encryption`), the private store it supplies, and the errors.
 *
 * Keys and decryption live in Jazz's runtime; this module is the app's side:
 * device approval, device removal, recovery and the reports of what could
 * not be read. Ordinary reads and writes of encrypted columns need nothing
 * from it.
 */
import { RecoveryPhrase } from "../local-first-auth/recovery-phrase.js";
import { parseSecret } from "../local-first-auth/secret.js";

export {
  memoryPrivateStore,
  servePrivateStore,
  PrivateStoreConflictError,
  PrivateStoreWipedError,
} from "./private-store.js";
export type { PrivateStore, StoredDocument, StoreTarget } from "./private-store.js";
export { unreadableRows } from "./unreadable.js";
export type { UnreadableRow } from "./unreadable.js";

/** The account's state as this device sees it. */
export type AccountStatus = "active" | "awaiting-approval" | "reenrolled";

/** `db.encryption` was used on an app whose files declare no encryption. */
export class EncryptionNotDeclaredError extends Error {
  constructor() {
    super("This app does not declare encryption: no table of its app files is encrypted.");
    this.name = "EncryptionNotDeclaredError";
  }
}

/** The app declares encryption but supplied no private store. */
export class EncryptionStoreRequiredError extends Error {
  constructor() {
    super(
      "This app declares end-to-end encryption, so it needs a private store for the device's keys: pass `encryption: { store }` (see the encryption docs for a store to start from).",
    );
    this.name = "EncryptionStoreRequiredError";
  }
}

/** A failure of an encryption call; `code` is stable (`approval-answered`, `approval-code-mismatch`, `no-challenge`, `store`, `closed`, `protocol`, ...). */
export class EncryptionError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.name = "EncryptionError";
    this.code = code;
  }
}

export interface DeviceInfo {
  id: string;
  status: "active" | "waiting";
  /** Whether this is the device that asked. */
  isThisDevice: boolean;
}

export interface RemovalOutcome {
  deviceId: string;
  error: string | null;
}

/** What Jazz reports about encrypted data, as it happens. */
export type EncryptionReport =
  | {
      type: "unreadable";
      table: string;
      id: string | null;
      column: string | null;
      code: string;
      space: { table: string; id: string } | null;
    }
  | { type: "challenged" }
  /**
   * Reports Jazz kept for the app while nothing took them, past its limit:
   * `count` older ones were dropped (a challenge never is).
   */
  | { type: "reports-dropped"; count: number }
  | { type: string; [key: string]: unknown };

/** The native calls the handle is built on. */
export interface EncryptionNative {
  encryptionCall(op: string, args?: Record<string, unknown> | null): Promise<string>;
  takeEncryptionReports(): string[];
  /** The loop's account status and last error, as JSON text. */
  encryptionState(): string;
}

export interface EncryptionOptions {
  /**
   * The device account's secret (`jazz-auth-v1:...`), when the account is
   * a device account: it is its recovery secret, so the device registers recovery
   * and recovers with it by itself. Read from the session when needed, never
   * stored by encryption.
   */
  accountSecret?: () => string | undefined | Promise<string | undefined>;
  /** How often to look for reports to deliver, in milliseconds. */
  reportIntervalMs?: number;
}

const toHex = (bytes: Uint8Array) =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

function failure(error: unknown): EncryptionError {
  const message = error instanceof Error ? error.message : String(error);
  const match = /^([a-z-]+): ([\s\S]*)$/.exec(message);
  return match ? new EncryptionError(match[1]!, match[2]!) : new EncryptionError("error", message);
}

/** The handle an app uses; see the docs. */
export class Encryption {
  readonly devices: Devices;
  readonly recovery: Recovery;
  readonly keys: Keys;
  readonly members: Members;
  private readonly listeners = new Set<(report: EncryptionReport) => void>();
  private timer: ReturnType<typeof setInterval> | undefined;

  constructor(
    private readonly native: EncryptionNative,
    private readonly options: EncryptionOptions = {},
  ) {
    const call = <T>(op: string, args: Record<string, unknown> = {}) => this.call<T>(op, args);
    this.devices = new Devices(call);
    this.recovery = new Recovery(call, options);
    this.keys = new Keys(call);
    this.members = new Members(call);
  }

  private async call<T>(op: string, args: Record<string, unknown>): Promise<T> {
    try {
      return JSON.parse(await this.native.encryptionCall(op, args)) as T;
    } catch (error) {
      throw failure(error);
    }
  }

  /** This installation's device ID. */
  deviceId(): Promise<string> {
    return this.call("deviceId", {});
  }

  /**
   * Verifies the account, enrols this device or founds the account's first
   * device, and, for a device account, registers recovery or recovers
   * with the account secret. Called for you when the database opens; call it
   * to wait for the result. The first verification belongs to the runtime's
   * own loop, which tries again with a growing wait while the server cannot
   * be reached or has not yet accepted what this device published; this
   * waits for it, up to `timeoutMs` (default 30 seconds), then fails.
   * Later calls verify again at once.
   */
  async ready(options: { timeoutMs?: number } = {}): Promise<AccountStatus> {
    return this.verify(Date.now() + (options.timeoutMs ?? 30_000));
  }

  /** The loop's verification, once it has one (see {@link ready}). */
  private async verified(deadline: number): Promise<AccountStatus> {
    for (;;) {
      const state = JSON.parse(this.native.encryptionState()) as {
        status: AccountStatus | null;
        error: string | null;
      };
      if (state.status !== null) return state.status;
      if (Date.now() >= deadline) {
        throw failure(new Error(state.error ?? "database: the account was not verified in time"));
      }
      await new Promise((resolve) => {
        const timer = setTimeout(resolve, 100);
        (timer as { unref?: () => void }).unref?.();
      });
    }
  }

  private verifying: Promise<AccountStatus> | undefined;

  /** One verification at a time: a second caller shares the one in flight (a recovery spends the device's approval nonce once). */
  private verify(deadline: number): Promise<AccountStatus> {
    return (this.verifying ??= this.verifyOnce(deadline).finally(() => {
      this.verifying = undefined;
    }));
  }

  private async verifyOnce(deadline: number): Promise<AccountStatus> {
    const known = JSON.parse(this.native.encryptionState()) as { status: AccountStatus | null };
    let status =
      known.status === null
        ? await this.verified(deadline)
        : await this.call<AccountStatus>("ready", {});
    const secret = await this.options.accountSecret?.();
    if (secret !== undefined) {
      const hex = toHex(parseSecret(secret));
      if (status === "active") await this.call("recoveryRegister", { secret: hex });
      else if (status === "awaiting-approval") {
        // A device with no other to approve it recovers with the account
        // secret; without a registered root there is nothing to recover from.
        try {
          status = await this.call<AccountStatus>("recoveryRestore", { secret: hex });
        } catch {
          // Stays awaiting approval.
        }
      }
    }
    return status;
  }

  private runner: { stopped: boolean } | undefined;

  /**
   * Starts the verification's follow-up in the background: for a device
   * account, registering recovery or recovering with the account secret once
   * the runtime's loop has verified the account. A refusal that will not pass
   * is reported (`ready-failed`). Called for you when the database opens.
   */
  start(): void {
    if (this.runner) return;
    const runner = { stopped: false };
    this.runner = runner;
    void (async () => {
      while (!runner.stopped) {
        try {
          await this.ready({ timeoutMs: 60_000 });
          return;
        } catch (error) {
          if (runner.stopped) return;
          const failed = failure(error);
          // The loop owns the retries; only the wait for its first answer ran out.
          if (failed.code === "database" || failed.code === "store") continue;
          this.emit({ type: "ready-failed", code: failed.code, message: failed.message });
          return;
        }
      }
    })();
  }

  /** Stops the background verification and the delivery of reports. */
  stop(): void {
    if (this.runner) this.runner.stopped = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    this.listeners.clear();
  }

  private emit(report: EncryptionReport) {
    for (const listener of this.listeners) listener(report);
  }

  /** Listens to reports; returns the function that stops it. */
  onReport(listener: (report: EncryptionReport) => void): () => void {
    this.listeners.add(listener);
    this.timer ??= setInterval(() => this.deliver(), this.options.reportIntervalMs ?? 25);
    (this.timer as { unref?: () => void }).unref?.();
    return () => {
      this.listeners.delete(listener);
      if (this.listeners.size === 0 && this.timer) {
        clearInterval(this.timer);
        this.timer = undefined;
      }
    };
  }

  /**
   * The reports of rows left out because they could not be read, as they
   * happen. The read that left a row out also says so itself: see
   * {@link unreadableRows}.
   */
  onUnreadable(listener: (report: EncryptionReport) => void): () => void {
    return this.onReport((report) => {
      if (report.type === "unreadable") listener(report);
    });
  }

  /** A device awaiting approval was challenged: answer with `devices.answer()`. */
  onChallenge(listener: () => void): () => void {
    return this.onReport((report) => {
      if (report.type === "challenged") listener();
    });
  }

  private deliver() {
    for (const text of this.native.takeEncryptionReports()) {
      this.emit(JSON.parse(text) as EncryptionReport);
    }
  }
}

type Call = <T>(op: string, args?: Record<string, unknown>) => Promise<T>;

/** One approval an approving device started. */
export class ApprovalSession {
  constructor(
    private readonly call: Call,
    readonly deviceId: string,
  ) {}

  /** The code this device computes once the new device answered, else `null`. */
  code(): Promise<string | null> {
    return this.call("approvalCode", { deviceId: this.deviceId });
  }

  /** Approves once the user confirmed both devices show this code. */
  async approve(code: string): Promise<void> {
    await this.call("approve", { deviceId: this.deviceId, code });
  }
}

export class Devices {
  constructor(private readonly call: Call) {}

  /** Devices of this account: those active and those waiting for approval. */
  async list(): Promise<DeviceInfo[]> {
    const [listed, mine] = await Promise.all([
      this.call<Omit<DeviceInfo, "isThisDevice">[]>("devices"),
      this.call<string>("deviceId"),
    ]);
    return listed.map((device) => ({ ...device, isThisDevice: device.id === mine }));
  }

  /** On an approved device: challenge a waiting device. */
  async challenge(deviceId: string): Promise<ApprovalSession> {
    await this.call("challenge", { deviceId });
    return new ApprovalSession(this.call, deviceId);
  }

  /** On the new device: answer the challenge; returns the code to show. */
  answer(): Promise<string> {
    return this.call("answer");
  }

  /** Removes a device and rotates the account. */
  async remove(deviceId: string): Promise<void> {
    await this.call("remove", { deviceId });
  }

  /** This device asks to be removed; another device completes it. */
  async requestRemoval(): Promise<void> {
    await this.call("requestRemoval");
  }

  /** Completes pending removal requests. */
  completeRemovals(): Promise<RemovalOutcome[]> {
    return this.call("completeRemovals");
  }
}

export class Recovery {
  constructor(
    private readonly call: Call,
    private readonly options: EncryptionOptions,
  ) {}

  private async phraseSecret(phrase: string): Promise<string> {
    return toHex(RecoveryPhrase.toBytes(phrase));
  }

  /**
   * For an account the app signs in itself (not a device account): a new recovery
   * secret as a 24-word phrase to keep. It registers nothing. (A device
   * account's recovery is its account secret, handled for you.)
   */
  create(): string {
    this.refuseForDeviceAccount();
    return RecoveryPhrase.fromBytes(crypto.getRandomValues(new Uint8Array(32)));
  }

  /** Registers the recovery root this phrase gives for the account. */
  async register(phrase: string): Promise<void> {
    this.refuseForDeviceAccount();
    await this.call("recoveryRegister", { secret: await this.phraseSecret(phrase) });
  }

  /** On a new device: approves it with the phrase. */
  async restore(phrase: string): Promise<AccountStatus> {
    this.refuseForDeviceAccount();
    return this.call("recoveryRestore", { secret: await this.phraseSecret(phrase) });
  }

  /** The recovery credentials registered for this account. */
  list(): Promise<{ id: string; device: string }[]> {
    return this.call("recoveryList");
  }

  /** Retires a recovery credential by its root ID. */
  async retire(rootId: string): Promise<void> {
    await this.call("recoveryRetire", { rootId });
  }

  private refuseForDeviceAccount() {
    if (this.options.accountSecret) {
      throw new EncryptionError(
        "device-account",
        "A device account's recovery is its account secret: restore the account and encryption follows.",
      );
    }
  }
}

/**
 * The handle of an app that declares no encryption: every use throws. What
 * looks at a value without using it is answered instead: `await` and
 * promise checks (`then`), `JSON.stringify` (`toJSON`), `console.log` and
 * the inspector (`util.inspect`, other symbols), so logging or awaiting
 * `db.encryption` is not an error.
 */
export function notDeclaredEncryption(): Encryption {
  return new Proxy({} as Encryption, {
    get(_target, property) {
      if (property === "then" || property === "toJSON" || typeof property === "symbol") {
        return undefined;
      }
      throw new EncryptionNotDeclaredError();
    },
  });
}

/** A space: a row of the table that holds the space's members and keys. */
export interface SpaceRef {
  table: string;
  id: string;
}

export class Keys {
  constructor(private readonly call: Call) {}

  /**
   * Runs a delivery pass now over every space this device holds keys of:
   * new members, newly approved devices and rotated spaces get their keys.
   * It also runs by itself at start, on reconnect and when a member
   * changes in a space in use.
   */
  deliver(): Promise<{ delivered: number; failed: number }> {
    return this.call("keysDeliver");
  }

  /**
   * Rotates a space's key at once (whoever may update the space's row can).
   * Call it right after removing a member: rotation is otherwise lazy.
   * Returns how many deliveries it wrote.
   */
  async rotate(space: SpaceRef): Promise<number> {
    const done = await this.call<{ delivered: number }>("keysRotate", { ...space });
    return done.delivered;
  }
}

export class Members {
  constructor(private readonly call: Call) {}

  /**
   * The values of the `__e2ee_members` row that makes `accountId` a member of
   * `space`: insert it with the app's own permissions (a space's members
   * are the rows of that table); delete it to remove the member.
   */
  async row(
    space: SpaceRef,
    accountId: string,
  ): Promise<{ scopeId: string; identifier: string; accountId: string }> {
    const at = await this.call<{ scopeId: string; identifier: string }>("spaceRow", { ...space });
    return { ...at, accountId };
  }
}
