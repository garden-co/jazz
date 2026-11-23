/**
 * End-to-end encryption for an app on Node: the handle an app uses
 * (`db.encryption`), the private store it supplies, and the errors.
 *
 * Keys and decryption live in Jazz's runtime; this module is the app's side:
 * device approval, device removal, recovery and the reports of what could
 * not be read. Ordinary reads and writes of encrypted columns need nothing
 * from it.
 */
import { runtimeRandomBytes } from "../runtime/runtime-entropy.js";
import { RecoveryPhrase } from "../local-first-auth/recovery-phrase.js";
import { formatSecret, parseSecret } from "../local-first-auth/secret.js";
import { BrowserPasskeyBackup, PasskeyBackupError } from "../local-first-auth/passkey.js";

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

/**
 * A failure of an encryption call; `code` is stable (`approval-answered`,
 * `approval-code-mismatch`, `no-challenge`, `store`, `closed`, a protocol
 * refusal's own code such as `recovery-root-mismatch` or `stale-state`, and
 * `protocol` for a protocol failure without one, ...).
 */
export class EncryptionError extends Error {
  readonly code:
    | "rotation-needs-key"
    | "not-a-member"
    | "no-account"
    | (string & Record<never, never>);
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
  /** Whether this device has published a request to be removed. */
  removalRequested: boolean;
}

export interface RemovalOutcome {
  deviceId: string;
  error: string | null;
}

/** A reported space carries its stable scope ID and, when known in this schema, its table name. */
export interface ReportSpace {
  scopeId: string;
  id: string;
  table?: string;
}

/** What Jazz reports about encrypted data, as it happens. */
export type EncryptionReport =
  | {
      type: "unreadable";
      table: string;
      id: string | null;
      column: string | null;
      code: string;
      space: ReportSpace | null;
    }
  | { type: "challenged" }
  /** Account verification or the space rotation check failed before a write. */
  | { type: "rotation-check-failed"; space: ReportSpace; reason: string }
  /**
   * A rotation Jazz tried before a write to `space` failed or was refused,
   * for `reason`. The write was sealed under the space's current key all the
   * same, so a member or device removed since may still read it. Told once
   * per failed attempt; the next write tries again.
   */
  | { type: "rotation-failed"; space: ReportSpace; reason: string }
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
  /**
   * The loop's account status, last error and stage, and whether the client
   * can read encrypted rows yet (`readable`), as JSON text.
   */
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
  /** The native host verifies the account and performs device-account recovery. */
  nativeReadiness?: boolean;
  /** The native host's account uses its own secret for encryption recovery. */
  deviceAccount?: boolean;
  /** How often to look for reports to deliver, in milliseconds. */
  reportIntervalMs?: number;
}

/** @internal Takes ownership of the decoded secret passed to the native host. */
export function takeSecretHex(bytes: Uint8Array): string {
  try {
    return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  } finally {
    bytes.fill(0);
  }
}

function failure(error: unknown): EncryptionError {
  const message = error instanceof Error ? error.message : String(error);
  const match = /^([a-z-]+): ([\s\S]*)$/.exec(message);
  if (!match) return new EncryptionError("error", message);
  const [, code, detail] = match;
  // For a protocol refusal, the runtime gives its reason in the code.
  return new EncryptionError(code!, detail === "E2EE operation refused" ? message : detail!);
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
    const timeoutMs = options.timeoutMs ?? 30_000;
    if (this.options.nativeReadiness) return this.call("ready", { timeoutMs });
    return this.verify(Date.now() + timeoutMs);
  }

  /**
   * Resolves once this client can open the encrypted rows its store holds:
   * it knows the app's catalogue identities (synced once, then kept with
   * the store) and has loaded the keys it holds. A device that synced
   * before is readable at once, with or without the server; one that never
   * synced waits, without a deadline, for its first sync. Live queries on
   * encrypted columns wait for the same thing and stay loading until then,
   * so while this has not resolved, a loading query is waiting for the
   * device's first sync. Unlike {@link ready} it does not wait for the
   * account's verification, which can need the server. Rejects only when
   * the runtime can no longer answer.
   */
  async readable(): Promise<void> {
    for (;;) {
      const state = JSON.parse(this.native.encryptionState()) as { readable?: boolean };
      if (state.readable) return;
      await new Promise((resolve) => {
        const timer = setTimeout(resolve, 50);
        (timer as { unref?: () => void }).unref?.();
      });
    }
  }

  /** The loop's verification, once it has one (see {@link ready}). */
  private async verified(deadline: number): Promise<AccountStatus> {
    for (;;) {
      const state = JSON.parse(this.native.encryptionState()) as {
        status: AccountStatus | null;
        error: string | null;
        stage?: string;
      };
      if (state.status !== null) return state.status;
      if (Date.now() >= deadline) {
        throw failure(
          new Error(
            state.error ??
              `database: the account was not verified in time (waiting: ${state.stage ?? "unknown"})`,
          ),
        );
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
      const hex = takeSecretHex(parseSecret(secret));
      // `automatic`: a retired root of the account secret is not an error
      // here, so the device still starts.
      if (status === "active")
        await this.call("recoveryRegister", { secret: hex, automatic: true });
      else if (status === "awaiting-approval") {
        // A device with no other to approve it recovers with the account
        // secret. `null`: nothing to recover from yet (no registered root
        // or delivery for it), so it stays awaiting approval. Any other
        // failure (store, database, a refused recovery) is the caller's.
        const restored = await this.call<AccountStatus | null>("recoveryRestore", {
          secret: hex,
          automatic: true,
        });
        if (restored !== null) status = restored;
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
    return takeSecretHex(RecoveryPhrase.toBytes(phrase));
  }

  /**
   * For an account the app signs in itself (not a device account): a new recovery
   * secret as a 24-word phrase to keep. It registers nothing. (A device
   * account's recovery is its account secret, handled for you.)
   */
  create(): string {
    this.refuseForDeviceAccount();
    const secret = runtimeRandomBytes(32);
    try {
      return RecoveryPhrase.fromBytes(secret);
    } finally {
      secret.fill(0);
    }
  }

  /**
   * For an account the app signs in itself: makes a recovery secret, backs it
   * up and registers it, in one call. Call it from a button the user pressed
   * (creating a passkey needs a user gesture), when `recovery.list()` shows
   * the account has none. Where the browser has passkeys the secret is kept
   * in one, as a credential whose name says it is the encryption recovery
   * key, and `method` is `"passkey"`; otherwise, or if the user declines the
   * passkey, `method` is `"phrase"` and `phrase` holds the 24 words to keep.
   * Either way the recovery is registered.
   */
  async enable(options: {
    /** Names the passkey's relying party, as the app is called to the user. */
    appName: string;
    /** Who the account is, as the passkey shows it (the encryption recovery key of whom). */
    accountName: string;
    /** The passkey's relying-party ID; defaults to the page's host name. */
    appHostname?: string;
  }): Promise<{ method: "passkey" } | { method: "phrase"; phrase: string }> {
    this.refuseForDeviceAccount();
    const secret = runtimeRandomBytes(32);
    try {
      let phrase: string | undefined;
      try {
        await new BrowserPasskeyBackup({
          appName: options.appName,
          appHostname: options.appHostname,
        }).backup(formatSecret(secret), `${options.accountName} (encryption recovery key)`);
      } catch (error) {
        if (!(error instanceof PasskeyBackupError)) throw error;
        phrase = RecoveryPhrase.fromBytes(secret);
      }
      await this.call("recoveryRegister", { secret: takeSecretHex(secret) });
      return phrase === undefined ? { method: "passkey" } : { method: "phrase", phrase };
    } finally {
      secret.fill(0);
    }
  }

  /**
   * On a new device of an account the app signs in itself: reads the recovery
   * secret from the passkey that `enable` made and approves this device with
   * it. Call it from a button (reading a passkey needs a user gesture).
   */
  async restoreFromPasskey(options: {
    appName: string;
    appHostname?: string;
  }): Promise<AccountStatus> {
    this.refuseForDeviceAccount();
    const secret = await new BrowserPasskeyBackup({
      appName: options.appName,
      appHostname: options.appHostname,
    }).restore();
    return this.call("recoveryRestore", { secret: takeSecretHex(parseSecret(secret)) });
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
    if (this.options.accountSecret || this.options.deviceAccount) {
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
   * Returns how many deliveries it wrote, or zero if the space has no key yet.
   */
  async rotate(space: SpaceRef): Promise<number> {
    const done = await this.call<{ delivered: number }>("keysRotate", { ...space });
    return done.delivered;
  }

  /** After user confirmation, rotate from the newest key this device holds. */
  async recoverRotation(space: SpaceRef): Promise<number> {
    const done = await this.call<{ delivered: number }>("keysRecoverRotation", { ...space });
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
