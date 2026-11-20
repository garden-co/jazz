/**
 * The `db.encryption` of a thin `Db` of an app that declares encryption.
 * The thin client holds no keys, so the real handle exists only on the
 * local-first client. This stands in for it: an async call waits for that
 * client (starting the load), then runs on the real handle. A handle the app
 * took while thin keeps working after the upgrade.
 *
 * Type imports only: it ships in the thin first load.
 */
import type { Encryption } from "./index.js";

export interface PendingEncryptionHost {
  /** The real handle, once the local-first client holds it; rejects when that can't happen. */
  resolve(): Promise<Encryption>;
  /** What a synchronous call throws: it can't wait. Starts the load. */
  refuseSync(operation: string): Error;
  /** Listeners that never attached because the wait failed. */
  onAttachFailed(error: unknown): void;
}

const LISTENERS = new Set(["onReport", "onUnreadable", "onChallenge"]);
/** Calls that return a value at once, not a promise. */
const SYNCHRONOUS = new Set(["recovery.create"]);
/** The runtime's own lifecycle: nothing to wait for on a thin client. */
const LIFECYCLE = new Set(["start", "stop"]);

export function pendingEncryption(host: PendingEncryptionHost): Encryption {
  const at = (path: readonly string[]): unknown =>
    new Proxy(function () {} as unknown as object, {
      get(_target, key) {
        if (typeof key !== "string" || key === "then") return undefined;
        return at([...path, key]);
      },
      apply(_target, _this, args: unknown[]) {
        const name = path.join(".");
        if (LIFECYCLE.has(name)) return undefined;
        if (SYNCHRONOUS.has(name)) throw host.refuseSync(name);
        if (path.length === 1 && LISTENERS.has(path[0]!)) return listen(path[0]!, args[0]);
        return host.resolve().then((real) => {
          let owner: unknown = real;
          let member: unknown = real;
          for (const key of path) {
            owner = member;
            member = (member as Record<string, unknown>)[key];
          }
          return (member as (...a: unknown[]) => unknown).apply(owner, args);
        });
      },
    });

  /** Attaches once the real handle exists; the returned function detaches either way. */
  const listen = (name: string, listener: unknown): (() => void) => {
    let stopped = false;
    let detach: (() => void) | undefined;
    host.resolve().then(
      (real) => {
        if (stopped) return;
        detach = (real as unknown as Record<string, (l: unknown) => () => void>)[name]!(listener);
      },
      (error: unknown) => {
        if (!stopped) host.onAttachFailed(error);
      },
    );
    return () => {
      stopped = true;
      detach?.();
      detach = undefined;
    };
  };

  return at([]) as Encryption;
}
