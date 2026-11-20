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
  /**
   * The real handle once this Db handed over to the local-first client;
   * undefined while it is still thin. Throws the Db's own closed or replaced
   * error once it shut down without handing over.
   */
  live(): Encryption | undefined;
  /** What a synchronous call throws: it can't wait. Starts the load. */
  refuseSync(operation: string): Error;
  /**
   * Runs `attach` with the real handle once the local-first client is there,
   * for whatever reason; never starts the load. Returns how to cancel.
   */
  whenLocalFirst(attach: (real: Encryption) => void): () => void;
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
        const lifecycle = LIFECYCLE.has(name);
        const synchronous = SYNCHRONOUS.has(name);
        let real: Encryption | undefined;
        try {
          real = host.live();
        } catch (error) {
          // A shut-down Db has nothing to stop; a call that needs it gets its error.
          if (lifecycle) return undefined;
          if (synchronous) throw error;
          return Promise.reject(error);
        }
        if (real) return call(real, path, args);
        if (lifecycle) return undefined;
        if (synchronous) throw host.refuseSync(name);
        if (path.length === 1 && LISTENERS.has(path[0]!)) return listen(path[0]!, args[0]);
        return host.resolve().then((next) => call(next, path, args));
      },
    });

  const call = (real: Encryption, path: readonly string[], args: unknown[]): unknown => {
    let owner: unknown = real;
    let member: unknown = real;
    for (const key of path) {
      owner = member;
      member = (member as Record<string, unknown>)[key];
    }
    return (member as (...a: unknown[]) => unknown).apply(owner, args);
  };

  /** Attaches once the local-first client arrives, by any other cause; starts nothing. */
  const listen = (name: string, listener: unknown): (() => void) => {
    let detach: (() => void) | undefined;
    const cancel = host.whenLocalFirst((real) => {
      detach = (real as unknown as Record<string, (l: unknown) => () => void>)[name]!(listener);
    });
    return () => {
      cancel();
      detach?.();
      detach = undefined;
    };
  };

  return at([]) as Encryption;
}
