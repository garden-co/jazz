import { NATIVE_ARTIFACT_MISMATCH } from "./native-artifact-compatibility.js";

/**
 * Why a session given a local-first client (`client`) runs thin instead:
 * `snapshot.localFirstUnavailable`.
 *
 * - `wasm-mismatch`: the WASM the page loaded is not the one this jazz-tools
 *   build expects (its fingerprint differs, or it has none). A stale or
 *   mismatched `.wasm` asset, a cache serving an old one, or packages of
 *   different Jazz versions. Retrying won't help until the asset matches.
 * - `load-failed`: the local-first client failed to load or open for another
 *   reason (a chunk that no longer loads, the network, storage).
 */
export interface LocalFirstUnavailable {
  readonly reason: "wasm-mismatch" | "load-failed";
  /** One line for a status display, e.g. `local-first unavailable: WASM fingerprint mismatch`. */
  readonly message: string;
  /** The fingerprint this jazz-tools build expects (`wasm-mismatch` only). */
  readonly expectedFingerprint?: string;
  /** The loaded WASM's fingerprint, when it has one (`wasm-mismatch` only). */
  readonly actualFingerprint?: string;
  readonly error: Error;
}

const MISMATCH_MESSAGE =
  /Jazz WASM artifact (?:ABI mismatch|is stale or incompatible|is incomplete despite matching fingerprint)/;

function mismatchOf(error: unknown, depth = 0): Record<string, unknown> | undefined {
  if (!error || typeof error !== "object" || depth > 4) return undefined;
  const candidate = error as Record<string, unknown>;
  // The class may not survive a hop from a worker or another chunk's copy of
  // jazz-tools; its code and message do.
  if (
    (candidate.code === NATIVE_ARTIFACT_MISMATCH && candidate.kind !== "NAPI") ||
    MISMATCH_MESSAGE.test(String(candidate.message ?? ""))
  ) {
    return candidate;
  }
  return mismatchOf(candidate.cause, depth + 1);
}

/** @internal Name why the local-first client could not open. */
export function describeLocalFirstFailure(error: unknown): LocalFirstUnavailable {
  const asError = error instanceof Error ? error : new Error(String(error));
  const mismatch = mismatchOf(error);
  if (mismatch) {
    const expected = typeof mismatch.expected === "string" ? mismatch.expected : undefined;
    const actual = typeof mismatch.actual === "string" ? mismatch.actual : undefined;
    return Object.freeze({
      reason: "wasm-mismatch",
      message: "local-first unavailable: WASM fingerprint mismatch",
      ...(expected ? { expectedFingerprint: expected } : {}),
      ...(actual ? { actualFingerprint: actual } : {}),
      error: asError,
    });
  }
  return Object.freeze({
    reason: "load-failed",
    message: "local-first unavailable: the local-first client failed to load",
    error: asError,
  });
}

/**
 * @internal The console line for a session that runs thin because of `info`:
 * `generic` unless the cause is a WASM mismatch, which it names.
 */
export function localFirstUnavailableWarning(
  info: LocalFirstUnavailable,
  generic: string,
  running: string,
): string {
  if (info.reason !== "wasm-mismatch") return generic;
  return (
    `Jazz ${info.message} (expected ${info.expectedFingerprint ?? "unknown"}, ` +
    `got ${info.actualFingerprint ?? "none"}); ${running}, without offline use or local reads. ` +
    "The .wasm this page loaded does not match this jazz-tools build: serve the jazz-wasm " +
    "asset from the same install (check caches and CDNs), reinstall matching Jazz package " +
    "versions, or in the Jazz monorepo rebuild the generated bindings."
  );
}

const unavailable = new WeakMap<object, LocalFirstUnavailable>();

/** @internal Record why `client` (a session's client) runs thin. */
export function markLocalFirstUnavailable(client: object, info: LocalFirstUnavailable): void {
  unavailable.set(client, info);
}

/** @internal The client upgraded, or a new attempt is under way. */
export function clearLocalFirstUnavailable(client: object): void {
  unavailable.delete(client);
}

/** @internal Why `client` runs thin although a local-first client was given. */
export function localFirstUnavailableOf(client: unknown): LocalFirstUnavailable | undefined {
  return client && typeof client === "object" ? unavailable.get(client) : undefined;
}
