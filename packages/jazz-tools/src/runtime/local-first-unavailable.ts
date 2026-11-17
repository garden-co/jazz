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
  /**
   * An export the loaded WASM lacks although its fingerprint matches: a
   * partial build (`wasm-mismatch` only).
   */
  readonly missingExport?: string;
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
    // Fields when the class survived; otherwise its message still says them.
    const text = String(mismatch.message ?? "");
    const field = (name: string) =>
      typeof mismatch[name] === "string" ? (mismatch[name] as string) : undefined;
    const pair = /expected (\S+?), got (\S+?)\.(?:\s|$)/.exec(text);
    const expected =
      field("expected") ??
      pair?.[1] ??
      /missing nativeArtifactFingerprint \(expected (\S+?)\)/.exec(text)?.[1];
    const missingExport =
      field("missingExport") ??
      /despite matching fingerprint: missing (\S+?)\.(?:\s|$)/.exec(text)?.[1];
    // A partial build's own fingerprint is the expected one: not a mismatch to report.
    const actual = missingExport ? undefined : (field("actual") ?? pair?.[2]);
    return Object.freeze({
      reason: "wasm-mismatch",
      message: missingExport
        ? "local-first unavailable: incomplete WASM build"
        : "local-first unavailable: WASM fingerprint mismatch",
      ...(expected ? { expectedFingerprint: expected } : {}),
      ...(actual ? { actualFingerprint: actual } : {}),
      ...(missingExport ? { missingExport } : {}),
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
  const { expectedFingerprint: expected, actualFingerprint: actual, missingExport } = info;
  const detail = missingExport
    ? ` (its fingerprint matches, but it lacks ${missingExport})`
    : expected && actual
      ? ` (expected ${expected}, got ${actual})`
      : expected
        ? ` (expected ${expected}; the loaded WASM has no fingerprint)`
        : "";
  return (
    `Jazz ${info.message}${detail}; ${running}, without offline use or local reads. ` +
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
