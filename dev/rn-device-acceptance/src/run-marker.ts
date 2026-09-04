const PERSISTED_TITLE_PREFIX = "high-level-foreground-row:";
const CANONICAL_UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

/** The acceptance host issues its cross-restart nonce with `randomUUID()`. */
export function isDeviceRunNonce(value: unknown): value is string {
  return typeof value === "string" && CANONICAL_UUID_V4.test(value);
}

/**
 * Derive the exact row marker for one host-issued device-acceptance run.
 *
 * The driver gives the same unpredictable nonce to the seed and verification
 * launches and the trusted fixture includes it in each accepted receipt.
 * Retained app data from an earlier install therefore cannot satisfy the
 * fresh-process persistence assertion.
 */
export function persistedTitleForRun(runNonce: string): string {
  if (typeof runNonce !== "string" || runNonce.trim().length === 0) {
    throw new Error("device acceptance requires a non-empty host-issued run nonce");
  }
  return `${PERSISTED_TITLE_PREFIX}${runNonce}`;
}

/**
 * Reuse the driver's UUID bytes as a run-unique fixture row ID. Device app
 * reinstalls deliberately retain SQLite, so a fixed row ID would turn a
 * subscription-insert receipt into an update of stale data from an older run.
 */
export function rowIdForRun(runNonce: string): Uint8Array {
  if (!isDeviceRunNonce(runNonce)) {
    throw new Error("device acceptance requires a canonical UUIDv4 host-issued run nonce");
  }
  const compact = runNonce.replaceAll("-", "");
  return Uint8Array.from(compact.match(/../g)!, (pair) => Number.parseInt(pair, 16));
}

/** Require the exact seed-run marker; older rows are intentionally insufficient. */
export function assertPersistedTitleForRun(titles: readonly string[], runNonce: string): void {
  const expected = persistedTitleForRun(runNonce);
  if (!titles.includes(expected)) {
    throw new Error(
      "high-level React Native foreground did not materialize this run's prior process persisted row",
    );
  }
}
