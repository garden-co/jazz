const PERSISTED_TITLE_PREFIX = "high-level-foreground-row:";

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

/** Require the exact seed-run marker; older rows are intentionally insufficient. */
export function assertPersistedTitleForRun(titles: readonly string[], runNonce: string): void {
  const expected = persistedTitleForRun(runNonce);
  if (!titles.includes(expected)) {
    throw new Error(
      "high-level React Native foreground did not materialize this run's prior process persisted row",
    );
  }
}
