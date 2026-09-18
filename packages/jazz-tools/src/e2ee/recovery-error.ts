const messages = {
  "recovery-material-unusable": "Recovery material could not be validated",
  "recovery-root-mismatch": "Recovery material does not match an accepted recovery root",
  "recovery-delivery-missing": "No authenticated recovery delivery for the current account epoch",
  "recovery-delivery-unusable": "No authenticated recovery delivery for the current account epoch",
  "recovery-protector-missing": "No usable local-first recovery protector",
  "recovery-protector-unusable": "No usable local-first recovery protector",
  "recovery-state-changed": "Account epoch changed during recovery inspection; retry",
} as const;

export type E2eeRecoveryErrorCode = keyof typeof messages;

/** Stable reasons with fixed text; never attach private material or an adapter's cause. */
export class E2eeRecoveryError extends Error {
  constructor(readonly code: E2eeRecoveryErrorCode) {
    super(messages[code]);
    this.name = "E2eeRecoveryError";
  }
}
