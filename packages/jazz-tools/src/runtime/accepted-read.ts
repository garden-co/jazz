/** @internal Opt in during creation, before the existing core batch opens. */
export type TransactionReadCapability = "accepted-evidence";

/** @internal Authority coordinates stay exact across JavaScript's boundary. */
export type AcceptedVersion = { transactionId: string; position: string };
/** @internal Content can predate deletion or restoration of the row's state. */
export type AcceptedRowEvidence = {
  rowId: string;
  state: AcceptedVersion;
  content: AcceptedVersion;
};
/** @internal Evidence does not replace the transaction's commit-time validation. */
export type AcceptedRows<T> = {
  rows: T[];
  through: string;
  schema: string;
  evidence: AcceptedRowEvidence[];
};

/** @internal Same host-number bound as Rust binding_args::checked_u64. */
export function validateAcceptedReadTimeout(timeoutMs: number): void {
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0)
    throw new TypeError("timeoutMs must be a nonnegative safe integer");
}
