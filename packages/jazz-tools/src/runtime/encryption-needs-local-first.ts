/**
 * The thin client holds no keys: an operation that reads or writes an
 * end-to-end encrypted column runs only on the local-first client.
 */

/** Why the operation could not run on the thin client. */
export type EncryptionNeedsLocalFirstReason =
  /** The app gave Jazz no way to load the local-first client. */
  | "no-loader"
  /** The thin `Db` was opened directly, outside a Jazz session. */
  | "direct-thin"
  /** Loading the local-first client failed (see `cause`). */
  | "load-failed"
  /** The local-first client did not load in time. */
  | "timeout"
  /** The operation was in a transaction begun on the thin client. */
  | "transaction"
  /** A synchronous call that can't wait: the load has started, call it again after it. */
  | "not-loaded";

export interface EncryptionNeedsLocalFirstDetails {
  reason: EncryptionNeedsLocalFirstReason;
  /** What the app asked for: `insert`, `update`, `query`, … */
  operation: string;
  table: string;
  /** The encrypted column it named, when it named one. */
  column?: string;
  /** The space insert needs to add its creator in the same transaction. */
  creatorMembership?: boolean;
}

/**
 * An operation touched an end-to-end encrypted column while the app was on
 * the thin client, which holds no keys and so never sends or reads such a
 * column's plaintext. Nothing of the operation was sent.
 */
export class EncryptionNeedsLocalFirstError extends Error {
  readonly reason: EncryptionNeedsLocalFirstReason;
  readonly operation: string;
  readonly table: string;
  readonly column: string | undefined;
  declare readonly creatorMembership?: boolean;

  constructor(details: EncryptionNeedsLocalFirstDetails, options?: ErrorOptions) {
    super(needsLocalFirstMessage(details, options?.cause), options);
    this.name = "EncryptionNeedsLocalFirstError";
    this.reason = details.reason;
    this.operation = details.operation;
    this.table = details.table;
    this.column = details.column;
    if (details.creatorMembership) this.creatorMembership = true;
  }
}

function needsLocalFirstMessage(
  { reason, table, column, creatorMembership }: EncryptionNeedsLocalFirstDetails,
  cause: unknown,
): string {
  const lead = creatorMembership
    ? `Creating a space in \`${table}\` with automatic membership needs the local-first client. Pass \`addCreatorAsMember: false\` to create the space without adding its creator.`
    : column === undefined
      ? `\`${table}\` has end-to-end encrypted columns, which need the local-first client.`
      : `\`${table}.${column}\` is end-to-end encrypted, which needs the local-first client.`;
  switch (reason) {
    case "no-loader":
      return `${lead} Give Jazz a loader for it, \`client: () => import("jazz-tools/local-first")\`, where you create the client or provider, or call \`db.upgradeToLocalFirst(loader)\` first.`;
    case "direct-thin":
      return `${lead} This thin Db was opened directly: upgrade it with \`upgradeToLocalFirst(thin, open)\` from \`jazz-tools/thin\` and use the Db it returns.`;
    case "load-failed":
      return `${lead} Loading the local-first client failed${
        cause instanceof Error ? `: ${cause.message}` : ""
      }.`;
    case "timeout":
      return `${lead} The local-first client did not load within 30 seconds.`;
    case "not-loaded":
      return `${lead} This call needs the local-first client, now loading; call it again after \`await db.encryption.ready()\`.`;
    case "transaction":
      return `${lead} This transaction began on the thin client and can't commit: begin it again after \`await db.upgradeToLocalFirst(loader)\`.`;
  }
}
