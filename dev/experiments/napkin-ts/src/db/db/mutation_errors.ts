import * as X from "../../_extern/db-core.js";
import type {
  DbError,
  MutationErrorCallback,
  MutationErrorEvent,
  MutationErrorState,
  TickScheduler,
} from "../db.js";
import { transactionKindFrom } from "../db.js";
import { SortedMap } from "../../support/collections.js";

export function mutationErrorEvent(rejected: X.RejectedTransaction): MutationErrorEvent {
  return mutationErrorEventFor(rejected.txId(), rejected.kind(), rejected.reason());
}

export function mutationErrorEventFor(
  txId: X.TxId,
  kind: X.TxKind,
  rejection: X.RejectionReason,
): MutationErrorEvent {
  const [code, reason] = mutationErrorDetails(rejection);
  return withDetails(X.TransactionId.fromCommittedTx(txId), kind, code, reason);
}

export function queuedMutationErrorEvent(
  txId: X.TxId,
  kind: X.TxKind,
  error: DbError,
): MutationErrorEvent {
  return withDetails(
    X.TransactionId.fromCommittedTx(txId),
    kind,
    X.errorCodeStr(error.code),
    error.message,
  );
}

function withDetails(
  transactionId: X.TransactionId,
  kind: X.TxKind,
  code: string,
  reason: string,
): MutationErrorEvent {
  return {
    code,
    reason,
    transaction: {
      transactionId,
      kind: transactionKindFrom(kind),
      sealed: true,
      latestSettlement: { kind: "rejected", transactionId, code, reason },
    },
  };
}

export function mutationErrorDetails(reason: X.RejectionReason): [string, string] {
  switch (reason.kind) {
    case "ClientClockTooFarAhead":
      return ["client_clock_too_far_ahead", "Client clock is too far ahead"];
    case "AuthorizationDenied":
      return ["permission_denied", "Write rejected by server authorization"];
    case "ExclusiveConflict":
      return ["exclusive_conflict", "Exclusive transaction conflicted with another write"];
    case "CausalityViolation":
      return ["causality_violation", "Transaction violated causal ordering"];
    case "Cascade":
      return [
        "cascade_rejected",
        `Transaction was rejected because ancestor transaction ${X.TransactionId.fromCommittedTx(reason.root)} was rejected`,
      ];
    case "MalformedCommit":
      return reason.reason.endsWith("is not supported yet")
        ? ["write_rejected", reason.reason]
        : ["write_rejected", `Malformed transaction: ${reason.reason}`];
  }
  throw new Error("unreachable");
}

type SharedScheduler = { v: TickScheduler | undefined };

export function queueMutationError(
  errors: MutationErrorState,
  scheduler: SharedScheduler,
  txId: X.TxId,
  event: MutationErrorEvent,
): void {
  errors.pending.getOrInsert(txId, () => event);
  if (errors.callback) X.scheduleTickIn(scheduler, "Immediate");
}

export class MutationErrorWait {
  constructor(
    readonly errors: MutationErrorState,
    readonly scheduler: SharedScheduler,
    readonly txId: X.TxId,
  ) {
    errors.applicationWaiters.set(txId, (errors.applicationWaiters.get(txId) ?? 0) + 1);
  }

  drop(): void {
    const s = this.errors;
    const count = s.applicationWaiters.get(this.txId);
    if (count === undefined) throw new Error("registered application waiter");
    const released = count - 1 === 0;
    if (released) s.applicationWaiters.delete(this.txId);
    else s.applicationWaiters.set(this.txId, count - 1);
    if (released && s.callback && s.pending.has(this.txId))
      X.scheduleTickIn(this.scheduler, "Immediate");
  }
}

export function takePendingMutationErrorDelivery(
  errors: MutationErrorState,
): [MutationErrorCallback, SortedMap<X.TxId, MutationErrorEvent>] | undefined {
  const callback = errors.callback;
  if (!callback) return undefined;
  const claimed = new SortedMap<X.TxId, MutationErrorEvent>(X.cmpTxId);
  const unclaimed = new SortedMap<X.TxId, MutationErrorEvent>(X.cmpTxId);
  for (const [txId, e] of errors.pending)
    (errors.applicationWaiters.has(txId) ? claimed : unclaimed).set(txId, e);
  errors.pending = claimed;
  return [callback, unclaimed];
}
