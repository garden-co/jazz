import type { DurabilityTier, OpenTransactionId, TxId } from "./client.js";

declare const reservedTxIdBrand: unique symbol;
declare const initializationSealBrand: unique symbol;

/** Journal linkage only. An unpublished reservation is not a public transaction identity. */
export type ReservedTxId = string & { readonly [reservedTxIdBrand]: true };
export interface InitializationSeal {
  readonly reservedTxId: ReservedTxId;
  readonly [initializationSealBrand]: true;
}

export type InitializationTransactionStatus =
  | { readonly kind: "not-observed" | "incomplete"; readonly reservedTxId: ReservedTxId }
  | {
      readonly kind: "complete";
      readonly reservedTxId: ReservedTxId;
      readonly fate:
        | { readonly kind: "pending" | "accepted" }
        | { readonly kind: "rejected"; readonly code: string; readonly reason: string };
      readonly durability: DurabilityTier | "none";
    };

/** Private runtime seam; absence of any capability must fail closed. */
export interface InitializationRuntime {
  sealInitializationTransaction(id: OpenTransactionId): Promise<InitializationSeal>;
  publishInitializationTransaction(seal: InitializationSeal): Promise<TxId>;
  cancelInitializationTransaction(seal: InitializationSeal): Promise<void>;
  initializationTransactionStatus(
    ids: readonly ReservedTxId[],
  ): Promise<readonly InitializationTransactionStatus[]>;
  recordInitializationInsertAbsence(
    id: OpenTransactionId,
    table: string,
    rowId: string,
  ): Promise<void>;
}

export class InitializationCapabilityError extends Error {
  override readonly name = "InitializationCapabilityError";
  constructor(capability: string) {
    super(`This Jazz runtime does not support automatic offline initialization: ${capability}`);
  }
}

/** Native adapters retain token ownership separately; callers can only journal the reservation. */
export function createInitializationSeal(reservedTxId: string): InitializationSeal {
  if (!reservedTxId) throw new Error("Invalid initialization reservation");
  return Object.freeze({ reservedTxId: reservedTxId as ReservedTxId }) as InitializationSeal;
}

export function assertInitializationStatusBound(ids: readonly ReservedTxId[]): void {
  if (ids.length > 64)
    throw new RangeError("Initialization status accepts at most 64 transaction identities");
}

/** Validate the versioned local bridge before any recovery decision can use it. */
export function decodeInitializationStatuses(
  encoded: string,
  ids: readonly ReservedTxId[],
): readonly InitializationTransactionStatus[] {
  assertInitializationStatusBound(ids);
  const envelope = JSON.parse(encoded);
  if (
    envelope?.version !== 1 ||
    !Array.isArray(envelope.statuses) ||
    envelope.statuses.length !== ids.length
  )
    throw new Error("Invalid initialization status response");
  for (const [index, status] of envelope.statuses.entries()) {
    if (!status || status.reservedTxId !== ids[index])
      throw new Error("Initialization status identity mismatch");
    if (status.kind === "not-observed" || status.kind === "incomplete") continue;
    if (status.kind !== "complete" || !["none", "local", "global"].includes(status.durability))
      throw new Error("Invalid initialization transaction status");
    if (status.fate?.kind === "pending" || status.fate?.kind === "accepted") continue;
    if (
      status.fate?.kind !== "rejected" ||
      typeof status.fate.code !== "string" ||
      typeof status.fate.reason !== "string"
    )
      throw new Error("Invalid initialization transaction fate");
  }
  return envelope.statuses;
}
