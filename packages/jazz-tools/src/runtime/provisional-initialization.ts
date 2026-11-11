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
        | {
            readonly kind: "rejected";
            readonly code: string;
            readonly reason: string | null;
            readonly message: string;
          };
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
  const envelope: unknown = JSON.parse(encoded);
  if (
    !isRecord(envelope) ||
    envelope.format !== "jazz.sqlite.initialization-status.v1" ||
    !Array.isArray(envelope.states) ||
    envelope.states.length !== ids.length
  )
    throw new Error("Invalid initialization status response");
  return envelope.states.map((status: unknown, index: number): InitializationTransactionStatus => {
    const reservedTxId = ids[index]!;
    if (!isRecord(status) || status.transactionId !== reservedTxId)
      throw new Error("Initialization status identity mismatch");
    if (status.status === "not-observed" || status.status === "incomplete")
      return { kind: status.status, reservedTxId };
    if (
      status.status !== "complete" ||
      !isPositivePosition(status.publicationPosition) ||
      (status.durability !== "Local" && status.durability !== "Global") ||
      (status.globalPosition !== null && !isPositivePosition(status.globalPosition)) ||
      (status.durability === "Global" && status.globalPosition === null)
    )
      throw new Error("Invalid initialization transaction status");
    const durability = status.durability === "Global" ? "global" : "local";
    if (status.fate === "pending" || status.fate === "accepted") {
      if (status.error !== null) throw new Error("Invalid initialization transaction fate");
      return { kind: "complete", reservedTxId, fate: { kind: status.fate }, durability };
    }
    if (
      status.fate !== "rejected" ||
      !isRecord(status.error) ||
      typeof status.error.code !== "string" ||
      (status.error.reason !== null && typeof status.error.reason !== "string") ||
      typeof status.error.message !== "string"
    )
      throw new Error("Invalid initialization transaction fate");
    return {
      kind: "complete",
      reservedTxId,
      durability,
      fate: {
        kind: "rejected",
        code: status.error.code,
        reason: status.error.reason,
        message: status.error.message,
      },
    };
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isPositivePosition(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^[1-9][0-9]*$/.test(value) &&
    value.length <= 19 &&
    BigInt(value) <= 9223372036854775807n
  );
}
