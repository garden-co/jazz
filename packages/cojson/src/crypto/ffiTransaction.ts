import { NapiFfiTransaction } from "cojson-core-napi";
import type { Transaction } from "../coValueCore/verifiedState.js";

// We intentionally avoid importing `WasmFfiTransaction` as a typed symbol because
// the wasm-bindgen-generated TypeScript declarations are generated artifacts.
// At runtime, `cojson-core-wasm` will export the class once the WASM package is rebuilt.
import { WasmFfiTransaction } from "cojson-core-wasm";

/**
 * Common FFI object shape for NAPI + RN (camelCase, matching generated bindings).
 * Uses bigint for madeAt to support u64 in Rust bindings.
 */
export type FfiTransactionObject = {
  privacy: "private" | "trusting";
  encryptedChanges?: string;
  keyUsed?: string;
  changes?: string;
  madeAt: bigint;
  meta?: string;
};

export function toFfiTransactionObject(tx: Transaction): FfiTransactionObject {
  if (tx.privacy === "private") {
    return {
      privacy: "private",
      encryptedChanges: tx.encryptedChanges,
      keyUsed: tx.keyUsed,
      madeAt: BigInt(tx.madeAt),
      meta: tx.meta,
    };
  }

  return {
    privacy: "trusting",
    changes: tx.changes,
    madeAt: BigInt(tx.madeAt),
    meta: tx.meta,
  };
}

export function toNapiFfiTransaction(tx: Transaction): NapiFfiTransaction {
  // NAPI generated type matches the camelCase shape.
  return toFfiTransactionObject(tx) as unknown as NapiFfiTransaction;
}

export function toWasmFfiTransaction(tx: Transaction): WasmFfiTransaction {
  return new WasmFfiTransaction(
    tx.privacy,
    tx.privacy === "private" ? tx.encryptedChanges : undefined,
    tx.privacy === "private" ? tx.keyUsed : undefined,
    tx.privacy === "trusting" ? tx.changes : undefined,
    BigInt(tx.madeAt),
    tx.meta,
  );
}
