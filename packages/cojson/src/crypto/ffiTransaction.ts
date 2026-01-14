import {
  NapiFfiTransaction,
  createTransactionFfi as createTransactionFfiNapi,
} from "cojson-core-napi";
import {
  UniffiFfiTransaction,
  createTransactionFfi as createTransactionFfiRn,
} from "cojson-core-rn";
import type { Transaction } from "../coValueCore/verifiedState.js";

// We intentionally avoid importing `WasmFfiTransaction` as a typed symbol because
// the wasm-bindgen-generated TypeScript declarations are generated artifacts.
// At runtime, `cojson-core-wasm` will export the class once the WASM package is rebuilt.
import { WasmFfiTransaction } from "cojson-core-wasm";

export function toUniffiFfiTransaction(tx: Transaction): UniffiFfiTransaction {
  if (tx.privacy === "private") {
    return createTransactionFfiRn(
      tx.privacy,
      tx.encryptedChanges,
      tx.keyUsed,
      BigInt(tx.madeAt),
      tx.meta,
    );
  }

  return createTransactionFfiRn(
    tx.privacy,
    tx.changes,
    undefined,
    BigInt(tx.madeAt),
    tx.meta,
  );
}

export function toNapiFfiTransaction(tx: Transaction): NapiFfiTransaction {
  if (tx.privacy === "private") {
    return createTransactionFfiNapi(
      tx.privacy,
      tx.encryptedChanges,
      tx.keyUsed,
      BigInt(tx.madeAt),
      tx.meta,
    );
  }

  return createTransactionFfiNapi(
    tx.privacy,
    tx.changes,
    undefined,
    BigInt(tx.madeAt),
    tx.meta,
  );
}

export function toWasmFfiTransaction(tx: Transaction): WasmFfiTransaction {
  if (tx.privacy === "private") {
    return new WasmFfiTransaction(
      tx.privacy,
      tx.keyUsed,
      tx.encryptedChanges,
      BigInt(tx.madeAt),
      tx.meta,
    );
  }

  return new WasmFfiTransaction(
    tx.privacy,
    undefined,
    tx.changes,
    BigInt(tx.madeAt),
    tx.meta,
  );
}
