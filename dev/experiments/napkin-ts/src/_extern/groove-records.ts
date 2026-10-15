import { extern } from "../support/runtime.js";

// groove storage (groove-db-storage): metadata column family and ordered layout store.
export const LARGE_VALUE_METADATA_CF = extern<string>();
export interface LayoutStorage {
  get(cf: string, key: Uint8Array): Promise<Uint8Array | null>;
  putIfAbsent(cf: string, key: Uint8Array, value: Uint8Array): Promise<Uint8Array | null>;
  compareAndDelete(cf: string, key: Uint8Array, expected: Uint8Array): Promise<boolean>;
}

// ivm runtime (groove-ivm-*): evaluation-session chunk inputs and the blocked outcome.
export interface LoadedChunk {
  verifiesObjectHash(hash: Uint8Array): boolean;
  bytes(): Uint8Array;
}
export interface EvaluationInputs {
  /** Throws the evaluator's `EvaluationBlocked` error when the chunk is not yet loaded. */
  loadedChunk(request: any): LoadedChunk;
  installChunk(request: any, bytes: Uint8Array): void;
}
export const isEvaluationBlocked = extern<(e: unknown) => boolean>();
export const evaluationBlocked = extern<() => Error>();
