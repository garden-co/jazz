/* tslint:disable */
/* eslint-disable */
export class SessionLog {
  free(): void;
  constructor(co_id: string, session_id: string, signer_id: string);
  clone(): SessionLog;
  tryAdd(transactions_json: string[], new_signature_str: string, skip_verify: boolean): string;
  addNewPrivateTransaction(changes_json: string, signer_secret: string, encryption_key: string, key_id: string, made_at: number): string;
  addNewTrustingTransaction(changes_json: string, signer_secret: string, made_at: number): string;
  testExpectedHashAfter(transactions_js: any): string;
  decryptNextTransactionChangesJson(tx_index: number, key_secret: Uint8Array): string;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly memory: WebAssembly.Memory;
  readonly __wbg_sessionlog_free: (a: number, b: number) => void;
  readonly sessionlog_new: (a: number, b: number, c: number, d: number, e: number, f: number) => number;
  readonly sessionlog_clone: (a: number) => number;
  readonly sessionlog_tryAdd: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
  readonly sessionlog_addNewPrivateTransaction: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number, j: number) => [number, number, number, number];
  readonly sessionlog_addNewTrustingTransaction: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
  readonly sessionlog_testExpectedHashAfter: (a: number, b: any) => [number, number, number, number];
  readonly sessionlog_decryptNextTransactionChangesJson: (a: number, b: number, c: number, d: number) => [number, number, number, number];
  readonly __wbindgen_malloc: (a: number, b: number) => number;
  readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
  readonly __wbindgen_exn_store: (a: number) => void;
  readonly __externref_table_alloc: () => number;
  readonly __wbindgen_export_4: WebAssembly.Table;
  readonly __externref_table_dealloc: (a: number) => void;
  readonly __wbindgen_free: (a: number, b: number, c: number) => void;
  readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;
/**
* Instantiates the given `module`, which can either be bytes or
* a precompiled `WebAssembly.Module`.
*
* @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
*
* @returns {InitOutput}
*/
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
* If `module_or_path` is {RequestInfo} or {URL}, makes a request and
* for everything else, calls `WebAssembly.instantiate` directly.
*
* @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
*
* @returns {Promise<InitOutput>}
*/
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
