/* tslint:disable */
/* eslint-disable */
export const memory: WebAssembly.Memory;
export const __wbg_wasmdatabase_free: (a: number, b: number) => void;
export const wasmdatabase_new: () => number;
export const wasmdatabase_execute: (a: number, b: number, c: number) => [number, number, number];
export const wasmdatabase_select_binary: (a: number, b: number, c: number) => [number, number, number];
export const wasmdatabase_update_row: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number) => [number, number, number];
export const wasmdatabase_update_row_i64: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: bigint) => [number, number, number];
export const wasmdatabase_init_schema: (a: number, b: number, c: number) => [number, number];
export const wasmdatabase_subscribe: (a: number, b: number, c: number, d: any) => [number, number, number];
export const wasmdatabase_subscribe_binary: (a: number, b: number, c: number, d: any) => [number, number, number];
export const wasmdatabase_subscribe_delta: (a: number, b: number, c: number, d: any) => [number, number, number];
export const __wbg_wasmqueryhandle_free: (a: number, b: number) => void;
export const wasmqueryhandle_unsubscribe: (a: number) => void;
export const __wbg_wasmqueryhandlebinary_free: (a: number, b: number) => void;
export const wasmqueryhandlebinary_unsubscribe: (a: number) => void;
export const __wbg_wasmqueryhandledelta_free: (a: number, b: number) => void;
export const wasmqueryhandledelta_unsubscribe: (a: number) => void;
export const wasmqueryhandledelta_diagram: (a: number) => [number, number];
export const init: () => void;
export const __wbindgen_malloc: (a: number, b: number) => number;
export const __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
export const __wbindgen_exn_store: (a: number) => void;
export const __externref_table_alloc: () => number;
export const __wbindgen_externrefs: WebAssembly.Table;
export const __wbindgen_free: (a: number, b: number, c: number) => void;
export const __externref_table_dealloc: (a: number) => void;
export const __wbindgen_start: () => void;
