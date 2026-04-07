var __defProp = Object.defineProperty;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });
var __esm = (fn, res) =>
  function __init() {
    return (fn && (res = (0, fn[__getOwnPropNames(fn)[0]])((fn = 0))), res);
  };
var __export = (target, all) => {
  for (var name in all) __defProp(target, name, { get: all[name], enumerable: true });
};

// ../../crates/jazz-wasm/pkg/jazz_wasm.js
var jazz_wasm_exports = {};
__export(jazz_wasm_exports, {
  WasmQueryBuilder: () => WasmQueryBuilder,
  WasmRuntime: () => WasmRuntime,
  bench_get_cache_bytes: () => bench_get_cache_bytes,
  bench_get_overflow_threshold_bytes: () => bench_get_overflow_threshold_bytes,
  bench_get_pin_internal_pages: () => bench_get_pin_internal_pages,
  bench_get_read_coalesce_pages: () => bench_get_read_coalesce_pages,
  bench_opfs_cold_random_read: () => bench_opfs_cold_random_read,
  bench_opfs_cold_sequential_read: () => bench_opfs_cold_sequential_read,
  bench_opfs_matrix: () => bench_opfs_matrix,
  bench_opfs_mixed_matrix: () => bench_opfs_mixed_matrix,
  bench_opfs_mixed_scenario: () => bench_opfs_mixed_scenario,
  bench_opfs_random_read: () => bench_opfs_random_read,
  bench_opfs_random_write: () => bench_opfs_random_write,
  bench_opfs_range_random_window: () => bench_opfs_range_random_window,
  bench_opfs_range_seq_window: () => bench_opfs_range_seq_window,
  bench_opfs_sequential_read: () => bench_opfs_sequential_read,
  bench_opfs_sequential_write: () => bench_opfs_sequential_write,
  bench_reset_cache_bytes: () => bench_reset_cache_bytes,
  bench_reset_overflow_threshold_bytes: () => bench_reset_overflow_threshold_bytes,
  bench_reset_pin_internal_pages: () => bench_reset_pin_internal_pages,
  bench_reset_read_coalesce_pages: () => bench_reset_read_coalesce_pages,
  bench_set_cache_bytes: () => bench_set_cache_bytes,
  bench_set_overflow_threshold_bytes: () => bench_set_overflow_threshold_bytes,
  bench_set_pin_internal_pages: () => bench_set_pin_internal_pages,
  bench_set_read_coalesce_pages: () => bench_set_read_coalesce_pages,
  currentTimestamp: () => currentTimestamp,
  default: () => __wbg_init,
  generateId: () => generateId,
  init: () => init,
  initSync: () => initSync,
  parseSchema: () => parseSchema,
});
function bench_get_cache_bytes() {
  const ret = wasm.bench_get_cache_bytes();
  return ret >>> 0;
}
function bench_get_overflow_threshold_bytes() {
  const ret = wasm.bench_get_overflow_threshold_bytes();
  return ret >>> 0;
}
function bench_get_pin_internal_pages() {
  const ret = wasm.bench_get_pin_internal_pages();
  return ret !== 0;
}
function bench_get_read_coalesce_pages() {
  const ret = wasm.bench_get_read_coalesce_pages();
  return ret >>> 0;
}
function bench_opfs_cold_random_read(count, value_size) {
  const ret = wasm.bench_opfs_cold_random_read(count, value_size);
  return ret;
}
function bench_opfs_cold_sequential_read(count, value_size) {
  const ret = wasm.bench_opfs_cold_sequential_read(count, value_size);
  return ret;
}
function bench_opfs_matrix(count) {
  const ret = wasm.bench_opfs_matrix(count);
  return ret;
}
function bench_opfs_mixed_matrix(count) {
  const ret = wasm.bench_opfs_mixed_matrix(count);
  return ret;
}
function bench_opfs_mixed_scenario(scenario_name, count, value_size, base_seed) {
  const ptr0 = passStringToWasm0(scenario_name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
  const len0 = WASM_VECTOR_LEN;
  const ret = wasm.bench_opfs_mixed_scenario(
    ptr0,
    len0,
    count,
    value_size,
    !isLikeNone(base_seed),
    isLikeNone(base_seed) ? BigInt(0) : base_seed,
  );
  return ret;
}
function bench_opfs_random_read(count, value_size) {
  const ret = wasm.bench_opfs_random_read(count, value_size);
  return ret;
}
function bench_opfs_random_write(count, value_size) {
  const ret = wasm.bench_opfs_random_write(count, value_size);
  return ret;
}
function bench_opfs_range_random_window(count, value_size) {
  const ret = wasm.bench_opfs_range_random_window(count, value_size);
  return ret;
}
function bench_opfs_range_seq_window(count, value_size) {
  const ret = wasm.bench_opfs_range_seq_window(count, value_size);
  return ret;
}
function bench_opfs_sequential_read(count, value_size) {
  const ret = wasm.bench_opfs_sequential_read(count, value_size);
  return ret;
}
function bench_opfs_sequential_write(count, value_size) {
  const ret = wasm.bench_opfs_sequential_write(count, value_size);
  return ret;
}
function bench_reset_cache_bytes() {
  wasm.bench_reset_cache_bytes();
}
function bench_reset_overflow_threshold_bytes() {
  wasm.bench_reset_overflow_threshold_bytes();
}
function bench_reset_pin_internal_pages() {
  wasm.bench_reset_pin_internal_pages();
}
function bench_reset_read_coalesce_pages() {
  wasm.bench_reset_read_coalesce_pages();
}
function bench_set_cache_bytes(cache_bytes) {
  const ret = wasm.bench_set_cache_bytes(cache_bytes);
  if (ret[1]) {
    throw takeFromExternrefTable0(ret[0]);
  }
}
function bench_set_overflow_threshold_bytes(overflow_threshold_bytes) {
  const ret = wasm.bench_set_overflow_threshold_bytes(overflow_threshold_bytes);
  if (ret[1]) {
    throw takeFromExternrefTable0(ret[0]);
  }
}
function bench_set_pin_internal_pages(pin_internal_pages) {
  wasm.bench_set_pin_internal_pages(pin_internal_pages);
}
function bench_set_read_coalesce_pages(read_coalesce_pages) {
  const ret = wasm.bench_set_read_coalesce_pages(read_coalesce_pages);
  if (ret[1]) {
    throw takeFromExternrefTable0(ret[0]);
  }
}
function currentTimestamp() {
  const ret = wasm.currentTimestamp();
  return BigInt.asUintN(64, ret);
}
function generateId() {
  let deferred1_0;
  let deferred1_1;
  try {
    const ret = wasm.generateId();
    deferred1_0 = ret[0];
    deferred1_1 = ret[1];
    return getStringFromWasm0(ret[0], ret[1]);
  } finally {
    wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
  }
}
function init() {
  wasm.init();
}
function parseSchema(json2) {
  const ptr0 = passStringToWasm0(json2, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
  const len0 = WASM_VECTOR_LEN;
  const ret = wasm.parseSchema(ptr0, len0);
  if (ret[2]) {
    throw takeFromExternrefTable0(ret[1]);
  }
  return takeFromExternrefTable0(ret[0]);
}
function __wbg_get_imports() {
  const import0 = {
    __proto__: null,
    __wbg_Error_83742b46f01ce22d: /* @__PURE__ */ __name(function (arg0, arg1) {
      const ret = Error(getStringFromWasm0(arg0, arg1));
      return ret;
    }, "__wbg_Error_83742b46f01ce22d"),
    __wbg_Number_a5a435bd7bbec835: /* @__PURE__ */ __name(function (arg0) {
      const ret = Number(arg0);
      return ret;
    }, "__wbg_Number_a5a435bd7bbec835"),
    __wbg_String_8564e559799eccda: /* @__PURE__ */ __name(function (arg0, arg1) {
      const ret = String(arg1);
      const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
      const len1 = WASM_VECTOR_LEN;
      getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
      getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
    }, "__wbg_String_8564e559799eccda"),
    __wbg___wbindgen_bigint_get_as_i64_447a76b5c6ef7bda: /* @__PURE__ */ __name(function (
      arg0,
      arg1,
    ) {
      const v = arg1;
      const ret = typeof v === "bigint" ? v : void 0;
      getDataViewMemory0().setBigInt64(arg0 + 8 * 1, isLikeNone(ret) ? BigInt(0) : ret, true);
      getDataViewMemory0().setInt32(arg0 + 4 * 0, !isLikeNone(ret), true);
    }, "__wbg___wbindgen_bigint_get_as_i64_447a76b5c6ef7bda"),
    __wbg___wbindgen_boolean_get_c0f3f60bac5a78d1: /* @__PURE__ */ __name(function (arg0) {
      const v = arg0;
      const ret = typeof v === "boolean" ? v : void 0;
      return isLikeNone(ret) ? 16777215 : ret ? 1 : 0;
    }, "__wbg___wbindgen_boolean_get_c0f3f60bac5a78d1"),
    __wbg___wbindgen_debug_string_5398f5bb970e0daa: /* @__PURE__ */ __name(function (arg0, arg1) {
      const ret = debugString(arg1);
      const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
      const len1 = WASM_VECTOR_LEN;
      getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
      getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
    }, "__wbg___wbindgen_debug_string_5398f5bb970e0daa"),
    __wbg___wbindgen_in_41dbb8413020e076: /* @__PURE__ */ __name(function (arg0, arg1) {
      const ret = arg0 in arg1;
      return ret;
    }, "__wbg___wbindgen_in_41dbb8413020e076"),
    __wbg___wbindgen_is_bigint_e2141d4f045b7eda: /* @__PURE__ */ __name(function (arg0) {
      const ret = typeof arg0 === "bigint";
      return ret;
    }, "__wbg___wbindgen_is_bigint_e2141d4f045b7eda"),
    __wbg___wbindgen_is_function_3c846841762788c1: /* @__PURE__ */ __name(function (arg0) {
      const ret = typeof arg0 === "function";
      return ret;
    }, "__wbg___wbindgen_is_function_3c846841762788c1"),
    __wbg___wbindgen_is_null_0b605fc6b167c56f: /* @__PURE__ */ __name(function (arg0) {
      const ret = arg0 === null;
      return ret;
    }, "__wbg___wbindgen_is_null_0b605fc6b167c56f"),
    __wbg___wbindgen_is_object_781bc9f159099513: /* @__PURE__ */ __name(function (arg0) {
      const val = arg0;
      const ret = typeof val === "object" && val !== null;
      return ret;
    }, "__wbg___wbindgen_is_object_781bc9f159099513"),
    __wbg___wbindgen_is_string_7ef6b97b02428fae: /* @__PURE__ */ __name(function (arg0) {
      const ret = typeof arg0 === "string";
      return ret;
    }, "__wbg___wbindgen_is_string_7ef6b97b02428fae"),
    __wbg___wbindgen_is_undefined_52709e72fb9f179c: /* @__PURE__ */ __name(function (arg0) {
      const ret = arg0 === void 0;
      return ret;
    }, "__wbg___wbindgen_is_undefined_52709e72fb9f179c"),
    __wbg___wbindgen_jsval_eq_ee31bfad3e536463: /* @__PURE__ */ __name(function (arg0, arg1) {
      const ret = arg0 === arg1;
      return ret;
    }, "__wbg___wbindgen_jsval_eq_ee31bfad3e536463"),
    __wbg___wbindgen_jsval_loose_eq_5bcc3bed3c69e72b: /* @__PURE__ */ __name(function (arg0, arg1) {
      const ret = arg0 == arg1;
      return ret;
    }, "__wbg___wbindgen_jsval_loose_eq_5bcc3bed3c69e72b"),
    __wbg___wbindgen_number_get_34bb9d9dcfa21373: /* @__PURE__ */ __name(function (arg0, arg1) {
      const obj = arg1;
      const ret = typeof obj === "number" ? obj : void 0;
      getDataViewMemory0().setFloat64(arg0 + 8 * 1, isLikeNone(ret) ? 0 : ret, true);
      getDataViewMemory0().setInt32(arg0 + 4 * 0, !isLikeNone(ret), true);
    }, "__wbg___wbindgen_number_get_34bb9d9dcfa21373"),
    __wbg___wbindgen_string_get_395e606bd0ee4427: /* @__PURE__ */ __name(function (arg0, arg1) {
      const obj = arg1;
      const ret = typeof obj === "string" ? obj : void 0;
      var ptr1 = isLikeNone(ret)
        ? 0
        : passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
      var len1 = WASM_VECTOR_LEN;
      getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
      getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
    }, "__wbg___wbindgen_string_get_395e606bd0ee4427"),
    __wbg___wbindgen_throw_6ddd609b62940d55: /* @__PURE__ */ __name(function (arg0, arg1) {
      throw new Error(getStringFromWasm0(arg0, arg1));
    }, "__wbg___wbindgen_throw_6ddd609b62940d55"),
    __wbg__wbg_cb_unref_6b5b6b8576d35cb1: /* @__PURE__ */ __name(function (arg0) {
      arg0._wbg_cb_unref();
    }, "__wbg__wbg_cb_unref_6b5b6b8576d35cb1"),
    __wbg_call_2d781c1f4d5c0ef8: /* @__PURE__ */ __name(function () {
      return handleError(function (arg0, arg1, arg2) {
        const ret = arg0.call(arg1, arg2);
        return ret;
      }, arguments);
    }, "__wbg_call_2d781c1f4d5c0ef8"),
    __wbg_call_89797ac1adb21543: /* @__PURE__ */ __name(function () {
      return handleError(function (arg0, arg1, arg2, arg3, arg4, arg5) {
        const ret = arg0.call(arg1, arg2, arg3, arg4, arg5);
        return ret;
      }, arguments);
    }, "__wbg_call_89797ac1adb21543"),
    __wbg_call_dcc2662fa17a72cf: /* @__PURE__ */ __name(function () {
      return handleError(function (arg0, arg1, arg2, arg3) {
        const ret = arg0.call(arg1, arg2, arg3);
        return ret;
      }, arguments);
    }, "__wbg_call_dcc2662fa17a72cf"),
    __wbg_call_e133b57c9155d22c: /* @__PURE__ */ __name(function () {
      return handleError(function (arg0, arg1) {
        const ret = arg0.call(arg1);
        return ret;
      }, arguments);
    }, "__wbg_call_e133b57c9155d22c"),
    __wbg_close_bea86eef0f71dd9b: /* @__PURE__ */ __name(function (arg0) {
      arg0.close();
    }, "__wbg_close_bea86eef0f71dd9b"),
    __wbg_createSyncAccessHandle_b7143219a305a2ce: /* @__PURE__ */ __name(function (arg0) {
      const ret = arg0.createSyncAccessHandle();
      return ret;
    }, "__wbg_createSyncAccessHandle_b7143219a305a2ce"),
    __wbg_debug_32973ac940f2ca14: /* @__PURE__ */ __name(function (arg0, arg1) {
      let deferred0_0;
      let deferred0_1;
      try {
        deferred0_0 = arg0;
        deferred0_1 = arg1;
        console.debug(getStringFromWasm0(arg0, arg1));
      } finally {
        wasm.__wbindgen_free(deferred0_0, deferred0_1, 1);
      }
    }, "__wbg_debug_32973ac940f2ca14"),
    __wbg_debug_982454fce39f6582: /* @__PURE__ */ __name(function (
      arg0,
      arg1,
      arg2,
      arg3,
      arg4,
      arg5,
      arg6,
      arg7,
    ) {
      let deferred0_0;
      let deferred0_1;
      try {
        deferred0_0 = arg0;
        deferred0_1 = arg1;
        console.debug(
          getStringFromWasm0(arg0, arg1),
          getStringFromWasm0(arg2, arg3),
          getStringFromWasm0(arg4, arg5),
          getStringFromWasm0(arg6, arg7),
        );
      } finally {
        wasm.__wbindgen_free(deferred0_0, deferred0_1, 1);
      }
    }, "__wbg_debug_982454fce39f6582"),
    __wbg_done_08ce71ee07e3bd17: /* @__PURE__ */ __name(function (arg0) {
      const ret = arg0.done;
      return ret;
    }, "__wbg_done_08ce71ee07e3bd17"),
    __wbg_entries_e8a20ff8c9757101: /* @__PURE__ */ __name(function (arg0) {
      const ret = Object.entries(arg0);
      return ret;
    }, "__wbg_entries_e8a20ff8c9757101"),
    __wbg_error_1fd0a521bc586cb5: /* @__PURE__ */ __name(function (arg0, arg1) {
      let deferred0_0;
      let deferred0_1;
      try {
        deferred0_0 = arg0;
        deferred0_1 = arg1;
        console.error(getStringFromWasm0(arg0, arg1));
      } finally {
        wasm.__wbindgen_free(deferred0_0, deferred0_1, 1);
      }
    }, "__wbg_error_1fd0a521bc586cb5"),
    __wbg_error_87093280954deb60: /* @__PURE__ */ __name(function (
      arg0,
      arg1,
      arg2,
      arg3,
      arg4,
      arg5,
      arg6,
      arg7,
    ) {
      let deferred0_0;
      let deferred0_1;
      try {
        deferred0_0 = arg0;
        deferred0_1 = arg1;
        console.error(
          getStringFromWasm0(arg0, arg1),
          getStringFromWasm0(arg2, arg3),
          getStringFromWasm0(arg4, arg5),
          getStringFromWasm0(arg6, arg7),
        );
      } finally {
        wasm.__wbindgen_free(deferred0_0, deferred0_1, 1);
      }
    }, "__wbg_error_87093280954deb60"),
    __wbg_error_a6fa202b58aa1cd3: /* @__PURE__ */ __name(function (arg0, arg1) {
      let deferred0_0;
      let deferred0_1;
      try {
        deferred0_0 = arg0;
        deferred0_1 = arg1;
        console.error(getStringFromWasm0(arg0, arg1));
      } finally {
        wasm.__wbindgen_free(deferred0_0, deferred0_1, 1);
      }
    }, "__wbg_error_a6fa202b58aa1cd3"),
    __wbg_flush_1eca046e0ff7c399: /* @__PURE__ */ __name(function () {
      return handleError(function (arg0) {
        arg0.flush();
      }, arguments);
    }, "__wbg_flush_1eca046e0ff7c399"),
    __wbg_getDirectory_2406d369de179ff0: /* @__PURE__ */ __name(function (arg0) {
      const ret = arg0.getDirectory();
      return ret;
    }, "__wbg_getDirectory_2406d369de179ff0"),
    __wbg_getFileHandle_b85805519dc63efa: /* @__PURE__ */ __name(function (arg0, arg1, arg2, arg3) {
      const ret = arg0.getFileHandle(getStringFromWasm0(arg1, arg2), arg3);
      return ret;
    }, "__wbg_getFileHandle_b85805519dc63efa"),
    __wbg_getRandomValues_3f44b700395062e5: /* @__PURE__ */ __name(function () {
      return handleError(function (arg0, arg1) {
        globalThis.crypto.getRandomValues(getArrayU8FromWasm0(arg0, arg1));
      }, arguments);
    }, "__wbg_getRandomValues_3f44b700395062e5"),
    __wbg_getRandomValues_a1cf2e70b003a59d: /* @__PURE__ */ __name(function () {
      return handleError(function (arg0, arg1) {
        globalThis.crypto.getRandomValues(getArrayU8FromWasm0(arg0, arg1));
      }, arguments);
    }, "__wbg_getRandomValues_a1cf2e70b003a59d"),
    __wbg_getSize_0a16c5e2524d34aa: /* @__PURE__ */ __name(function () {
      return handleError(function (arg0) {
        const ret = arg0.getSize();
        return ret;
      }, arguments);
    }, "__wbg_getSize_0a16c5e2524d34aa"),
    __wbg_get_326e41e095fb2575: /* @__PURE__ */ __name(function () {
      return handleError(function (arg0, arg1) {
        const ret = Reflect.get(arg0, arg1);
        return ret;
      }, arguments);
    }, "__wbg_get_326e41e095fb2575"),
    __wbg_get_3ef1eba1850ade27: /* @__PURE__ */ __name(function () {
      return handleError(function (arg0, arg1) {
        const ret = Reflect.get(arg0, arg1);
        return ret;
      }, arguments);
    }, "__wbg_get_3ef1eba1850ade27"),
    __wbg_get_a8ee5c45dabc1b3b: /* @__PURE__ */ __name(function (arg0, arg1) {
      const ret = arg0[arg1 >>> 0];
      return ret;
    }, "__wbg_get_a8ee5c45dabc1b3b"),
    __wbg_get_unchecked_329cfe50afab7352: /* @__PURE__ */ __name(function (arg0, arg1) {
      const ret = arg0[arg1 >>> 0];
      return ret;
    }, "__wbg_get_unchecked_329cfe50afab7352"),
    __wbg_get_with_ref_key_6412cf3094599694: /* @__PURE__ */ __name(function (arg0, arg1) {
      const ret = arg0[arg1];
      return ret;
    }, "__wbg_get_with_ref_key_6412cf3094599694"),
    __wbg_instanceof_ArrayBuffer_101e2bf31071a9f6: /* @__PURE__ */ __name(function (arg0) {
      let result;
      try {
        result = arg0 instanceof ArrayBuffer;
      } catch (_) {
        result = false;
      }
      const ret = result;
      return ret;
    }, "__wbg_instanceof_ArrayBuffer_101e2bf31071a9f6"),
    __wbg_instanceof_DomException_2bdcf7791a2d7d09: /* @__PURE__ */ __name(function (arg0) {
      let result;
      try {
        result = arg0 instanceof DOMException;
      } catch (_) {
        result = false;
      }
      const ret = result;
      return ret;
    }, "__wbg_instanceof_DomException_2bdcf7791a2d7d09"),
    __wbg_instanceof_FileSystemDirectoryHandle_2944d0641b4ea10c: /* @__PURE__ */ __name(function (
      arg0,
    ) {
      let result;
      try {
        result = arg0 instanceof FileSystemDirectoryHandle;
      } catch (_) {
        result = false;
      }
      const ret = result;
      return ret;
    }, "__wbg_instanceof_FileSystemDirectoryHandle_2944d0641b4ea10c"),
    __wbg_instanceof_FileSystemFileHandle_37ac45c6adcff28f: /* @__PURE__ */ __name(function (arg0) {
      let result;
      try {
        result = arg0 instanceof FileSystemFileHandle;
      } catch (_) {
        result = false;
      }
      const ret = result;
      return ret;
    }, "__wbg_instanceof_FileSystemFileHandle_37ac45c6adcff28f"),
    __wbg_instanceof_FileSystemSyncAccessHandle_dc45d7dabb2f5ad9: /* @__PURE__ */ __name(function (
      arg0,
    ) {
      let result;
      try {
        result = arg0 instanceof FileSystemSyncAccessHandle;
      } catch (_) {
        result = false;
      }
      const ret = result;
      return ret;
    }, "__wbg_instanceof_FileSystemSyncAccessHandle_dc45d7dabb2f5ad9"),
    __wbg_instanceof_Map_f194b366846aca0c: /* @__PURE__ */ __name(function (arg0) {
      let result;
      try {
        result = arg0 instanceof Map;
      } catch (_) {
        result = false;
      }
      const ret = result;
      return ret;
    }, "__wbg_instanceof_Map_f194b366846aca0c"),
    __wbg_instanceof_Promise_7c3bdd7805c2c6e6: /* @__PURE__ */ __name(function (arg0) {
      let result;
      try {
        result = arg0 instanceof Promise;
      } catch (_) {
        result = false;
      }
      const ret = result;
      return ret;
    }, "__wbg_instanceof_Promise_7c3bdd7805c2c6e6"),
    __wbg_instanceof_Uint8Array_740438561a5b956d: /* @__PURE__ */ __name(function (arg0) {
      let result;
      try {
        result = arg0 instanceof Uint8Array;
      } catch (_) {
        result = false;
      }
      const ret = result;
      return ret;
    }, "__wbg_instanceof_Uint8Array_740438561a5b956d"),
    __wbg_instanceof_WorkerGlobalScope_de6976d00cb213c6: /* @__PURE__ */ __name(function (arg0) {
      let result;
      try {
        result = arg0 instanceof WorkerGlobalScope;
      } catch (_) {
        result = false;
      }
      const ret = result;
      return ret;
    }, "__wbg_instanceof_WorkerGlobalScope_de6976d00cb213c6"),
    __wbg_isArray_33b91feb269ff46e: /* @__PURE__ */ __name(function (arg0) {
      const ret = Array.isArray(arg0);
      return ret;
    }, "__wbg_isArray_33b91feb269ff46e"),
    __wbg_isSafeInteger_ecd6a7f9c3e053cd: /* @__PURE__ */ __name(function (arg0) {
      const ret = Number.isSafeInteger(arg0);
      return ret;
    }, "__wbg_isSafeInteger_ecd6a7f9c3e053cd"),
    __wbg_iterator_d8f549ec8fb061b1: /* @__PURE__ */ __name(function () {
      const ret = Symbol.iterator;
      return ret;
    }, "__wbg_iterator_d8f549ec8fb061b1"),
    __wbg_length_b3416cf66a5452c8: /* @__PURE__ */ __name(function (arg0) {
      const ret = arg0.length;
      return ret;
    }, "__wbg_length_b3416cf66a5452c8"),
    __wbg_length_ea16607d7b61445b: /* @__PURE__ */ __name(function (arg0) {
      const ret = arg0.length;
      return ret;
    }, "__wbg_length_ea16607d7b61445b"),
    __wbg_log_6a8b55ee2e172f54: /* @__PURE__ */ __name(function (
      arg0,
      arg1,
      arg2,
      arg3,
      arg4,
      arg5,
      arg6,
      arg7,
    ) {
      let deferred0_0;
      let deferred0_1;
      try {
        deferred0_0 = arg0;
        deferred0_1 = arg1;
        console.log(
          getStringFromWasm0(arg0, arg1),
          getStringFromWasm0(arg2, arg3),
          getStringFromWasm0(arg4, arg5),
          getStringFromWasm0(arg6, arg7),
        );
      } finally {
        wasm.__wbindgen_free(deferred0_0, deferred0_1, 1);
      }
    }, "__wbg_log_6a8b55ee2e172f54"),
    __wbg_log_a25c2a4d205f1618: /* @__PURE__ */ __name(function (arg0, arg1) {
      let deferred0_0;
      let deferred0_1;
      try {
        deferred0_0 = arg0;
        deferred0_1 = arg1;
        console.log(getStringFromWasm0(arg0, arg1));
      } finally {
        wasm.__wbindgen_free(deferred0_0, deferred0_1, 1);
      }
    }, "__wbg_log_a25c2a4d205f1618"),
    __wbg_mark_e4b209bb53de57a7: /* @__PURE__ */ __name(function (arg0, arg1) {
      performance.mark(getStringFromWasm0(arg0, arg1));
    }, "__wbg_mark_e4b209bb53de57a7"),
    __wbg_measure_0cab89f3addcdc37: /* @__PURE__ */ __name(function () {
      return handleError(function (arg0, arg1, arg2, arg3) {
        let deferred0_0;
        let deferred0_1;
        let deferred1_0;
        let deferred1_1;
        try {
          deferred0_0 = arg0;
          deferred0_1 = arg1;
          deferred1_0 = arg2;
          deferred1_1 = arg3;
          performance.measure(getStringFromWasm0(arg0, arg1), getStringFromWasm0(arg2, arg3));
        } finally {
          wasm.__wbindgen_free(deferred0_0, deferred0_1, 1);
          wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
      }, arguments);
    }, "__wbg_measure_0cab89f3addcdc37"),
    __wbg_navigator_583ffd4fc14c0f7a: /* @__PURE__ */ __name(function (arg0) {
      const ret = arg0.navigator;
      return ret;
    }, "__wbg_navigator_583ffd4fc14c0f7a"),
    __wbg_new_227d7c05414eb861: /* @__PURE__ */ __name(function () {
      const ret = new Error();
      return ret;
    }, "__wbg_new_227d7c05414eb861"),
    __wbg_new_49d5571bd3f0c4d4: /* @__PURE__ */ __name(function () {
      const ret = /* @__PURE__ */ new Map();
      return ret;
    }, "__wbg_new_49d5571bd3f0c4d4"),
    __wbg_new_5f486cdf45a04d78: /* @__PURE__ */ __name(function (arg0) {
      const ret = new Uint8Array(arg0);
      return ret;
    }, "__wbg_new_5f486cdf45a04d78"),
    __wbg_new_a70fbab9066b301f: /* @__PURE__ */ __name(function () {
      const ret = new Array();
      return ret;
    }, "__wbg_new_a70fbab9066b301f"),
    __wbg_new_ab79df5bd7c26067: /* @__PURE__ */ __name(function () {
      const ret = new Object();
      return ret;
    }, "__wbg_new_ab79df5bd7c26067"),
    __wbg_new_d098e265629cd10f: /* @__PURE__ */ __name(function (arg0, arg1) {
      try {
        var state0 = { a: arg0, b: arg1 };
        var cb0 = /* @__PURE__ */ __name((arg02, arg12) => {
          const a = state0.a;
          state0.a = 0;
          try {
            return wasm_bindgen__convert__closures_____invoke__h5f63d9cdaddb36c3(
              a,
              state0.b,
              arg02,
              arg12,
            );
          } finally {
            state0.a = a;
          }
        }, "cb0");
        const ret = new Promise(cb0);
        return ret;
      } finally {
        state0.a = state0.b = 0;
      }
    }, "__wbg_new_d098e265629cd10f"),
    __wbg_new_from_slice_22da9388ac046e50: /* @__PURE__ */ __name(function (arg0, arg1) {
      const ret = new Uint8Array(getArrayU8FromWasm0(arg0, arg1));
      return ret;
    }, "__wbg_new_from_slice_22da9388ac046e50"),
    __wbg_new_typed_aaaeaf29cf802876: /* @__PURE__ */ __name(function (arg0, arg1) {
      try {
        var state0 = { a: arg0, b: arg1 };
        var cb0 = /* @__PURE__ */ __name((arg02, arg12) => {
          const a = state0.a;
          state0.a = 0;
          try {
            return wasm_bindgen__convert__closures_____invoke__h5f63d9cdaddb36c3(
              a,
              state0.b,
              arg02,
              arg12,
            );
          } finally {
            state0.a = a;
          }
        }, "cb0");
        const ret = new Promise(cb0);
        return ret;
      } finally {
        state0.a = state0.b = 0;
      }
    }, "__wbg_new_typed_aaaeaf29cf802876"),
    __wbg_next_11b99ee6237339e3: /* @__PURE__ */ __name(function () {
      return handleError(function (arg0) {
        const ret = arg0.next();
        return ret;
      }, arguments);
    }, "__wbg_next_11b99ee6237339e3"),
    __wbg_next_e01a967809d1aa68: /* @__PURE__ */ __name(function (arg0) {
      const ret = arg0.next;
      return ret;
    }, "__wbg_next_e01a967809d1aa68"),
    __wbg_now_16f0c993d5dd6c27: /* @__PURE__ */ __name(function () {
      const ret = Date.now();
      return ret;
    }, "__wbg_now_16f0c993d5dd6c27"),
    __wbg_now_ad1121946ba97ea0: /* @__PURE__ */ __name(function () {
      return handleError(function () {
        const ret = Date.now();
        return ret;
      }, arguments);
    }, "__wbg_now_ad1121946ba97ea0"),
    __wbg_prototypesetcall_d62e5099504357e6: /* @__PURE__ */ __name(function (arg0, arg1, arg2) {
      Uint8Array.prototype.set.call(getArrayU8FromWasm0(arg0, arg1), arg2);
    }, "__wbg_prototypesetcall_d62e5099504357e6"),
    __wbg_queueMicrotask_0c399741342fb10f: /* @__PURE__ */ __name(function (arg0) {
      const ret = arg0.queueMicrotask;
      return ret;
    }, "__wbg_queueMicrotask_0c399741342fb10f"),
    __wbg_queueMicrotask_a082d78ce798393e: /* @__PURE__ */ __name(function (arg0) {
      queueMicrotask(arg0);
    }, "__wbg_queueMicrotask_a082d78ce798393e"),
    __wbg_random_5bb86cae65a45bf6: /* @__PURE__ */ __name(function () {
      const ret = Math.random();
      return ret;
    }, "__wbg_random_5bb86cae65a45bf6"),
    __wbg_read_0285869b4fd131af: /* @__PURE__ */ __name(function () {
      return handleError(function (arg0, arg1, arg2, arg3) {
        const ret = arg0.read(getArrayU8FromWasm0(arg1, arg2), arg3);
        return ret;
      }, arguments);
    }, "__wbg_read_0285869b4fd131af"),
    __wbg_resolve_ae8d83246e5bcc12: /* @__PURE__ */ __name(function (arg0) {
      const ret = Promise.resolve(arg0);
      return ret;
    }, "__wbg_resolve_ae8d83246e5bcc12"),
    __wbg_setTimeout_90ea1b70d376baa9: /* @__PURE__ */ __name(function (arg0, arg1) {
      setTimeout(arg0, arg1);
    }, "__wbg_setTimeout_90ea1b70d376baa9"),
    __wbg_set_282384002438957f: /* @__PURE__ */ __name(function (arg0, arg1, arg2) {
      arg0[arg1 >>> 0] = arg2;
    }, "__wbg_set_282384002438957f"),
    __wbg_set_6be42768c690e380: /* @__PURE__ */ __name(function (arg0, arg1, arg2) {
      arg0[arg1] = arg2;
    }, "__wbg_set_6be42768c690e380"),
    __wbg_set_7eaa4f96924fd6b3: /* @__PURE__ */ __name(function () {
      return handleError(function (arg0, arg1, arg2) {
        const ret = Reflect.set(arg0, arg1, arg2);
        return ret;
      }, arguments);
    }, "__wbg_set_7eaa4f96924fd6b3"),
    __wbg_set_at_e227be75df7f9abf: /* @__PURE__ */ __name(function (arg0, arg1) {
      arg0.at = arg1;
    }, "__wbg_set_at_e227be75df7f9abf"),
    __wbg_set_bf7251625df30a02: /* @__PURE__ */ __name(function (arg0, arg1, arg2) {
      const ret = arg0.set(arg1, arg2);
      return ret;
    }, "__wbg_set_bf7251625df30a02"),
    __wbg_set_create_ef897736206a6f05: /* @__PURE__ */ __name(function (arg0, arg1) {
      arg0.create = arg1 !== 0;
    }, "__wbg_set_create_ef897736206a6f05"),
    __wbg_stack_3b0d974bbf31e44f: /* @__PURE__ */ __name(function (arg0, arg1) {
      const ret = arg1.stack;
      const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
      const len1 = WASM_VECTOR_LEN;
      getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
      getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
    }, "__wbg_stack_3b0d974bbf31e44f"),
    __wbg_static_accessor_GLOBAL_8adb955bd33fac2f: /* @__PURE__ */ __name(function () {
      const ret = typeof global === "undefined" ? null : global;
      return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
    }, "__wbg_static_accessor_GLOBAL_8adb955bd33fac2f"),
    __wbg_static_accessor_GLOBAL_THIS_ad356e0db91c7913: /* @__PURE__ */ __name(function () {
      const ret = typeof globalThis === "undefined" ? null : globalThis;
      return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
    }, "__wbg_static_accessor_GLOBAL_THIS_ad356e0db91c7913"),
    __wbg_static_accessor_SELF_f207c857566db248: /* @__PURE__ */ __name(function () {
      const ret = typeof self === "undefined" ? null : self;
      return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
    }, "__wbg_static_accessor_SELF_f207c857566db248"),
    __wbg_static_accessor_WINDOW_bb9f1ba69d61b386: /* @__PURE__ */ __name(function () {
      const ret = typeof window === "undefined" ? null : window;
      return isLikeNone(ret) ? 0 : addToExternrefTable0(ret);
    }, "__wbg_static_accessor_WINDOW_bb9f1ba69d61b386"),
    __wbg_storage_8d917976d6753ee0: /* @__PURE__ */ __name(function (arg0) {
      const ret = arg0.storage;
      return ret;
    }, "__wbg_storage_8d917976d6753ee0"),
    __wbg_then_098abe61755d12f6: /* @__PURE__ */ __name(function (arg0, arg1) {
      const ret = arg0.then(arg1);
      return ret;
    }, "__wbg_then_098abe61755d12f6"),
    __wbg_then_9e335f6dd892bc11: /* @__PURE__ */ __name(function (arg0, arg1, arg2) {
      const ret = arg0.then(arg1, arg2);
      return ret;
    }, "__wbg_then_9e335f6dd892bc11"),
    __wbg_value_21fc78aab0322612: /* @__PURE__ */ __name(function (arg0) {
      const ret = arg0.value;
      return ret;
    }, "__wbg_value_21fc78aab0322612"),
    __wbg_warn_173c62eb2a78dd0b: /* @__PURE__ */ __name(function (arg0, arg1) {
      let deferred0_0;
      let deferred0_1;
      try {
        deferred0_0 = arg0;
        deferred0_1 = arg1;
        console.warn(getStringFromWasm0(arg0, arg1));
      } finally {
        wasm.__wbindgen_free(deferred0_0, deferred0_1, 1);
      }
    }, "__wbg_warn_173c62eb2a78dd0b"),
    __wbg_warn_783eb0d84a16b85c: /* @__PURE__ */ __name(function (
      arg0,
      arg1,
      arg2,
      arg3,
      arg4,
      arg5,
      arg6,
      arg7,
    ) {
      let deferred0_0;
      let deferred0_1;
      try {
        deferred0_0 = arg0;
        deferred0_1 = arg1;
        console.warn(
          getStringFromWasm0(arg0, arg1),
          getStringFromWasm0(arg2, arg3),
          getStringFromWasm0(arg4, arg5),
          getStringFromWasm0(arg6, arg7),
        );
      } finally {
        wasm.__wbindgen_free(deferred0_0, deferred0_1, 1);
      }
    }, "__wbg_warn_783eb0d84a16b85c"),
    __wbg_wasmruntime_new: /* @__PURE__ */ __name(function (arg0) {
      const ret = WasmRuntime.__wrap(arg0);
      return ret;
    }, "__wbg_wasmruntime_new"),
    __wbg_write_57c477a82b886339: /* @__PURE__ */ __name(function () {
      return handleError(function (arg0, arg1, arg2, arg3) {
        const ret = arg0.write(getArrayU8FromWasm0(arg1, arg2), arg3);
        return ret;
      }, arguments);
    }, "__wbg_write_57c477a82b886339"),
    __wbindgen_cast_0000000000000001: /* @__PURE__ */ __name(function (arg0, arg1) {
      const ret = makeMutClosure(
        arg0,
        arg1,
        wasm.wasm_bindgen__closure__destroy__h69bd3a437cc03f08,
        wasm_bindgen__convert__closures_____invoke__h23c7399fde9998d2,
      );
      return ret;
    }, "__wbindgen_cast_0000000000000001"),
    __wbindgen_cast_0000000000000002: /* @__PURE__ */ __name(function (arg0) {
      const ret = arg0;
      return ret;
    }, "__wbindgen_cast_0000000000000002"),
    __wbindgen_cast_0000000000000003: /* @__PURE__ */ __name(function (arg0) {
      const ret = arg0;
      return ret;
    }, "__wbindgen_cast_0000000000000003"),
    __wbindgen_cast_0000000000000004: /* @__PURE__ */ __name(function (arg0, arg1) {
      const ret = getStringFromWasm0(arg0, arg1);
      return ret;
    }, "__wbindgen_cast_0000000000000004"),
    __wbindgen_cast_0000000000000005: /* @__PURE__ */ __name(function (arg0) {
      const ret = BigInt.asUintN(64, arg0);
      return ret;
    }, "__wbindgen_cast_0000000000000005"),
    __wbindgen_init_externref_table: /* @__PURE__ */ __name(function () {
      const table = wasm.__wbindgen_externrefs;
      const offset = table.grow(4);
      table.set(0, void 0);
      table.set(offset + 0, void 0);
      table.set(offset + 1, null);
      table.set(offset + 2, true);
      table.set(offset + 3, false);
    }, "__wbindgen_init_externref_table"),
  };
  return {
    __proto__: null,
    "./jazz_wasm_bg.js": import0,
  };
}
function wasm_bindgen__convert__closures_____invoke__h23c7399fde9998d2(arg0, arg1, arg2) {
  const ret = wasm.wasm_bindgen__convert__closures_____invoke__h23c7399fde9998d2(arg0, arg1, arg2);
  if (ret[1]) {
    throw takeFromExternrefTable0(ret[0]);
  }
}
function wasm_bindgen__convert__closures_____invoke__h5f63d9cdaddb36c3(arg0, arg1, arg2, arg3) {
  wasm.wasm_bindgen__convert__closures_____invoke__h5f63d9cdaddb36c3(arg0, arg1, arg2, arg3);
}
function addToExternrefTable0(obj) {
  const idx = wasm.__externref_table_alloc();
  wasm.__wbindgen_externrefs.set(idx, obj);
  return idx;
}
function debugString(val) {
  const type = typeof val;
  if (type == "number" || type == "boolean" || val == null) {
    return `${val}`;
  }
  if (type == "string") {
    return `"${val}"`;
  }
  if (type == "symbol") {
    const description = val.description;
    if (description == null) {
      return "Symbol";
    } else {
      return `Symbol(${description})`;
    }
  }
  if (type == "function") {
    const name = val.name;
    if (typeof name == "string" && name.length > 0) {
      return `Function(${name})`;
    } else {
      return "Function";
    }
  }
  if (Array.isArray(val)) {
    const length = val.length;
    let debug = "[";
    if (length > 0) {
      debug += debugString(val[0]);
    }
    for (let i = 1; i < length; i++) {
      debug += ", " + debugString(val[i]);
    }
    debug += "]";
    return debug;
  }
  const builtInMatches = /\[object ([^\]]+)\]/.exec(toString.call(val));
  let className;
  if (builtInMatches && builtInMatches.length > 1) {
    className = builtInMatches[1];
  } else {
    return toString.call(val);
  }
  if (className == "Object") {
    try {
      return "Object(" + JSON.stringify(val) + ")";
    } catch (_) {
      return "Object";
    }
  }
  if (val instanceof Error) {
    return `${val.name}: ${val.message}
${val.stack}`;
  }
  return className;
}
function getArrayU8FromWasm0(ptr, len) {
  ptr = ptr >>> 0;
  return getUint8ArrayMemory0().subarray(ptr / 1, ptr / 1 + len);
}
function getDataViewMemory0() {
  if (
    cachedDataViewMemory0 === null ||
    cachedDataViewMemory0.buffer.detached === true ||
    (cachedDataViewMemory0.buffer.detached === void 0 &&
      cachedDataViewMemory0.buffer !== wasm.memory.buffer)
  ) {
    cachedDataViewMemory0 = new DataView(wasm.memory.buffer);
  }
  return cachedDataViewMemory0;
}
function getStringFromWasm0(ptr, len) {
  ptr = ptr >>> 0;
  return decodeText(ptr, len);
}
function getUint8ArrayMemory0() {
  if (cachedUint8ArrayMemory0 === null || cachedUint8ArrayMemory0.byteLength === 0) {
    cachedUint8ArrayMemory0 = new Uint8Array(wasm.memory.buffer);
  }
  return cachedUint8ArrayMemory0;
}
function handleError(f, args) {
  try {
    return f.apply(this, args);
  } catch (e) {
    const idx = addToExternrefTable0(e);
    wasm.__wbindgen_exn_store(idx);
  }
}
function isLikeNone(x) {
  return x === void 0 || x === null;
}
function makeMutClosure(arg0, arg1, dtor, f) {
  const state = { a: arg0, b: arg1, cnt: 1, dtor };
  const real = /* @__PURE__ */ __name((...args) => {
    state.cnt++;
    const a = state.a;
    state.a = 0;
    try {
      return f(a, state.b, ...args);
    } finally {
      state.a = a;
      real._wbg_cb_unref();
    }
  }, "real");
  real._wbg_cb_unref = () => {
    if (--state.cnt === 0) {
      state.dtor(state.a, state.b);
      state.a = 0;
      CLOSURE_DTORS.unregister(state);
    }
  };
  CLOSURE_DTORS.register(real, state, state);
  return real;
}
function passArrayJsValueToWasm0(array, malloc) {
  const ptr = malloc(array.length * 4, 4) >>> 0;
  for (let i = 0; i < array.length; i++) {
    const add = addToExternrefTable0(array[i]);
    getDataViewMemory0().setUint32(ptr + 4 * i, add, true);
  }
  WASM_VECTOR_LEN = array.length;
  return ptr;
}
function passStringToWasm0(arg, malloc, realloc) {
  if (realloc === void 0) {
    const buf = cachedTextEncoder.encode(arg);
    const ptr2 = malloc(buf.length, 1) >>> 0;
    getUint8ArrayMemory0()
      .subarray(ptr2, ptr2 + buf.length)
      .set(buf);
    WASM_VECTOR_LEN = buf.length;
    return ptr2;
  }
  let len = arg.length;
  let ptr = malloc(len, 1) >>> 0;
  const mem = getUint8ArrayMemory0();
  let offset = 0;
  for (; offset < len; offset++) {
    const code = arg.charCodeAt(offset);
    if (code > 127) break;
    mem[ptr + offset] = code;
  }
  if (offset !== len) {
    if (offset !== 0) {
      arg = arg.slice(offset);
    }
    ptr = realloc(ptr, len, (len = offset + arg.length * 3), 1) >>> 0;
    const view = getUint8ArrayMemory0().subarray(ptr + offset, ptr + len);
    const ret = cachedTextEncoder.encodeInto(arg, view);
    offset += ret.written;
    ptr = realloc(ptr, len, offset, 1) >>> 0;
  }
  WASM_VECTOR_LEN = offset;
  return ptr;
}
function takeFromExternrefTable0(idx) {
  const value = wasm.__wbindgen_externrefs.get(idx);
  wasm.__externref_table_dealloc(idx);
  return value;
}
function decodeText(ptr, len) {
  numBytesDecoded += len;
  if (numBytesDecoded >= MAX_SAFARI_DECODE_BYTES) {
    cachedTextDecoder = new TextDecoder("utf-8", { ignoreBOM: true, fatal: true });
    cachedTextDecoder.decode();
    numBytesDecoded = len;
  }
  return cachedTextDecoder.decode(getUint8ArrayMemory0().subarray(ptr, ptr + len));
}
function __wbg_finalize_init(instance, module) {
  wasm = instance.exports;
  wasmModule = module;
  cachedDataViewMemory0 = null;
  cachedUint8ArrayMemory0 = null;
  wasm.__wbindgen_start();
  return wasm;
}
async function __wbg_load(module, imports) {
  if (typeof Response === "function" && module instanceof Response) {
    if (typeof WebAssembly.instantiateStreaming === "function") {
      try {
        return await WebAssembly.instantiateStreaming(module, imports);
      } catch (e) {
        const validResponse = module.ok && expectedResponseType(module.type);
        if (validResponse && module.headers.get("Content-Type") !== "application/wasm") {
          console.warn(
            "`WebAssembly.instantiateStreaming` failed because your server does not serve Wasm with `application/wasm` MIME type. Falling back to `WebAssembly.instantiate` which is slower. Original error:\n",
            e,
          );
        } else {
          throw e;
        }
      }
    }
    const bytes = await module.arrayBuffer();
    return await WebAssembly.instantiate(bytes, imports);
  } else {
    const instance = await WebAssembly.instantiate(module, imports);
    if (instance instanceof WebAssembly.Instance) {
      return { instance, module };
    } else {
      return instance;
    }
  }
  function expectedResponseType(type) {
    switch (type) {
      case "basic":
      case "cors":
      case "default":
        return true;
    }
    return false;
  }
  __name(expectedResponseType, "expectedResponseType");
}
function initSync(module) {
  if (wasm !== void 0) return wasm;
  if (module !== void 0) {
    if (Object.getPrototypeOf(module) === Object.prototype) {
      ({ module } = module);
    } else {
      console.warn("using deprecated parameters for `initSync()`; pass a single object instead");
    }
  }
  const imports = __wbg_get_imports();
  if (!(module instanceof WebAssembly.Module)) {
    module = new WebAssembly.Module(module);
  }
  const instance = new WebAssembly.Instance(module, imports);
  return __wbg_finalize_init(instance, module);
}
async function __wbg_init(module_or_path) {
  if (wasm !== void 0) return wasm;
  if (module_or_path !== void 0) {
    if (Object.getPrototypeOf(module_or_path) === Object.prototype) {
      ({ module_or_path } = module_or_path);
    } else {
      console.warn(
        "using deprecated parameters for the initialization function; pass a single object instead",
      );
    }
  }
  if (module_or_path === void 0) {
    module_or_path = new URL("jazz_wasm_bg.wasm", import.meta.url);
  }
  const imports = __wbg_get_imports();
  if (
    typeof module_or_path === "string" ||
    (typeof Request === "function" && module_or_path instanceof Request) ||
    (typeof URL === "function" && module_or_path instanceof URL)
  ) {
    module_or_path = fetch(module_or_path);
  }
  const { instance, module } = await __wbg_load(await module_or_path, imports);
  return __wbg_finalize_init(instance, module);
}
var WasmQueryBuilder,
  WasmRuntime,
  WasmQueryBuilderFinalization,
  WasmRuntimeFinalization,
  CLOSURE_DTORS,
  cachedDataViewMemory0,
  cachedUint8ArrayMemory0,
  cachedTextDecoder,
  MAX_SAFARI_DECODE_BYTES,
  numBytesDecoded,
  cachedTextEncoder,
  WASM_VECTOR_LEN,
  wasmModule,
  wasm;
var init_jazz_wasm = __esm({
  "../../crates/jazz-wasm/pkg/jazz_wasm.js"() {
    WasmQueryBuilder = class _WasmQueryBuilder {
      static {
        __name(this, "WasmQueryBuilder");
      }
      static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(_WasmQueryBuilder.prototype);
        obj.__wbg_ptr = ptr;
        WasmQueryBuilderFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
      }
      __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        WasmQueryBuilderFinalization.unregister(this);
        return ptr;
      }
      free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_wasmquerybuilder_free(ptr, 0);
      }
      /**
       * Set a table alias.
       * @param {string} alias
       * @returns {WasmQueryBuilder}
       */
      alias(alias) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(alias, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmquerybuilder_alias(ptr, ptr0, len0);
        return _WasmQueryBuilder.__wrap(ret);
      }
      /**
       * Set the branch to query.
       * @param {string} branch
       * @returns {WasmQueryBuilder}
       */
      branch(branch) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(branch, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmquerybuilder_branch(ptr, ptr0, len0);
        return _WasmQueryBuilder.__wrap(ret);
      }
      /**
       * Set multiple branches to query.
       * @param {string[]} branches
       * @returns {WasmQueryBuilder}
       */
      branches(branches) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passArrayJsValueToWasm0(branches, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmquerybuilder_branches(ptr, ptr0, len0);
        return _WasmQueryBuilder.__wrap(ret);
      }
      /**
       * Build the query and return as JSON string.
       * @returns {string}
       */
      build() {
        let deferred2_0;
        let deferred2_1;
        try {
          const ptr = this.__destroy_into_raw();
          const ret = wasm.wasmquerybuilder_build(ptr);
          var ptr1 = ret[0];
          var len1 = ret[1];
          if (ret[3]) {
            ptr1 = 0;
            len1 = 0;
            throw takeFromExternrefTable0(ret[2]);
          }
          deferred2_0 = ptr1;
          deferred2_1 = len1;
          return getStringFromWasm0(ptr1, len1);
        } finally {
          wasm.__wbindgen_free(deferred2_0, deferred2_1, 1);
        }
      }
      /**
       * Build and return as JsValue.
       * @returns {any}
       */
      buildJs() {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.wasmquerybuilder_buildJs(ptr);
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
      }
      /**
       * Add an equals filter.
       * @param {string} column
       * @param {any} value
       * @returns {WasmQueryBuilder}
       */
      filterEq(column, value) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(column, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmquerybuilder_filterEq(ptr, ptr0, len0, value);
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return _WasmQueryBuilder.__wrap(ret[0]);
      }
      /**
       * Add a greater-than-or-equal filter.
       * @param {string} column
       * @param {any} value
       * @returns {WasmQueryBuilder}
       */
      filterGe(column, value) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(column, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmquerybuilder_filterGe(ptr, ptr0, len0, value);
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return _WasmQueryBuilder.__wrap(ret[0]);
      }
      /**
       * Add a greater-than filter.
       * @param {string} column
       * @param {any} value
       * @returns {WasmQueryBuilder}
       */
      filterGt(column, value) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(column, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmquerybuilder_filterGt(ptr, ptr0, len0, value);
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return _WasmQueryBuilder.__wrap(ret[0]);
      }
      /**
       * Add a less-than-or-equal filter.
       * @param {string} column
       * @param {any} value
       * @returns {WasmQueryBuilder}
       */
      filterLe(column, value) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(column, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmquerybuilder_filterLe(ptr, ptr0, len0, value);
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return _WasmQueryBuilder.__wrap(ret[0]);
      }
      /**
       * Add a less-than filter.
       * @param {string} column
       * @param {any} value
       * @returns {WasmQueryBuilder}
       */
      filterLt(column, value) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(column, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmquerybuilder_filterLt(ptr, ptr0, len0, value);
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return _WasmQueryBuilder.__wrap(ret[0]);
      }
      /**
       * Add a not-equals filter.
       * @param {string} column
       * @param {any} value
       * @returns {WasmQueryBuilder}
       */
      filterNe(column, value) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(column, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmquerybuilder_filterNe(ptr, ptr0, len0, value);
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return _WasmQueryBuilder.__wrap(ret[0]);
      }
      /**
       * Include soft-deleted rows.
       * @returns {WasmQueryBuilder}
       */
      includeDeleted() {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.wasmquerybuilder_includeDeleted(ptr);
        return _WasmQueryBuilder.__wrap(ret);
      }
      /**
       * Join another table.
       * @param {string} table
       * @returns {WasmQueryBuilder}
       */
      join(table) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmquerybuilder_join(ptr, ptr0, len0);
        return _WasmQueryBuilder.__wrap(ret);
      }
      /**
       * Set a limit.
       * @param {number} n
       * @returns {WasmQueryBuilder}
       */
      limit(n) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.wasmquerybuilder_limit(ptr, n);
        return _WasmQueryBuilder.__wrap(ret);
      }
      /**
       * Create a new QueryBuilder for a table.
       * @param {string} table
       */
      constructor(table) {
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmquerybuilder_new(ptr0, len0);
        this.__wbg_ptr = ret >>> 0;
        WasmQueryBuilderFinalization.register(this, this.__wbg_ptr, this);
        return this;
      }
      /**
       * Set an offset.
       * @param {number} n
       * @returns {WasmQueryBuilder}
       */
      offset(n) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.wasmquerybuilder_offset(ptr, n);
        return _WasmQueryBuilder.__wrap(ret);
      }
      /**
       * Specify join condition.
       * @param {string} left_col
       * @param {string} right_col
       * @returns {WasmQueryBuilder}
       */
      on(left_col, right_col) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(left_col, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(right_col, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.wasmquerybuilder_on(ptr, ptr0, len0, ptr1, len1);
        return _WasmQueryBuilder.__wrap(ret);
      }
      /**
       * Start a new OR branch.
       * @returns {WasmQueryBuilder}
       */
      or() {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.wasmquerybuilder_or(ptr);
        return _WasmQueryBuilder.__wrap(ret);
      }
      /**
       * Add ascending order by.
       * @param {string} column
       * @returns {WasmQueryBuilder}
       */
      orderBy(column) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(column, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmquerybuilder_orderBy(ptr, ptr0, len0);
        return _WasmQueryBuilder.__wrap(ret);
      }
      /**
       * Add descending order by.
       * @param {string} column
       * @returns {WasmQueryBuilder}
       */
      orderByDesc(column) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(column, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmquerybuilder_orderByDesc(ptr, ptr0, len0);
        return _WasmQueryBuilder.__wrap(ret);
      }
      /**
       * Select specific columns.
       * @param {string[]} columns
       * @returns {WasmQueryBuilder}
       */
      select(columns) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passArrayJsValueToWasm0(columns, wasm.__wbindgen_malloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmquerybuilder_select(ptr, ptr0, len0);
        return _WasmQueryBuilder.__wrap(ret);
      }
    };
    if (Symbol.dispose)
      WasmQueryBuilder.prototype[Symbol.dispose] = WasmQueryBuilder.prototype.free;
    WasmRuntime = class _WasmRuntime {
      static {
        __name(this, "WasmRuntime");
      }
      static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(_WasmRuntime.prototype);
        obj.__wbg_ptr = ptr;
        WasmRuntimeFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
      }
      __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        WasmRuntimeFinalization.unregister(this);
        return ptr;
      }
      free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_wasmruntime_free(ptr, 0);
      }
      /**
       * Debug helper: expose schema/lens state currently loaded in SchemaManager.
       * @returns {any}
       */
      __debugSchemaState() {
        const ret = wasm.wasmruntime___debugSchemaState(this.__wbg_ptr);
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
      }
      /**
       * Debug helper: seed a historical schema and persist schema/lens catalogue objects.
       * @param {string} schema_json
       */
      __debugSeedLiveSchema(schema_json) {
        const ptr0 = passStringToWasm0(
          schema_json,
          wasm.__wbindgen_malloc,
          wasm.__wbindgen_realloc,
        );
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime___debugSeedLiveSchema(this.__wbg_ptr, ptr0, len0);
        if (ret[1]) {
          throw takeFromExternrefTable0(ret[0]);
        }
      }
      /**
       * Add a client connection (for server-side use in tests).
       * @returns {string}
       */
      addClient() {
        let deferred1_0;
        let deferred1_1;
        try {
          const ret = wasm.wasmruntime_addClient(this.__wbg_ptr);
          deferred1_0 = ret[0];
          deferred1_1 = ret[1];
          return getStringFromWasm0(ret[0], ret[1]);
        } finally {
          wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
      }
      /**
       * Add a server connection.
       *
       * After adding the server, immediately flushes the outbox so that
       * catalogue sync messages (from queue_full_sync_to_server) are sent
       * before the call returns, rather than being deferred to a microtask.
       * @param {string | null} [server_catalogue_state_hash]
       */
      addServer(server_catalogue_state_hash) {
        var ptr0 = isLikeNone(server_catalogue_state_hash)
          ? 0
          : passStringToWasm0(
              server_catalogue_state_hash,
              wasm.__wbindgen_malloc,
              wasm.__wbindgen_realloc,
            );
        var len0 = WASM_VECTOR_LEN;
        wasm.wasmruntime_addServer(this.__wbg_ptr, ptr0, len0);
      }
      /**
       * Phase 1 of 2-phase subscribe: allocate a handle and store query params.
       * No compilation, no sync, no tick — just bookkeeping.
       * @param {string} query_json
       * @param {string | null} [session_json]
       * @param {string | null} [settled_tier]
       * @param {string | null} [options_json]
       * @returns {number}
       */
      createSubscription(query_json, session_json, settled_tier, options_json) {
        const ptr0 = passStringToWasm0(query_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(session_json)
          ? 0
          : passStringToWasm0(session_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len1 = WASM_VECTOR_LEN;
        var ptr2 = isLikeNone(settled_tier)
          ? 0
          : passStringToWasm0(settled_tier, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len2 = WASM_VECTOR_LEN;
        var ptr3 = isLikeNone(options_json)
          ? 0
          : passStringToWasm0(options_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len3 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_createSubscription(
          this.__wbg_ptr,
          ptr0,
          len0,
          ptr1,
          len1,
          ptr2,
          len2,
          ptr3,
          len3,
        );
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return ret[0];
      }
      /**
       * Delete a row by ObjectId.
       * @param {string} object_id
       */
      delete(object_id) {
        const ptr0 = passStringToWasm0(object_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_delete(this.__wbg_ptr, ptr0, len0);
        if (ret[1]) {
          throw takeFromExternrefTable0(ret[0]);
        }
      }
      /**
       * Delete a row and return a Promise that resolves when the tier acks.
       * @param {string} object_id
       * @param {string} tier
       * @returns {Promise<any>}
       */
      deleteDurable(object_id, tier) {
        const ptr0 = passStringToWasm0(object_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(tier, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_deleteDurable(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
      }
      /**
       * Delete a row and return a Promise that resolves when the tier acks,
       * scoped to an explicit session principal.
       * @param {string} object_id
       * @param {string | null | undefined} write_context_json
       * @param {string} tier
       * @returns {Promise<any>}
       */
      deleteDurableWithSession(object_id, write_context_json, tier) {
        const ptr0 = passStringToWasm0(object_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(write_context_json)
          ? 0
          : passStringToWasm0(write_context_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(tier, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_deleteDurableWithSession(
          this.__wbg_ptr,
          ptr0,
          len0,
          ptr1,
          len1,
          ptr2,
          len2,
        );
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
      }
      /**
       * Delete a row by ObjectId as an explicit session principal.
       * @param {string} object_id
       * @param {string | null} [write_context_json]
       */
      deleteWithSession(object_id, write_context_json) {
        const ptr0 = passStringToWasm0(object_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(write_context_json)
          ? 0
          : passStringToWasm0(write_context_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len1 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_deleteWithSession(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        if (ret[1]) {
          throw takeFromExternrefTable0(ret[0]);
        }
      }
      /**
       * Phase 2 of 2-phase subscribe: compile graph, register subscription,
       * sync to servers, attach callback, and deliver the first delta.
       *
       * No-ops silently if the handle was already unsubscribed.
       * @param {number} handle
       * @param {Function} on_update
       */
      executeSubscription(handle, on_update) {
        const ret = wasm.wasmruntime_executeSubscription(this.__wbg_ptr, handle, on_update);
        if (ret[1]) {
          throw takeFromExternrefTable0(ret[0]);
        }
      }
      /**
       * Flush all data to persistent storage (snapshot).
       */
      flush() {
        wasm.wasmruntime_flush(this.__wbg_ptr);
      }
      /**
       * Flush only the WAL buffer to OPFS (not the snapshot).
       */
      flushWal() {
        wasm.wasmruntime_flushWal(this.__wbg_ptr);
      }
      /**
       * Get the current schema as JSON.
       * @returns {any}
       */
      getSchema() {
        const ret = wasm.wasmruntime_getSchema(this.__wbg_ptr);
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
      }
      /**
       * Get the canonical schema hash (64-char hex).
       * @returns {string}
       */
      getSchemaHash() {
        let deferred1_0;
        let deferred1_1;
        try {
          const ret = wasm.wasmruntime_getSchemaHash(this.__wbg_ptr);
          deferred1_0 = ret[0];
          deferred1_1 = ret[1];
          return getStringFromWasm0(ret[0], ret[1]);
        } finally {
          wasm.__wbindgen_free(deferred1_0, deferred1_1, 1);
        }
      }
      /**
       * Insert a row into a table.
       *
       * # Returns
       * The inserted row as `{ id, values }`.
       * @param {string} table
       * @param {any} values
       * @returns {any}
       */
      insert(table, values) {
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_insert(this.__wbg_ptr, ptr0, len0, values);
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
      }
      /**
       * Insert a row and return a Promise that resolves when the tier acks.
       *
       * `tier` must be one of: "worker", "edge", "global".
       * @param {string} table
       * @param {any} values
       * @param {string} tier
       * @returns {Promise<any>}
       */
      insertDurable(table, values, tier) {
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(tier, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_insertDurable(this.__wbg_ptr, ptr0, len0, values, ptr1, len1);
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
      }
      /**
       * Insert a row and return a Promise that resolves when the tier acks,
       * scoped to an explicit session principal.
       * @param {string} table
       * @param {any} values
       * @param {string | null | undefined} write_context_json
       * @param {string} tier
       * @returns {Promise<any>}
       */
      insertDurableWithSession(table, values, write_context_json, tier) {
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(write_context_json)
          ? 0
          : passStringToWasm0(write_context_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(tier, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_insertDurableWithSession(
          this.__wbg_ptr,
          ptr0,
          len0,
          values,
          ptr1,
          len1,
          ptr2,
          len2,
        );
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
      }
      /**
       * Insert a row into a table as an explicit session principal.
       * @param {string} table
       * @param {any} values
       * @param {string | null} [write_context_json]
       * @returns {any}
       */
      insertWithSession(table, values, write_context_json) {
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(write_context_json)
          ? 0
          : passStringToWasm0(write_context_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len1 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_insertWithSession(
          this.__wbg_ptr,
          ptr0,
          len0,
          values,
          ptr1,
          len1,
        );
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
      }
      /**
       * Create a new WasmRuntime.
       *
       * Storage is synchronous (in-memory via MemoryStorage).
       *
       * # Arguments
       * * `schema_json` - JSON-encoded schema definition
       * * `app_id` - Application identifier
       * * `env` - Environment (e.g., "dev", "prod")
       * * `user_branch` - User's branch name (e.g., "main")
       * * `tier` - Optional node durability tier ("worker", "edge", "global").
       *            Set for server nodes to enable ack emission.
       * * `use_binary_encoding` - Optional outgoing sync payload encoding mode.
       *   `Some(true)` emits postcard bytes (`Uint8Array`), otherwise JSON strings.
       * @param {string} schema_json
       * @param {string} app_id
       * @param {string} env
       * @param {string} user_branch
       * @param {string | null} [tier]
       * @param {boolean | null} [use_binary_encoding]
       */
      constructor(schema_json, app_id, env, user_branch, tier, use_binary_encoding) {
        const ptr0 = passStringToWasm0(
          schema_json,
          wasm.__wbindgen_malloc,
          wasm.__wbindgen_realloc,
        );
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(app_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(env, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        const ptr3 = passStringToWasm0(
          user_branch,
          wasm.__wbindgen_malloc,
          wasm.__wbindgen_realloc,
        );
        const len3 = WASM_VECTOR_LEN;
        var ptr4 = isLikeNone(tier)
          ? 0
          : passStringToWasm0(tier, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len4 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_new(
          ptr0,
          len0,
          ptr1,
          len1,
          ptr2,
          len2,
          ptr3,
          len3,
          ptr4,
          len4,
          isLikeNone(use_binary_encoding) ? 16777215 : use_binary_encoding ? 1 : 0,
        );
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        this.__wbg_ptr = ret[0] >>> 0;
        WasmRuntimeFinalization.register(this, this.__wbg_ptr, this);
        return this;
      }
      /**
       * Called by JS when a sync message arrives from the server.
       *
       * # Arguments
       * * `payload` - Either postcard-encoded SyncPayload bytes (`Uint8Array`)
       *   or JSON-encoded SyncPayload (`string`)
       * @param {any} payload
       */
      onSyncMessageReceived(payload) {
        const ret = wasm.wasmruntime_onSyncMessageReceived(this.__wbg_ptr, payload);
        if (ret[1]) {
          throw takeFromExternrefTable0(ret[0]);
        }
      }
      /**
       * Called by JS when a sync message arrives from a client (not a server).
       *
       * # Arguments
       * * `client_id` - UUID string of the sending client
       * * `payload` - Postcard-encoded SyncPayload bytes
       * @param {string} client_id
       * @param {any} payload
       */
      onSyncMessageReceivedFromClient(client_id, payload) {
        const ptr0 = passStringToWasm0(client_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_onSyncMessageReceivedFromClient(
          this.__wbg_ptr,
          ptr0,
          len0,
          payload,
        );
        if (ret[1]) {
          throw takeFromExternrefTable0(ret[0]);
        }
      }
      /**
       * Register a callback for outgoing sync messages.
       * @param {Function} callback
       */
      onSyncMessageToSend(callback) {
        wasm.wasmruntime_onSyncMessageToSend(this.__wbg_ptr, callback);
      }
      /**
       * Create a persistent WasmRuntime backed by OPFS.
       *
       * Opens a single OPFS file namespace and restores state from the latest
       * durable checkpoint.
       * @param {string} schema_json
       * @param {string} app_id
       * @param {string} env
       * @param {string} user_branch
       * @param {string} db_name
       * @param {string | null | undefined} tier
       * @param {boolean} use_binary_encoding
       * @returns {Promise<WasmRuntime>}
       */
      static openPersistent(
        schema_json,
        app_id,
        env,
        user_branch,
        db_name,
        tier,
        use_binary_encoding,
      ) {
        const ptr0 = passStringToWasm0(
          schema_json,
          wasm.__wbindgen_malloc,
          wasm.__wbindgen_realloc,
        );
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(app_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(env, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        const ptr3 = passStringToWasm0(
          user_branch,
          wasm.__wbindgen_malloc,
          wasm.__wbindgen_realloc,
        );
        const len3 = WASM_VECTOR_LEN;
        const ptr4 = passStringToWasm0(db_name, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len4 = WASM_VECTOR_LEN;
        var ptr5 = isLikeNone(tier)
          ? 0
          : passStringToWasm0(tier, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len5 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_openPersistent(
          ptr0,
          len0,
          ptr1,
          len1,
          ptr2,
          len2,
          ptr3,
          len3,
          ptr4,
          len4,
          ptr5,
          len5,
          use_binary_encoding,
        );
        return ret;
      }
      /**
       * Execute a query and return results as a Promise.
       *
       * Optional durability tier controls remote settlement behavior.
       * @param {string} query_json
       * @param {string | null} [session_json]
       * @param {string | null} [settled_tier]
       * @param {string | null} [options_json]
       * @returns {Promise<any>}
       */
      query(query_json, session_json, settled_tier, options_json) {
        const ptr0 = passStringToWasm0(query_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(session_json)
          ? 0
          : passStringToWasm0(session_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len1 = WASM_VECTOR_LEN;
        var ptr2 = isLikeNone(settled_tier)
          ? 0
          : passStringToWasm0(settled_tier, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len2 = WASM_VECTOR_LEN;
        var ptr3 = isLikeNone(options_json)
          ? 0
          : passStringToWasm0(options_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len3 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_query(
          this.__wbg_ptr,
          ptr0,
          len0,
          ptr1,
          len1,
          ptr2,
          len2,
          ptr3,
          len3,
        );
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
      }
      /**
       * Remove the current upstream server connection.
       */
      removeServer() {
        wasm.wasmruntime_removeServer(this.__wbg_ptr);
      }
      /**
       * Set a client's role.
       *
       * # Arguments
       * * `client_id` - UUID string of the client
       * * `role` - One of "user", "admin", "peer"
       * @param {string} client_id
       * @param {string} role
       */
      setClientRole(client_id, role) {
        const ptr0 = passStringToWasm0(client_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(role, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_setClientRole(this.__wbg_ptr, ptr0, len0, ptr1, len1);
        if (ret[1]) {
          throw takeFromExternrefTable0(ret[0]);
        }
      }
      /**
       * Subscribe to a query with a callback.
       *
       * Default behavior matches RuntimeCore:
       * - with upstream server: first callback waits for protocol QuerySettled convergence
       * - without upstream server: first callback is local-immediate
       *
       * Pass durability options to override this default.
       *
       * # Returns
       * Subscription handle (f64) for later unsubscription.
       * @param {string} query_json
       * @param {Function} on_update
       * @param {string | null} [session_json]
       * @param {string | null} [settled_tier]
       * @param {string | null} [options_json]
       * @returns {number}
       */
      subscribe(query_json, on_update, session_json, settled_tier, options_json) {
        const ptr0 = passStringToWasm0(query_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(session_json)
          ? 0
          : passStringToWasm0(session_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len1 = WASM_VECTOR_LEN;
        var ptr2 = isLikeNone(settled_tier)
          ? 0
          : passStringToWasm0(settled_tier, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len2 = WASM_VECTOR_LEN;
        var ptr3 = isLikeNone(options_json)
          ? 0
          : passStringToWasm0(options_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len3 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_subscribe(
          this.__wbg_ptr,
          ptr0,
          len0,
          on_update,
          ptr1,
          len1,
          ptr2,
          len2,
          ptr3,
          len3,
        );
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return ret[0];
      }
      /**
       * Unsubscribe from a query.
       * @param {number} handle
       */
      unsubscribe(handle) {
        wasm.wasmruntime_unsubscribe(this.__wbg_ptr, handle);
      }
      /**
       * Update a row by ObjectId.
       * @param {string} object_id
       * @param {any} values
       */
      update(object_id, values) {
        const ptr0 = passStringToWasm0(object_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_update(this.__wbg_ptr, ptr0, len0, values);
        if (ret[1]) {
          throw takeFromExternrefTable0(ret[0]);
        }
      }
      /**
       * Update a row and return a Promise that resolves when the tier acks.
       * @param {string} object_id
       * @param {any} values
       * @param {string} tier
       * @returns {Promise<any>}
       */
      updateDurable(object_id, values, tier) {
        const ptr0 = passStringToWasm0(object_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(tier, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_updateDurable(this.__wbg_ptr, ptr0, len0, values, ptr1, len1);
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
      }
      /**
       * Update a row and return a Promise that resolves when the tier acks,
       * scoped to an explicit session principal.
       * @param {string} object_id
       * @param {any} values
       * @param {string | null | undefined} write_context_json
       * @param {string} tier
       * @returns {Promise<any>}
       */
      updateDurableWithSession(object_id, values, write_context_json, tier) {
        const ptr0 = passStringToWasm0(object_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(write_context_json)
          ? 0
          : passStringToWasm0(write_context_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(tier, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len2 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_updateDurableWithSession(
          this.__wbg_ptr,
          ptr0,
          len0,
          values,
          ptr1,
          len1,
          ptr2,
          len2,
        );
        if (ret[2]) {
          throw takeFromExternrefTable0(ret[1]);
        }
        return takeFromExternrefTable0(ret[0]);
      }
      /**
       * Update a row by ObjectId as an explicit session principal.
       *
       * # Arguments
       * * `object_id` - UUID string of target object
       * * `values` - Partial update map (`{ columnName: Value }`)
       * * `session_json` - Optional JSON-encoded Session used for policy checks
       * @param {string} object_id
       * @param {any} values
       * @param {string | null} [write_context_json]
       */
      updateWithSession(object_id, values, write_context_json) {
        const ptr0 = passStringToWasm0(object_id, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        var ptr1 = isLikeNone(write_context_json)
          ? 0
          : passStringToWasm0(write_context_json, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len1 = WASM_VECTOR_LEN;
        const ret = wasm.wasmruntime_updateWithSession(
          this.__wbg_ptr,
          ptr0,
          len0,
          values,
          ptr1,
          len1,
        );
        if (ret[1]) {
          throw takeFromExternrefTable0(ret[0]);
        }
      }
    };
    if (Symbol.dispose) WasmRuntime.prototype[Symbol.dispose] = WasmRuntime.prototype.free;
    __name(bench_get_cache_bytes, "bench_get_cache_bytes");
    __name(bench_get_overflow_threshold_bytes, "bench_get_overflow_threshold_bytes");
    __name(bench_get_pin_internal_pages, "bench_get_pin_internal_pages");
    __name(bench_get_read_coalesce_pages, "bench_get_read_coalesce_pages");
    __name(bench_opfs_cold_random_read, "bench_opfs_cold_random_read");
    __name(bench_opfs_cold_sequential_read, "bench_opfs_cold_sequential_read");
    __name(bench_opfs_matrix, "bench_opfs_matrix");
    __name(bench_opfs_mixed_matrix, "bench_opfs_mixed_matrix");
    __name(bench_opfs_mixed_scenario, "bench_opfs_mixed_scenario");
    __name(bench_opfs_random_read, "bench_opfs_random_read");
    __name(bench_opfs_random_write, "bench_opfs_random_write");
    __name(bench_opfs_range_random_window, "bench_opfs_range_random_window");
    __name(bench_opfs_range_seq_window, "bench_opfs_range_seq_window");
    __name(bench_opfs_sequential_read, "bench_opfs_sequential_read");
    __name(bench_opfs_sequential_write, "bench_opfs_sequential_write");
    __name(bench_reset_cache_bytes, "bench_reset_cache_bytes");
    __name(bench_reset_overflow_threshold_bytes, "bench_reset_overflow_threshold_bytes");
    __name(bench_reset_pin_internal_pages, "bench_reset_pin_internal_pages");
    __name(bench_reset_read_coalesce_pages, "bench_reset_read_coalesce_pages");
    __name(bench_set_cache_bytes, "bench_set_cache_bytes");
    __name(bench_set_overflow_threshold_bytes, "bench_set_overflow_threshold_bytes");
    __name(bench_set_pin_internal_pages, "bench_set_pin_internal_pages");
    __name(bench_set_read_coalesce_pages, "bench_set_read_coalesce_pages");
    __name(currentTimestamp, "currentTimestamp");
    __name(generateId, "generateId");
    __name(init, "init");
    __name(parseSchema, "parseSchema");
    __name(__wbg_get_imports, "__wbg_get_imports");
    __name(
      wasm_bindgen__convert__closures_____invoke__h23c7399fde9998d2,
      "wasm_bindgen__convert__closures_____invoke__h23c7399fde9998d2",
    );
    __name(
      wasm_bindgen__convert__closures_____invoke__h5f63d9cdaddb36c3,
      "wasm_bindgen__convert__closures_____invoke__h5f63d9cdaddb36c3",
    );
    WasmQueryBuilderFinalization =
      typeof FinalizationRegistry === "undefined"
        ? {
            register: /* @__PURE__ */ __name(() => {}, "register"),
            unregister: /* @__PURE__ */ __name(() => {}, "unregister"),
          }
        : new FinalizationRegistry((ptr) => wasm.__wbg_wasmquerybuilder_free(ptr >>> 0, 1));
    WasmRuntimeFinalization =
      typeof FinalizationRegistry === "undefined"
        ? {
            register: /* @__PURE__ */ __name(() => {}, "register"),
            unregister: /* @__PURE__ */ __name(() => {}, "unregister"),
          }
        : new FinalizationRegistry((ptr) => wasm.__wbg_wasmruntime_free(ptr >>> 0, 1));
    __name(addToExternrefTable0, "addToExternrefTable0");
    CLOSURE_DTORS =
      typeof FinalizationRegistry === "undefined"
        ? {
            register: /* @__PURE__ */ __name(() => {}, "register"),
            unregister: /* @__PURE__ */ __name(() => {}, "unregister"),
          }
        : new FinalizationRegistry((state) => state.dtor(state.a, state.b));
    __name(debugString, "debugString");
    __name(getArrayU8FromWasm0, "getArrayU8FromWasm0");
    cachedDataViewMemory0 = null;
    __name(getDataViewMemory0, "getDataViewMemory0");
    __name(getStringFromWasm0, "getStringFromWasm0");
    cachedUint8ArrayMemory0 = null;
    __name(getUint8ArrayMemory0, "getUint8ArrayMemory0");
    __name(handleError, "handleError");
    __name(isLikeNone, "isLikeNone");
    __name(makeMutClosure, "makeMutClosure");
    __name(passArrayJsValueToWasm0, "passArrayJsValueToWasm0");
    __name(passStringToWasm0, "passStringToWasm0");
    __name(takeFromExternrefTable0, "takeFromExternrefTable0");
    cachedTextDecoder = new TextDecoder("utf-8", { ignoreBOM: true, fatal: true });
    cachedTextDecoder.decode();
    MAX_SAFARI_DECODE_BYTES = 2146435072;
    numBytesDecoded = 0;
    __name(decodeText, "decodeText");
    cachedTextEncoder = new TextEncoder();
    if (!("encodeInto" in cachedTextEncoder)) {
      cachedTextEncoder.encodeInto = function (arg, view) {
        const buf = cachedTextEncoder.encode(arg);
        view.set(buf);
        return {
          read: arg.length,
          written: buf.length,
        };
      };
    }
    WASM_VECTOR_LEN = 0;
    __name(__wbg_finalize_init, "__wbg_finalize_init");
    __name(__wbg_load, "__wbg_load");
    __name(initSync, "initSync");
    __name(__wbg_init, "__wbg_init");
  },
});

// ../../packages/jazz-tools/src/drivers/schema-wire.ts
function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
__name(isRecord, "isRecord");
function isWasmSchema(value) {
  return isRecord(value);
}
__name(isWasmSchema, "isWasmSchema");
function normalizeRuntimeSchema(schema2) {
  if (schema2 instanceof Map) {
    return Object.fromEntries(schema2.entries());
  }
  if (!isWasmSchema(schema2)) {
    throw new Error("Invalid runtime schema value.");
  }
  return schema2;
}
__name(normalizeRuntimeSchema, "normalizeRuntimeSchema");
function runtimeSchemaJsonReplacer(_key, value) {
  if (value instanceof Uint8Array) {
    return Array.from(value);
  }
  return value;
}
__name(runtimeSchemaJsonReplacer, "runtimeSchemaJsonReplacer");
function serializeRuntimeSchema(schema2) {
  return JSON.stringify(schema2, runtimeSchemaJsonReplacer);
}
__name(serializeRuntimeSchema, "serializeRuntimeSchema");

// ../../packages/jazz-tools/src/runtime/utils.ts
async function fetchWithTimeout(url, init2, timeoutMs) {
  if (typeof AbortController !== "function") {
    return fetch(url, init2);
  }
  const controller = new AbortController();
  const timeout = setTimeout(() => {
    controller.abort();
  }, timeoutMs);
  try {
    return await fetch(url, { ...init2, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}
__name(fetchWithTimeout, "fetchWithTimeout");

// ../../packages/jazz-tools/src/runtime/sync-transport.ts
function errorMessage(error) {
  if (error instanceof Error && typeof error.message === "string") {
    return error.message;
  }
  if (typeof error === "string") return error;
  return String(error);
}
__name(errorMessage, "errorMessage");
function isExpectedFetchAbortError(error, signal) {
  if (signal?.aborted) return true;
  if (error && typeof error === "object") {
    const maybeName = error.name;
    if (maybeName === "AbortError") return true;
  }
  const message = errorMessage(error).toLowerCase();
  if (message.includes("fetch request has been canceled")) return true;
  if (message.includes("fetch request has been cancelled")) return true;
  if (message.includes("the operation was aborted")) return true;
  const cause = error?.cause;
  if (cause !== void 0) {
    const causeMessage = errorMessage(cause).toLowerCase();
    if (causeMessage.includes("fetch request has been canceled")) return true;
    if (causeMessage.includes("fetch request has been cancelled")) return true;
    if (causeMessage.includes("the operation was aborted")) return true;
  }
  return false;
}
__name(isExpectedFetchAbortError, "isExpectedFetchAbortError");
function logSchemaWarningPayload(payload, logPrefix = "") {
  const warning = payload?.SchemaWarning;
  if (!warning) return;
  const rowCount = warning.rowCount ?? warning.row_count ?? 0;
  const tableName = warning.tableName ?? warning.table_name ?? "unknown";
  const fromHash = warning.fromHash ?? warning.from_hash ?? "unknown";
  const toHash = warning.toHash ?? warning.to_hash ?? "unknown";
  const shortHash = /* @__PURE__ */ __name(
    (hash) =>
      typeof hash === "string" && /^[0-9a-f]{12,}$/i.test(hash) ? hash.slice(0, 12) : hash,
    "shortHash",
  );
  console.warn(
    `${logPrefix}Detected ${rowCount} rows of ${tableName} with differing schema versions. To ensure data visibility and forward/backward compatibility please create a new migration with \`npx jazz-tools migrations create ${shortHash(fromHash)} ${shortHash(toHash)}\``,
  );
}
__name(logSchemaWarningPayload, "logSchemaWarningPayload");
var SyncStreamController = class {
  constructor(options) {
    this.options = options;
    this.logPrefix = options.logPrefix ?? "";
  }
  static {
    __name(this, "SyncStreamController");
  }
  logPrefix;
  streamAbortController = null;
  reconnectTimer = null;
  reconnectAttempt = 0;
  streamConnecting = false;
  streamAttached = false;
  activeServerUrl = null;
  activeServerPathPrefix;
  stopped = true;
  start(serverUrl, pathPrefix) {
    this.stop();
    this.stopped = false;
    this.activeServerUrl = serverUrl;
    this.activeServerPathPrefix = pathPrefix;
    this.connectStream();
  }
  stop() {
    this.stopped = true;
    this.activeServerUrl = null;
    this.activeServerPathPrefix = void 0;
    this.clearReconnectTimer();
    this.abortStream();
    this.detachServer();
  }
  updateAuth() {
    this.abortStream();
    this.detachServer();
    if (this.activeServerUrl && !this.stopped) {
      this.scheduleReconnect();
    }
  }
  notifyTransportFailure() {
    this.abortStream();
    this.detachServer();
    this.scheduleReconnect();
  }
  getServerUrl() {
    return this.activeServerUrl;
  }
  getPathPrefix() {
    return this.activeServerPathPrefix;
  }
  attachServer(catalogueStateHash) {
    if (this.streamAttached) {
      this.options.onDisconnected();
    }
    this.options.onConnected(catalogueStateHash);
    this.streamAttached = true;
    this.reconnectAttempt = 0;
  }
  detachServer() {
    if (!this.streamAttached) return;
    this.options.onDisconnected();
    this.streamAttached = false;
  }
  clearReconnectTimer() {
    if (!this.reconnectTimer) return;
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
  }
  abortStream() {
    if (!this.streamAbortController) return;
    this.streamAbortController.abort();
    this.streamAbortController = null;
  }
  scheduleReconnect() {
    if (this.stopped || !this.activeServerUrl) return;
    if (this.reconnectTimer) return;
    const baseMs = 300;
    const maxMs = 1e4;
    const jitterMs = Math.floor(Math.random() * 200);
    const delayMs = Math.min(maxMs, baseMs * 2 ** this.reconnectAttempt) + jitterMs;
    this.reconnectAttempt += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connectStream();
    }, delayMs);
  }
  async connectStream() {
    if (this.streamConnecting || this.stopped || !this.activeServerUrl) return;
    this.streamConnecting = true;
    const serverUrl = this.activeServerUrl;
    const serverPathPrefix = this.activeServerPathPrefix;
    const headers = {
      Accept: "application/octet-stream",
    };
    applySyncAuthHeaders(headers, this.options.getAuth());
    const abortController = new AbortController();
    this.streamAbortController = abortController;
    try {
      const eventsUrl = buildEventsUrl(serverUrl, this.options.getClientId(), serverPathPrefix);
      const response = await fetch(eventsUrl, {
        headers,
        signal: abortController.signal,
      });
      if (!response.ok) {
        console.error(`${this.logPrefix}Stream connect failed: ${response.status}`);
        this.detachServer();
        this.streamConnecting = false;
        this.scheduleReconnect();
        return;
      }
      if (!response.body) {
        throw new Error("Stream response did not include a body");
      }
      const reader = response.body.getReader();
      let connected = false;
      await readBinaryFrames(
        reader,
        {
          onSyncMessage: this.options.onSyncMessage,
          onConnected: /* @__PURE__ */ __name((clientId, catalogueStateHash) => {
            this.options.setClientId(clientId);
            if (!connected) {
              connected = true;
              this.attachServer(catalogueStateHash);
            }
          }, "onConnected"),
        },
        this.logPrefix,
      );
    } catch (e) {
      if (isExpectedFetchAbortError(e, abortController.signal)) return;
      console.error(`${this.logPrefix}Stream connect error:`, e);
    } finally {
      if (this.streamAbortController === abortController) {
        this.streamAbortController = null;
      }
      this.streamConnecting = false;
    }
    if (!abortController.signal.aborted && !this.stopped) {
      this.detachServer();
      this.scheduleReconnect();
    }
  }
};
function createRuntimeSyncStreamController(options) {
  return new SyncStreamController({
    logPrefix: options.logPrefix,
    getAuth: options.getAuth,
    getClientId: options.getClientId,
    setClientId: options.setClientId,
    onConnected: /* @__PURE__ */ __name(
      (catalogueStateHash) => options.getRuntime()?.addServer(catalogueStateHash),
      "onConnected",
    ),
    onDisconnected: /* @__PURE__ */ __name(
      () => options.getRuntime()?.removeServer(),
      "onDisconnected",
    ),
    onSyncMessage: /* @__PURE__ */ __name(
      (payload) => options.getRuntime()?.onSyncMessageReceived(payload),
      "onSyncMessage",
    ),
  });
}
__name(createRuntimeSyncStreamController, "createRuntimeSyncStreamController");
function isOutboxDestinationKind(value) {
  return value === "server" || value === "client";
}
__name(isOutboxDestinationKind, "isOutboxDestinationKind");
function isOutboxPayload(value) {
  return typeof value === "string" || value instanceof Uint8Array;
}
__name(isOutboxPayload, "isOutboxPayload");
function normalizeOutboxCallbackArgs(args) {
  if (isOutboxDestinationKind(args[0])) {
    const payload = args[2];
    if (!isOutboxPayload(payload)) return null;
    return {
      destinationKind: args[0],
      payload,
      isCatalogue: Boolean(args[3]),
    };
  }
  if (isOutboxDestinationKind(args[1])) {
    const payload = args[3];
    if (!isOutboxPayload(payload)) return null;
    return {
      destinationKind: args[1],
      payload,
      isCatalogue: Boolean(args[4]),
    };
  }
  if (Array.isArray(args[1]) && isOutboxDestinationKind(args[1][0])) {
    const payload = args[1][2];
    if (!isOutboxPayload(payload)) return null;
    return {
      destinationKind: args[1][0],
      payload,
      isCatalogue: Boolean(args[1][3]),
    };
  }
  return null;
}
__name(normalizeOutboxCallbackArgs, "normalizeOutboxCallbackArgs");
function createSyncOutboxRouter(options) {
  const logPrefix = options.logPrefix ?? "";
  return (...args) => {
    const normalized = normalizeOutboxCallbackArgs(args);
    if (!normalized) {
      console.error(`${logPrefix}Invalid sync outbox callback arguments`, args);
      return;
    }
    const { destinationKind, payload, isCatalogue } = normalized;
    if (destinationKind === "client") {
      options.onClientPayload?.(payload);
      return;
    }
    Promise.resolve(options.onServerPayload(payload, isCatalogue)).catch((error) => {
      if (options.onServerPayloadError) {
        options.onServerPayloadError(error);
        return;
      }
      console.error(`${logPrefix}Sync POST error:`, error);
    });
  };
}
__name(createSyncOutboxRouter, "createSyncOutboxRouter");
function generateClientId() {
  const cryptoObj = globalThis.crypto;
  if (cryptoObj && typeof cryptoObj.randomUUID === "function") {
    return cryptoObj.randomUUID();
  }
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = Math.floor(Math.random() * 16);
    const v = c === "x" ? r : (r & 3) | 8;
    return v.toString(16);
  });
}
__name(generateClientId, "generateClientId");
var fallbackClientId = null;
var SYNC_FETCH_TIMEOUT_MS = 1e4;
function getFallbackClientId() {
  if (!fallbackClientId) {
    fallbackClientId = generateClientId();
  }
  return fallbackClientId;
}
__name(getFallbackClientId, "getFallbackClientId");
function trimTrailingSlash(url) {
  return url.replace(/\/+$/, "");
}
__name(trimTrailingSlash, "trimTrailingSlash");
function normalizePathPrefix(pathPrefix) {
  if (!pathPrefix) return "";
  const trimmed = pathPrefix.trim();
  if (!trimmed) return "";
  const withoutTrailing = trimmed.replace(/\/+$/, "");
  return withoutTrailing.startsWith("/") ? withoutTrailing : `/${withoutTrailing}`;
}
__name(normalizePathPrefix, "normalizePathPrefix");
function buildEndpointUrl(serverUrl, endpoint, pathPrefix) {
  const normalizedEndpoint = endpoint.startsWith("/") ? endpoint : `/${endpoint}`;
  return `${trimTrailingSlash(serverUrl)}${normalizePathPrefix(pathPrefix)}${normalizedEndpoint}`;
}
__name(buildEndpointUrl, "buildEndpointUrl");
function buildEventsUrl(serverUrl, clientId, pathPrefix) {
  return `${buildEndpointUrl(serverUrl, "/events", pathPrefix)}?client_id=${encodeURIComponent(clientId)}`;
}
__name(buildEventsUrl, "buildEventsUrl");
function applyUserAuthHeaders(headers, auth) {
  if (auth.jwtToken) {
    headers["Authorization"] = `Bearer ${auth.jwtToken}`;
    return;
  }
  if (auth.localAuthMode && auth.localAuthToken) {
    headers["X-Jazz-Local-Mode"] = auth.localAuthMode;
    headers["X-Jazz-Local-Token"] = auth.localAuthToken;
  }
}
__name(applyUserAuthHeaders, "applyUserAuthHeaders");
function applySyncAuthHeaders(headers, auth) {
  if (auth.backendSecret) {
    headers["X-Jazz-Backend-Secret"] = auth.backendSecret;
    return;
  }
  applyUserAuthHeaders(headers, auth);
}
__name(applySyncAuthHeaders, "applySyncAuthHeaders");
async function postSyncBatch(url, headers, body, logPrefix) {
  let response;
  try {
    response = await fetchWithTimeout(
      url,
      { method: "POST", headers, body },
      SYNC_FETCH_TIMEOUT_MS,
    );
  } catch (e) {
    if (e?.name === "AbortError") {
      console.error(`${logPrefix}Sync POST timeout after ${SYNC_FETCH_TIMEOUT_MS}ms`);
      throw new Error(`${logPrefix}Sync POST failed: timeout after ${SYNC_FETCH_TIMEOUT_MS}ms`);
    }
    if (isExpectedFetchAbortError(e)) {
      throw new Error(`${logPrefix}Sync POST failed: ${errorMessage(e)}`);
    }
    console.error(`${logPrefix}Sync POST fetch error:`, e);
    throw new Error(`${logPrefix}Sync POST failed: ${errorMessage(e)}`);
  }
  if (!response.ok) {
    const statusText = response.statusText ? ` ${response.statusText}` : "";
    throw new Error(`${logPrefix}Sync POST failed: ${response.status}${statusText}`);
  }
}
__name(postSyncBatch, "postSyncBatch");
function catalogueObjectTypeFromPayloadJson(payloadJson) {
  try {
    const parsed = JSON.parse(payloadJson);
    const kind = parsed.ObjectUpdated?.metadata?.metadata?.type;
    return typeof kind === "string" ? kind : null;
  } catch {
    return null;
  }
}
__name(catalogueObjectTypeFromPayloadJson, "catalogueObjectTypeFromPayloadJson");
function isStructuralSchemaCataloguePayload(payloadJson) {
  return catalogueObjectTypeFromPayloadJson(payloadJson) === "catalogue_schema";
}
__name(isStructuralSchemaCataloguePayload, "isStructuralSchemaCataloguePayload");
async function sendSyncPayload(serverUrl, payloadJson, isCatalogue, auth, logPrefix = "") {
  const isSchemaCatalogue = isCatalogue && isStructuralSchemaCataloguePayload(payloadJson);
  if (isCatalogue && !auth.adminSecret && !isSchemaCatalogue) {
    return;
  }
  const headers = { "Content-Type": "application/json" };
  if (isCatalogue && auth.adminSecret) {
    headers["X-Jazz-Admin-Secret"] = auth.adminSecret;
  } else {
    applySyncAuthHeaders(headers, auth);
  }
  const body = `{"payloads":[${payloadJson}],"client_id":${JSON.stringify(auth.clientId ?? getFallbackClientId())}}`;
  await postSyncBatch(
    buildEndpointUrl(serverUrl, "/sync", auth.pathPrefix),
    headers,
    body,
    logPrefix,
  );
}
__name(sendSyncPayload, "sendSyncPayload");
async function linkExternalIdentity(serverUrl, auth, logPrefix = "") {
  const headers = {
    Authorization: `Bearer ${auth.jwtToken}`,
    "X-Jazz-Local-Mode": auth.localAuthMode,
    "X-Jazz-Local-Token": auth.localAuthToken,
  };
  let response;
  try {
    response = await fetchWithTimeout(
      buildEndpointUrl(serverUrl, "/auth/link-external", auth.pathPrefix),
      {
        method: "POST",
        headers,
      },
      SYNC_FETCH_TIMEOUT_MS,
    );
  } catch (e) {
    if (e?.name === "AbortError") {
      console.error(`${logPrefix}Link external timeout after ${SYNC_FETCH_TIMEOUT_MS}ms`);
      throw new Error(`${logPrefix}Link external failed: timeout after ${SYNC_FETCH_TIMEOUT_MS}ms`);
    }
    if (isExpectedFetchAbortError(e)) {
      const msg2 = e instanceof Error ? e.message : String(e);
      throw new Error(`${logPrefix}Link external failed: ${msg2}`);
    }
    console.error(`${logPrefix}Link external fetch error:`, e);
    const msg = e instanceof Error ? e.message : String(e);
    throw new Error(`${logPrefix}Link external failed: ${msg}`);
  }
  if (!response.ok) {
    const statusText = response.statusText ? ` ${response.statusText}` : "";
    const body = await response.text().catch(() => "");
    const bodySuffix = body ? `: ${body}` : "";
    throw new Error(
      `${logPrefix}Link external failed: ${response.status}${statusText}${bodySuffix}`,
    );
  }
  return await response.json();
}
__name(linkExternalIdentity, "linkExternalIdentity");
async function readBinaryFrames(reader, callbacks, logPrefix = "") {
  let buffer = new Uint8Array(0);
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    const newBuffer = new Uint8Array(buffer.length + value.length);
    newBuffer.set(buffer);
    newBuffer.set(value, buffer.length);
    buffer = newBuffer;
    while (buffer.length >= 4) {
      const len = new DataView(buffer.buffer, buffer.byteOffset).getUint32(0, false);
      if (buffer.length < 4 + len) break;
      const json2 = new TextDecoder().decode(buffer.slice(4, 4 + len));
      buffer = buffer.slice(4 + len);
      let event;
      try {
        event = JSON.parse(json2);
      } catch (error) {
        console.error(`${logPrefix}Stream parse error:`, error);
        continue;
      }
      try {
        if (event.type === "Connected" && event.client_id) {
          callbacks.onConnected?.(event.client_id, event.catalogue_state_hash ?? null);
        } else if (event.type === "SyncUpdate") {
          logSchemaWarningPayload(event.payload, logPrefix);
          callbacks.onSyncMessage(JSON.stringify(event.payload));
        }
      } catch (error) {
        console.error(`${logPrefix}Stream callback error:`, error);
      }
    }
  }
}
__name(readBinaryFrames, "readBinaryFrames");

// ../../packages/jazz-tools/src/runtime/local-auth.ts
var LOCAL_AUTH_TOKEN_STORAGE_PREFIX = "jazz-tools:local-auth-token:";
function trimOptional(value) {
  if (typeof value !== "string") return void 0;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : void 0;
}
__name(trimOptional, "trimOptional");
function generateLocalAuthToken() {
  const cryptoObj = globalThis.crypto;
  if (cryptoObj && typeof cryptoObj.randomUUID === "function") {
    return cryptoObj.randomUUID();
  }
  return `tok-${Math.random().toString(16).slice(2)}-${Date.now().toString(16)}`;
}
__name(generateLocalAuthToken, "generateLocalAuthToken");
function tryGetStorage(storage) {
  if (storage) return storage;
  if (typeof globalThis === "undefined") return void 0;
  try {
    const maybeStorage = globalThis.localStorage;
    return maybeStorage;
  } catch {
    return void 0;
  }
}
__name(tryGetStorage, "tryGetStorage");
function localAuthTokenStorageKey(appId, mode) {
  return `${LOCAL_AUTH_TOKEN_STORAGE_PREFIX}${appId}:${mode}`;
}
__name(localAuthTokenStorageKey, "localAuthTokenStorageKey");
function loadOrCreateLocalAuthToken(appId, mode, storage) {
  if (storage) {
    const key = localAuthTokenStorageKey(appId, mode);
    try {
      const existing = trimOptional(storage.getItem(key) ?? void 0);
      if (existing) return existing;
    } catch {}
    const token = generateLocalAuthToken();
    try {
      storage.setItem(key, token);
    } catch {}
    return token;
  }
  return generateLocalAuthToken();
}
__name(loadOrCreateLocalAuthToken, "loadOrCreateLocalAuthToken");
function resolveLocalAuthDefaults(config, options = {}) {
  const storage = tryGetStorage(options.storage);
  const explicitJwtToken = trimOptional(config.jwtToken);
  const explicitBackendSecret = trimOptional(config.backendSecret);
  const explicitMode = config.localAuthMode;
  const explicitToken = trimOptional(config.localAuthToken);
  if (!explicitMode && !explicitToken && (explicitJwtToken || explicitBackendSecret)) {
    return config;
  }
  let localAuthMode = explicitMode;
  let localAuthToken = explicitToken;
  if (!localAuthMode && localAuthToken) {
    localAuthMode = "anonymous";
  }
  if (!localAuthMode && !localAuthToken && storage) {
    localAuthMode = "anonymous";
  }
  if (!localAuthMode) {
    return config;
  }
  if (!localAuthToken) {
    localAuthToken = loadOrCreateLocalAuthToken(config.appId, localAuthMode, storage);
  }
  return {
    ...config,
    localAuthMode,
    localAuthToken,
  };
}
__name(resolveLocalAuthDefaults, "resolveLocalAuthDefaults");

// ../../packages/jazz-tools/src/runtime/client-session.ts
var SHA256_K = new Uint32Array([
  1116352408, 1899447441, 3049323471, 3921009573, 961987163, 1508970993, 2453635748, 2870763221,
  3624381080, 310598401, 607225278, 1426881987, 1925078388, 2162078206, 2614888103, 3248222580,
  3835390401, 4022224774, 264347078, 604807628, 770255983, 1249150122, 1555081692, 1996064986,
  2554220882, 2821834349, 2952996808, 3210313671, 3336571891, 3584528711, 113926993, 338241895,
  666307205, 773529912, 1294757372, 1396182291, 1695183700, 1986661051, 2177026350, 2456956037,
  2730485921, 2820302411, 3259730800, 3345764771, 3516065817, 3600352804, 4094571909, 275423344,
  430227734, 506948616, 659060556, 883997877, 958139571, 1322822218, 1537002063, 1747873779,
  1955562222, 2024104815, 2227730452, 2361852424, 2428436474, 2756734187, 3204031479, 3329325298,
]);
function trimOptional2(value) {
  if (typeof value !== "string") return void 0;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : void 0;
}
__name(trimOptional2, "trimOptional");
function asNonEmptyString(value) {
  return typeof value === "string" ? trimOptional2(value) : void 0;
}
__name(asNonEmptyString, "asNonEmptyString");
function isRecord2(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
__name(isRecord2, "isRecord");
function maybeBuffer() {
  return globalThis.Buffer;
}
__name(maybeBuffer, "maybeBuffer");
function utf8Encode(value) {
  if (typeof TextEncoder !== "undefined") {
    return new TextEncoder().encode(value);
  }
  const encoded = encodeURIComponent(value);
  const bytes = [];
  for (let i = 0; i < encoded.length; i += 1) {
    const char = encoded[i];
    if (char === "%") {
      bytes.push(Number.parseInt(encoded.slice(i + 1, i + 3), 16));
      i += 2;
    } else {
      bytes.push(char.charCodeAt(0));
    }
  }
  return Uint8Array.from(bytes);
}
__name(utf8Encode, "utf8Encode");
function base64UrlToBase64(input) {
  const normalized = input.replace(/-/g, "+").replace(/_/g, "/");
  const padding = normalized.length % 4;
  if (padding === 0) return normalized;
  return normalized + "=".repeat(4 - padding);
}
__name(base64UrlToBase64, "base64UrlToBase64");
function decodeBase64ToUtf8(base64) {
  const buffer = maybeBuffer();
  if (buffer) {
    try {
      return buffer.from(base64, "base64").toString("utf8");
    } catch {
      return null;
    }
  }
  if (typeof atob === "function") {
    try {
      const binary = atob(base64);
      const bytes = new Uint8Array(binary.length);
      for (let i = 0; i < binary.length; i += 1) {
        bytes[i] = binary.charCodeAt(i);
      }
      return new TextDecoder().decode(bytes);
    } catch {
      return null;
    }
  }
  return null;
}
__name(decodeBase64ToUtf8, "decodeBase64ToUtf8");
function parseJwtPayload(jwtToken) {
  const token = trimOptional2(jwtToken);
  if (!token) return null;
  const parts = token.split(".");
  if (parts.length < 2) return null;
  const payloadPart = parts[1];
  if (payloadPart === void 0) return null;
  const payloadJson = decodeBase64ToUtf8(base64UrlToBase64(payloadPart));
  if (!payloadJson) return null;
  try {
    const parsed = JSON.parse(payloadJson);
    return isRecord2(parsed) ? parsed : null;
  } catch {
    return null;
  }
}
__name(parseJwtPayload, "parseJwtPayload");
function encodeBase64(bytes) {
  const buffer = maybeBuffer();
  if (buffer) {
    return buffer.from(bytes).toString("base64");
  }
  if (typeof btoa === "function") {
    let binary = "";
    for (const byte of bytes) {
      binary += String.fromCharCode(byte);
    }
    return btoa(binary);
  }
  throw new Error("No base64 encoder available in this runtime");
}
__name(encodeBase64, "encodeBase64");
function encodeLocalPrincipalId(digest) {
  const encoded = encodeBase64(digest).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
  return `local:${encoded}`;
}
__name(encodeLocalPrincipalId, "encodeLocalPrincipalId");
function rightRotate(value, amount) {
  return (value >>> amount) | (value << (32 - amount));
}
__name(rightRotate, "rightRotate");
function sha256PureJs(input) {
  const bytes = utf8Encode(input);
  const bitLength = bytes.length * 8;
  const oneBitAppendedLength = bytes.length + 1;
  const zeroPadLength = (64 - ((oneBitAppendedLength + 8) % 64)) % 64;
  const totalLength = oneBitAppendedLength + zeroPadLength + 8;
  const padded = new Uint8Array(totalLength);
  padded.set(bytes, 0);
  padded[bytes.length] = 128;
  const bitLengthHi = Math.floor(bitLength / 4294967296);
  const bitLengthLo = bitLength >>> 0;
  padded[totalLength - 8] = (bitLengthHi >>> 24) & 255;
  padded[totalLength - 7] = (bitLengthHi >>> 16) & 255;
  padded[totalLength - 6] = (bitLengthHi >>> 8) & 255;
  padded[totalLength - 5] = bitLengthHi & 255;
  padded[totalLength - 4] = (bitLengthLo >>> 24) & 255;
  padded[totalLength - 3] = (bitLengthLo >>> 16) & 255;
  padded[totalLength - 2] = (bitLengthLo >>> 8) & 255;
  padded[totalLength - 1] = bitLengthLo & 255;
  let h0 = 1779033703;
  let h1 = 3144134277;
  let h2 = 1013904242;
  let h3 = 2773480762;
  let h4 = 1359893119;
  let h5 = 2600822924;
  let h6 = 528734635;
  let h7 = 1541459225;
  const w = new Uint32Array(64);
  for (let offset = 0; offset < totalLength; offset += 64) {
    for (let i = 0; i < 16; i += 1) {
      const index = offset + i * 4;
      w[i] =
        ((padded[index] << 24) |
          (padded[index + 1] << 16) |
          (padded[index + 2] << 8) |
          padded[index + 3]) >>>
        0;
    }
    for (let i = 16; i < 64; i += 1) {
      const s0 = rightRotate(w[i - 15], 7) ^ rightRotate(w[i - 15], 18) ^ (w[i - 15] >>> 3);
      const s1 = rightRotate(w[i - 2], 17) ^ rightRotate(w[i - 2], 19) ^ (w[i - 2] >>> 10);
      w[i] = (w[i - 16] + s0 + w[i - 7] + s1) >>> 0;
    }
    let a = h0;
    let b = h1;
    let c = h2;
    let d = h3;
    let e = h4;
    let f = h5;
    let g = h6;
    let h = h7;
    for (let i = 0; i < 64; i += 1) {
      const sum1 = rightRotate(e, 6) ^ rightRotate(e, 11) ^ rightRotate(e, 25);
      const choice = (e & f) ^ (~e & g);
      const temp1 = (h + sum1 + choice + SHA256_K[i] + w[i]) >>> 0;
      const sum0 = rightRotate(a, 2) ^ rightRotate(a, 13) ^ rightRotate(a, 22);
      const majority = (a & b) ^ (a & c) ^ (b & c);
      const temp2 = (sum0 + majority) >>> 0;
      h = g;
      g = f;
      f = e;
      e = (d + temp1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (temp1 + temp2) >>> 0;
    }
    h0 = (h0 + a) >>> 0;
    h1 = (h1 + b) >>> 0;
    h2 = (h2 + c) >>> 0;
    h3 = (h3 + d) >>> 0;
    h4 = (h4 + e) >>> 0;
    h5 = (h5 + f) >>> 0;
    h6 = (h6 + g) >>> 0;
    h7 = (h7 + h) >>> 0;
  }
  const digest = new Uint8Array(32);
  const words = [h0, h1, h2, h3, h4, h5, h6, h7];
  for (let i = 0; i < words.length; i += 1) {
    const value = words[i];
    const base = i * 4;
    digest[base] = (value >>> 24) & 255;
    digest[base + 1] = (value >>> 16) & 255;
    digest[base + 2] = (value >>> 8) & 255;
    digest[base + 3] = value & 255;
  }
  return digest;
}
__name(sha256PureJs, "sha256PureJs");
function deriveLocalPrincipalIdSync(appId, mode, token) {
  const input = `${appId}:${mode}:${token}`;
  return encodeLocalPrincipalId(sha256PureJs(input));
}
__name(deriveLocalPrincipalIdSync, "deriveLocalPrincipalIdSync");
function resolveJwtSession(jwtToken) {
  const payload = parseJwtPayload(jwtToken);
  if (!payload) return null;
  const subject = asNonEmptyString(payload.sub);
  const issuer = asNonEmptyString(payload.iss);
  const principalId = asNonEmptyString(payload.jazz_principal_id) ?? subject;
  if (!principalId) return null;
  const claimsSource = payload.claims;
  const claims = isRecord2(claimsSource) ? { ...claimsSource } : {};
  claims.auth_mode = "external";
  if (subject) claims.subject = subject;
  if (issuer) claims.issuer = issuer;
  if (!isRecord2(claimsSource) && claimsSource !== void 0) {
    claims.raw_claims = claimsSource;
  }
  return {
    user_id: principalId,
    claims,
  };
}
__name(resolveJwtSession, "resolveJwtSession");
function resolveClientSessionSync(config) {
  const jwtSession = resolveJwtSession(config.jwtToken ?? "");
  if (jwtSession) return jwtSession;
  const localMode = config.localAuthMode;
  const localToken = trimOptional2(config.localAuthToken);
  if (!localMode || !localToken) {
    return null;
  }
  return {
    user_id: deriveLocalPrincipalIdSync(config.appId, localMode, localToken),
    claims: {
      auth_mode: "local",
      local_mode: localMode,
    },
  };
}
__name(resolveClientSessionSync, "resolveClientSessionSync");

// ../../packages/jazz-tools/src/runtime/json-text.ts
function toJsonText(value) {
  if (typeof value === "string") {
    return value;
  }
  let encoded;
  try {
    encoded = JSON.stringify(value);
  } catch (error) {
    throw new Error(
      `JSON values must be serializable: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  if (encoded === void 0) {
    throw new Error("JSON values must be serializable");
  }
  return encoded;
}
__name(toJsonText, "toJsonText");

// ../../node_modules/.pnpm/pluralize-esm@9.0.5/node_modules/pluralize-esm/dist/index.js
var pluralRules = [];
var singularRules = [];
var uncountables = /* @__PURE__ */ new Set();
var irregularPlurals = /* @__PURE__ */ new Map();
var irregularSingles = /* @__PURE__ */ new Map();
var sanitizeRule = /* @__PURE__ */ __name(
  (rule) => (typeof rule === "string" ? new RegExp("^".concat(rule, "$"), "i") : rule),
  "sanitizeRule",
);
var restoreCase = /* @__PURE__ */ __name((word, token) => {
  if (typeof token !== "string") return word;
  if (word === token) return token;
  if (word === word.toLowerCase()) return token.toLowerCase();
  if (word === word.toUpperCase()) return token.toUpperCase();
  if (word[0] === word[0].toUpperCase()) {
    return token.charAt(0).toUpperCase() + token.substr(1).toLowerCase();
  }
  return token.toLowerCase();
}, "restoreCase");
var sanitizeWord = /* @__PURE__ */ __name((token, word, rules) => {
  if (!token.length || uncountables.has(token)) {
    return word;
  }
  let { length: len } = rules;
  while (len--) {
    const rule = rules[len];
    if (rule[0].test(word)) {
      return word.replace(rule[0], function () {
        for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
          args[_key] = arguments[_key];
        }
        const [match, index] = args;
        const result = rule[1].replace(/\$(\d{1,2})/g, (_, index2) => args[index2] || "");
        if (match === "") {
          return restoreCase(word[index - 1], result);
        }
        return restoreCase(match, result);
      });
    }
  }
  return word;
}, "sanitizeWord");
var compute = /* @__PURE__ */ __name((word, replaceMap, keepMap, rules) => {
  const token = word.toLowerCase();
  if (keepMap.has(token)) {
    return restoreCase(word, token);
  }
  if (replaceMap.has(token)) {
    return restoreCase(word, replaceMap.get(token));
  }
  return sanitizeWord(token, word, rules);
}, "compute");
var mapHas = /* @__PURE__ */ __name((word, replaceMap, keepMap, rules) => {
  const token = word.toLowerCase();
  if (keepMap.has(token)) return true;
  if (replaceMap.has(token)) return false;
  return sanitizeWord(token, token, rules) === token;
}, "mapHas");
var pluralize = /* @__PURE__ */ __name((word, count, inclusive) => {
  const pluralized = count === 1 ? pluralize.singular(word) : pluralize.plural(word);
  if (inclusive) return "".concat(count, " ").concat(pluralized);
  return pluralized;
}, "pluralize");
pluralize.plural = (word) => compute(word, irregularSingles, irregularPlurals, pluralRules);
pluralize.singular = (word) => compute(word, irregularPlurals, irregularSingles, singularRules);
pluralize.addPluralRule = (rule, replacement) => {
  pluralRules.push([sanitizeRule(rule), replacement]);
};
pluralize.addSingularRule = (rule, replacement) => {
  singularRules.push([sanitizeRule(rule), replacement]);
};
pluralize.addIrregularRule = (single, plural) => {
  const _plural = plural.toLowerCase();
  const _single = single.toLowerCase();
  irregularSingles.set(_single, _plural);
  irregularPlurals.set(_plural, _single);
};
pluralize.addUncountableRule = (rule) => {
  if (typeof rule === "string") {
    uncountables.add(rule.toLowerCase());
    return;
  }
  pluralize.addPluralRule(rule, "$0");
  pluralize.addSingularRule(rule, "$0");
};
pluralize.isPlural = (word) => mapHas(word, irregularSingles, irregularPlurals, pluralRules);
pluralize.isSingular = (word) => mapHas(word, irregularPlurals, irregularSingles, singularRules);
var defaultIrregulars = [
  // Pronouns.
  ["I", "we"],
  ["me", "us"],
  ["he", "they"],
  ["she", "they"],
  ["them", "them"],
  ["myself", "ourselves"],
  ["yourself", "yourselves"],
  ["itself", "themselves"],
  ["herself", "themselves"],
  ["himself", "themselves"],
  ["themself", "themselves"],
  ["is", "are"],
  ["was", "were"],
  ["has", "have"],
  ["this", "these"],
  ["that", "those"],
  ["my", "our"],
  ["its", "their"],
  ["his", "their"],
  ["her", "their"],
  // Words ending in with a consonant and `o`.
  ["echo", "echoes"],
  ["dingo", "dingoes"],
  ["volcano", "volcanoes"],
  ["tornado", "tornadoes"],
  ["torpedo", "torpedoes"],
  // Ends with `us`.
  ["genus", "genera"],
  ["viscus", "viscera"],
  // Ends with `ma`.
  ["stigma", "stigmata"],
  ["stoma", "stomata"],
  ["dogma", "dogmata"],
  ["lemma", "lemmata"],
  ["schema", "schemata"],
  ["anathema", "anathemata"],
  // Other irregular rules.
  ["ox", "oxen"],
  ["axe", "axes"],
  ["die", "dice"],
  ["yes", "yeses"],
  ["foot", "feet"],
  ["eave", "eaves"],
  ["goose", "geese"],
  ["tooth", "teeth"],
  ["quiz", "quizzes"],
  ["human", "humans"],
  ["proof", "proofs"],
  ["carve", "carves"],
  ["valve", "valves"],
  ["looey", "looies"],
  ["thief", "thieves"],
  ["groove", "grooves"],
  ["pickaxe", "pickaxes"],
  ["passerby", "passersby"],
  ["canvas", "canvases"],
];
var defaultPlurals = [
  [/s?$/i, "s"],
  [/[^\u0000-\u007F]$/i, "$0"],
  [/([^aeiou]ese)$/i, "$1"],
  [/(ax|test)is$/i, "$1es"],
  [/(alias|[^aou]us|t[lm]as|gas|ris)$/i, "$1es"],
  [/(e[mn]u)s?$/i, "$1s"],
  [/([^l]ias|[aeiou]las|[ejzr]as|[iu]am)$/i, "$1"],
  [
    /(alumn|syllab|vir|radi|nucle|fung|cact|stimul|termin|bacill|foc|uter|loc|strat)(?:us|i)$/i,
    "$1i",
  ],
  [/(alumn|alg|vertebr)(?:a|ae)$/i, "$1ae"],
  [/(seraph|cherub)(?:im)?$/i, "$1im"],
  [/(her|at|gr)o$/i, "$1oes"],
  [
    /(agend|addend|millenni|dat|extrem|bacteri|desiderat|strat|candelabr|errat|ov|symposi|curricul|automat|quor)(?:a|um)$/i,
    "$1a",
  ],
  [
    /(apheli|hyperbat|periheli|asyndet|noumen|phenomen|criteri|organ|prolegomen|hedr|automat)(?:a|on)$/i,
    "$1a",
  ],
  [/sis$/i, "ses"],
  [/(?:(kni|wi|li)fe|(ar|l|ea|eo|oa|hoo)f)$/i, "$1$2ves"],
  [/([^aeiouy]|qu)y$/i, "$1ies"],
  [/([^ch][ieo][ln])ey$/i, "$1ies"],
  [/(x|ch|ss|sh|zz)$/i, "$1es"],
  [/(matr|cod|mur|sil|vert|ind|append)(?:ix|ex)$/i, "$1ices"],
  [/\b((?:tit)?m|l)(?:ice|ouse)$/i, "$1ice"],
  [/(pe)(?:rson|ople)$/i, "$1ople"],
  [/(child)(?:ren)?$/i, "$1ren"],
  [/eaux$/i, "$0"],
  [/m[ae]n$/i, "men"],
  ["thou", "you"],
];
var defaultSingles = [
  [/s$/i, ""],
  [/(ss)$/i, "$1"],
  [/(wi|kni|(?:after|half|high|low|mid|non|night|[^\w]|^)li)ves$/i, "$1fe"],
  [/(ar|(?:wo|[ae])l|[eo][ao])ves$/i, "$1f"],
  [/ies$/i, "y"],
  [/(dg|ss|ois|lk|ok|wn|mb|th|ch|ec|oal|is|ck|ix|sser|ts|wb)ies$/i, "$1ie"],
  [
    /\b(l|(?:neck|cross|hog|aun)?t|coll|faer|food|gen|goon|group|hipp|junk|vegg|(?:pork)?p|charl|calor|cut)ies$/i,
    "$1ie",
  ],
  [/\b(mon|smil)ies$/i, "$1ey"],
  [/\b((?:tit)?m|l)ice$/i, "$1ouse"],
  [/(seraph|cherub)im$/i, "$1"],
  [
    /(x|ch|ss|sh|zz|tto|go|cho|alias|[^aou]us|t[lm]as|gas|(?:her|at|gr)o|[aeiou]ris)(?:es)?$/i,
    "$1",
  ],
  [/(analy|diagno|parenthe|progno|synop|the|empha|cri|ne)(?:sis|ses)$/i, "$1sis"],
  [/(movie|twelve|abuse|e[mn]u)s$/i, "$1"],
  [/(test)(?:is|es)$/i, "$1is"],
  [
    /(alumn|syllab|vir|radi|nucle|fung|cact|stimul|termin|bacill|foc|uter|loc|strat)(?:us|i)$/i,
    "$1us",
  ],
  [
    /(agend|addend|millenni|dat|extrem|bacteri|desiderat|strat|candelabr|errat|ov|symposi|curricul|quor)a$/i,
    "$1um",
  ],
  [
    /(apheli|hyperbat|periheli|asyndet|noumen|phenomen|criteri|organ|prolegomen|hedr|automat)a$/i,
    "$1on",
  ],
  [/(alumn|alg|vertebr)ae$/i, "$1a"],
  [/(cod|mur|sil|vert|ind)ices$/i, "$1ex"],
  [/(matr|append)ices$/i, "$1ix"],
  [/(pe)(rson|ople)$/i, "$1rson"],
  [/(child)ren$/i, "$1"],
  [/(eau)x?$/i, "$1"],
  [/men$/i, "man"],
];
var defaultUncountables = [
  // Singular words with no plurals.
  "adulthood",
  "advice",
  "agenda",
  "aid",
  "aircraft",
  "alcohol",
  "ammo",
  "analytics",
  "anime",
  "athletics",
  "audio",
  "bison",
  "blood",
  "bream",
  "buffalo",
  "butter",
  "carp",
  "cash",
  "chassis",
  "chess",
  "clothing",
  "cod",
  "commerce",
  "cooperation",
  "corps",
  "debris",
  "diabetes",
  "digestion",
  "elk",
  "energy",
  "equipment",
  "excretion",
  "expertise",
  "firmware",
  "flounder",
  "fun",
  "gallows",
  "garbage",
  "graffiti",
  "hardware",
  "headquarters",
  "health",
  "herpes",
  "highjinks",
  "homework",
  "housework",
  "information",
  "jeans",
  "justice",
  "kudos",
  "labour",
  "literature",
  "machinery",
  "mackerel",
  "mail",
  "media",
  "mews",
  "moose",
  "music",
  "mud",
  "manga",
  "news",
  "only",
  "personnel",
  "pike",
  "plankton",
  "pliers",
  "police",
  "pollution",
  "premises",
  "rain",
  "research",
  "rice",
  "salmon",
  "scissors",
  "series",
  "sewage",
  "shambles",
  "shrimp",
  "software",
  "staff",
  "swine",
  "tennis",
  "traffic",
  "transportation",
  "trout",
  "tuna",
  "wealth",
  "welfare",
  "whiting",
  "wildebeest",
  "wildlife",
  "you",
  /pok[eé]mon$/i,
  // Regexes.
  /[^aeiou]ese$/i,
  // "chinese", "japanese"
  /deer$/i,
  // "deer", "reindeer"
  /fish$/i,
  // "fish", "blowfish", "angelfish"
  /measles$/i,
  /o[iu]s$/i,
  // "carnivorous"
  /pox$/i,
  // "chickpox", "smallpox"
  /sheep$/i,
];
for (const [single, plural] of defaultIrregulars) {
  pluralize.addIrregularRule(single, plural);
}
for (const [search, replacement] of defaultPlurals) {
  pluralize.addPluralRule(search, replacement);
}
for (const [search, replacement] of defaultSingles) {
  pluralize.addSingularRule(search, replacement);
}
for (const search of defaultUncountables) {
  pluralize.addUncountableRule(search);
}

// ../../packages/jazz-tools/src/codegen/relation-analyzer.ts
function capitalize(s) {
  return s.charAt(0).toUpperCase() + s.slice(1);
}
__name(capitalize, "capitalize");
function forwardRefNameFromFK(columnName) {
  const withoutIdSuffix = columnName.replace(/(?:_ids|Ids|_id|Id)$/, "");
  const requiresPluralization = columnName.endsWith("s");
  return requiresPluralization ? pluralize.plural(withoutIdSuffix) : withoutIdSuffix;
}
__name(forwardRefNameFromFK, "forwardRefNameFromFK");
function analyzeRelations(schema2) {
  const relations = /* @__PURE__ */ new Map();
  for (const tableName of Object.keys(schema2)) {
    relations.set(tableName, []);
  }
  for (const [tableName, table] of Object.entries(schema2)) {
    for (const col2 of table.columns) {
      if (col2.references) {
        const isUuidRef =
          col2.column_type.type === "Uuid" ||
          (col2.column_type.type === "Array" && col2.column_type.element.type === "Uuid");
        if (!isUuidRef) {
          throw new Error(
            `Column "${tableName}.${col2.name}" uses references but is not UUID or UUID[]`,
          );
        }
        const isForwardArray =
          col2.column_type.type === "Array" && col2.column_type.element.type === "Uuid";
        const forwardName = forwardRefNameFromFK(col2.name);
        const forwardRelation = {
          name: forwardName,
          type: "forward",
          fromTable: tableName,
          toTable: col2.references,
          fromColumn: col2.name,
          toColumn: "id",
          isArray: isForwardArray,
          nullable: col2.nullable,
        };
        relations.get(tableName).push(forwardRelation);
        if (!relations.has(col2.references)) {
          throw new Error(
            `Table "${tableName}" references unknown table "${col2.references}" via column "${col2.name}"`,
          );
        }
        const reverseName = `${tableName}Via${capitalize(forwardName)}`;
        const reverseRelation = {
          name: reverseName,
          type: "reverse",
          fromTable: col2.references,
          toTable: tableName,
          fromColumn: "id",
          toColumn: col2.name,
          isArray: true,
          nullable: false,
          // Arrays are not nullable, just empty
        };
        relations.get(col2.references).push(reverseRelation);
      }
    }
  }
  return relations;
}
__name(analyzeRelations, "analyzeRelations");

// ../../packages/jazz-tools/src/magic-columns.ts
var RESERVED_MAGIC_COLUMN_PREFIX = "$";
var PERMISSION_INTROSPECTION_COLUMNS = ["$canRead", "$canEdit", "$canDelete"];
var PROVENANCE_MAGIC_COLUMNS = ["$createdBy", "$createdAt", "$updatedBy", "$updatedAt"];
function isPermissionIntrospectionColumn(column) {
  return PERMISSION_INTROSPECTION_COLUMNS.includes(column);
}
__name(isPermissionIntrospectionColumn, "isPermissionIntrospectionColumn");
function isReservedMagicColumnName(column) {
  return column.startsWith(RESERVED_MAGIC_COLUMN_PREFIX);
}
__name(isReservedMagicColumnName, "isReservedMagicColumnName");
function assertUserColumnNameAllowed(column) {
  if (isReservedMagicColumnName(column)) {
    throw new Error(
      `Column name "${column}" is reserved for magic columns. Names starting with "${RESERVED_MAGIC_COLUMN_PREFIX}" are reserved for system fields.`,
    );
  }
}
__name(assertUserColumnNameAllowed, "assertUserColumnNameAllowed");
function magicColumnType(column) {
  if (isPermissionIntrospectionColumn(column)) {
    return { type: "Boolean" };
  }
  if (column === "$createdBy" || column === "$updatedBy") {
    return { type: "Text" };
  }
  if (column === "$createdAt" || column === "$updatedAt") {
    return { type: "Timestamp" };
  }
  return void 0;
}
__name(magicColumnType, "magicColumnType");

// ../../packages/jazz-tools/src/runtime/query-builder-shape.ts
var INTERNAL_REQUIRE_INCLUDES_KEY = "__jazz_requireIncludes";
function isPlainObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
__name(isPlainObject, "isPlainObject");
function normalizeConditions(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter(
    (condition) =>
      isPlainObject(condition) &&
      typeof condition.column === "string" &&
      typeof condition.op === "string",
  );
}
__name(normalizeConditions, "normalizeConditions");
function normalizeOrderBy(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter(
    (entry) =>
      Array.isArray(entry) &&
      entry.length === 2 &&
      typeof entry[0] === "string" &&
      (entry[1] === "asc" || entry[1] === "desc"),
  );
}
__name(normalizeOrderBy, "normalizeOrderBy");
function normalizeSelect(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((column) => typeof column === "string");
}
__name(normalizeSelect, "normalizeSelect");
function normalizeGather(value) {
  const maxDepth =
    isPlainObject(value) && typeof value.max_depth === "number" ? value.max_depth : NaN;
  if (
    !isPlainObject(value) ||
    !Number.isInteger(maxDepth) ||
    maxDepth <= 0 ||
    typeof value.step_table !== "string" ||
    typeof value.step_current_column !== "string"
  ) {
    return void 0;
  }
  return {
    max_depth: maxDepth,
    step_table: value.step_table,
    step_current_column: value.step_current_column,
    step_conditions: normalizeConditions(value.step_conditions),
    step_hops: Array.isArray(value.step_hops)
      ? value.step_hops.filter((hop) => typeof hop === "string")
      : [],
  };
}
__name(normalizeGather, "normalizeGather");
function createEmptyIncludeEntry() {
  return {
    conditions: [],
    includes: {},
    requireIncludes: false,
    select: [],
    orderBy: [],
    hops: [],
  };
}
__name(createEmptyIncludeEntry, "createEmptyIncludeEntry");
function normalizeShorthandIncludeEntries(raw) {
  const nested = { ...raw };
  delete nested[INTERNAL_REQUIRE_INCLUDES_KEY];
  return normalizeIncludeEntries(nested);
}
__name(normalizeShorthandIncludeEntries, "normalizeShorthandIncludeEntries");
function isBuiltQueryShape(value) {
  return "table" in value && "conditions" in value && "includes" in value && "orderBy" in value;
}
__name(isBuiltQueryShape, "isBuiltQueryShape");
function isNormalizedIncludeEntryShape(value) {
  return "conditions" in value && "includes" in value && "select" in value && "orderBy" in value;
}
__name(isNormalizedIncludeEntryShape, "isNormalizedIncludeEntryShape");
function normalizeIncludeEntry(raw) {
  if (raw === true) {
    return createEmptyIncludeEntry();
  }
  if (!isPlainObject(raw)) {
    return null;
  }
  if (isBuiltQueryShape(raw)) {
    const normalized = normalizeBuiltQuery(raw, "");
    return {
      table: normalized.table || void 0,
      conditions: normalized.conditions,
      includes: normalized.includes,
      requireIncludes: normalized.requireIncludes,
      select: normalized.select,
      orderBy: normalized.orderBy,
      limit: normalized.limit,
      offset: normalized.offset,
      hops: normalized.hops,
      gather: normalized.gather,
    };
  }
  if (isNormalizedIncludeEntryShape(raw)) {
    return {
      table: typeof raw.table === "string" ? raw.table : void 0,
      conditions: normalizeConditions(raw.conditions),
      includes: normalizeIncludeEntries(raw.includes),
      requireIncludes: raw[INTERNAL_REQUIRE_INCLUDES_KEY] === true,
      select: normalizeSelect(raw.select),
      orderBy: normalizeOrderBy(raw.orderBy),
      limit: typeof raw.limit === "number" ? raw.limit : void 0,
      offset: typeof raw.offset === "number" ? raw.offset : void 0,
      hops: Array.isArray(raw.hops) ? raw.hops.filter((hop) => typeof hop === "string") : [],
      gather: normalizeGather(raw.gather),
    };
  }
  const entry = createEmptyIncludeEntry();
  entry.requireIncludes = raw[INTERNAL_REQUIRE_INCLUDES_KEY] === true;
  entry.includes = normalizeShorthandIncludeEntries(raw);
  return entry;
}
__name(normalizeIncludeEntry, "normalizeIncludeEntry");
function normalizeIncludeEntries(raw) {
  if (!isPlainObject(raw)) {
    return {};
  }
  const includes = {};
  for (const [relationName, spec] of Object.entries(raw)) {
    if (!spec) {
      continue;
    }
    const normalized = normalizeIncludeEntry(spec);
    if (normalized) {
      includes[relationName] = normalized;
    }
  }
  return includes;
}
__name(normalizeIncludeEntries, "normalizeIncludeEntries");
function normalizeBuiltQuery(raw, fallbackTable) {
  const value = isPlainObject(raw) ? raw : {};
  return {
    table: typeof value.table === "string" && value.table.length > 0 ? value.table : fallbackTable,
    conditions: normalizeConditions(value.conditions),
    includes: normalizeIncludeEntries(value.includes),
    requireIncludes: value[INTERNAL_REQUIRE_INCLUDES_KEY] === true,
    select: normalizeSelect(value.select),
    orderBy: normalizeOrderBy(value.orderBy),
    limit: typeof value.limit === "number" ? value.limit : void 0,
    offset: typeof value.offset === "number" ? value.offset : void 0,
    hops: Array.isArray(value.hops) ? value.hops.filter((hop) => typeof hop === "string") : [],
    gather: normalizeGather(value.gather),
  };
}
__name(normalizeBuiltQuery, "normalizeBuiltQuery");

// ../../packages/jazz-tools/src/runtime/select-projection.ts
var HIDDEN_INCLUDE_COLUMN_PREFIX = "__jazz_include_";
function hiddenIncludeColumnName(relationName) {
  return `${HIDDEN_INCLUDE_COLUMN_PREFIX}${relationName}`;
}
__name(hiddenIncludeColumnName, "hiddenIncludeColumnName");
function isHiddenIncludeColumnName(columnName) {
  return columnName.startsWith(HIDDEN_INCLUDE_COLUMN_PREFIX);
}
__name(isHiddenIncludeColumnName, "isHiddenIncludeColumnName");
function resolveSelectedColumns(tableName, schema2, projection) {
  const table = schema2[tableName];
  if (!table) {
    throw new Error(`Unknown table "${tableName}" in schema`);
  }
  if (!projection || projection.length === 0) {
    return table.columns.map((column) => column.name);
  }
  const schemaColumnNames = new Set(table.columns.map((column) => column.name));
  const selection = {
    explicitColumnsInSchema: /* @__PURE__ */ new Set(),
    explicitColumnsNotInSchema: /* @__PURE__ */ new Set(),
    hasWildcard: false,
  };
  for (const column of projection) {
    if (column === "*") {
      selection.hasWildcard = true;
      continue;
    }
    if (column === "id") {
      continue;
    }
    if (schemaColumnNames.has(column)) {
      selection.explicitColumnsInSchema.add(column);
    } else {
      selection.explicitColumnsNotInSchema.add(column);
    }
  }
  if (!selection.hasWildcard) {
    return [...selection.explicitColumnsInSchema, ...selection.explicitColumnsNotInSchema];
  }
  if (selection.explicitColumnsNotInSchema.size === 0) {
    return [...schemaColumnNames];
  }
  return [...schemaColumnNames, ...selection.explicitColumnsNotInSchema];
}
__name(resolveSelectedColumns, "resolveSelectedColumns");

// ../../packages/jazz-tools/src/runtime/query-adapter.ts
function relColumn(column, scope) {
  return scope ? { scope, column } : { column };
}
__name(relColumn, "relColumn");
function relationColumnsForTable(table, scope, schema2) {
  const tableSchema = schema2[table];
  if (!tableSchema) {
    throw new Error(`Unknown table "${table}" in relation projection.`);
  }
  return [
    {
      alias: "id",
      expr: { Column: relColumn("id", scope) },
    },
    ...tableSchema.columns.map((column) => ({
      alias: column.name,
      expr: { Column: relColumn(column.name, scope) },
    })),
  ];
}
__name(relationColumnsForTable, "relationColumnsForTable");
function getColumnType(schema2, table, column) {
  if (column === "id") return { type: "Uuid" };
  const magicType = magicColumnType(column);
  if (magicType) return magicType;
  const tableSchema = schema2[table];
  if (!tableSchema) return void 0;
  const col2 = tableSchema.columns.find((c) => c.name === column);
  return col2?.column_type;
}
__name(getColumnType, "getColumnType");
function stripQualifier(column) {
  const parts = column.split(".");
  return parts[parts.length - 1] ?? column;
}
__name(stripQualifier, "stripQualifier");
function toTimestampMs(value) {
  if (value instanceof Date) {
    const ts = value.getTime();
    if (!Number.isFinite(ts)) {
      throw new Error("Invalid Date value for timestamp condition");
    }
    return ts;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error("Invalid number value for timestamp condition");
    }
    return value;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
      const fromNumber = Number(trimmed);
      if (Number.isFinite(fromNumber)) {
        return fromNumber;
      }
    }
    const fromIso = Date.parse(trimmed);
    if (Number.isFinite(fromIso)) {
      return fromIso;
    }
  }
  throw new Error("Invalid timestamp condition. Expected Date, ISO string, or finite number.");
}
__name(toTimestampMs, "toTimestampMs");
function toWasmValue(value, columnType) {
  if (value === null || value === void 0) {
    return { type: "Null" };
  }
  if (columnType.type === "Json") {
    return { type: "Text", value: toJsonText(value) };
  }
  if (columnType.type === "Timestamp" && value instanceof Date) {
    return { type: "Timestamp", value: toTimestampMs(value) };
  }
  if (columnType.type === "Bytea") {
    if (value instanceof Uint8Array) {
      return { type: "Bytea", value: [...value] };
    }
    if (Array.isArray(value)) {
      const bytes = value.map((entry) => {
        const n = Number(entry);
        if (!Number.isInteger(n) || n < 0 || n > 255) {
          throw new Error("Bytea values must contain integers in range 0..255");
        }
        return n;
      });
      return { type: "Bytea", value: bytes };
    }
    throw new Error("Bytea values must be Uint8Array or byte arrays");
  }
  if (Array.isArray(value)) {
    if (columnType.type !== "Array") {
      throw new Error("Unexpected array value for scalar column");
    }
    return {
      type: "Array",
      value: value.map((item) => toWasmValue(item, columnType.element)),
    };
  }
  if (typeof value === "boolean") {
    return { type: "Boolean", value };
  }
  if (typeof value === "number") {
    if (columnType?.type === "Timestamp") {
      return { type: "Timestamp", value: toTimestampMs(value) };
    }
    return { type: "Integer", value };
  }
  if (typeof value === "string") {
    if (columnType?.type === "Timestamp") {
      return { type: "Timestamp", value: toTimestampMs(value) };
    }
    if (columnType?.type === "Uuid") {
      return { type: "Uuid", value };
    }
    if (columnType?.type === "Enum" && !columnType.variants.includes(value)) {
      throw new Error(
        `Invalid enum value "${value}". Expected one of: ${columnType.variants.join(", ")}`,
      );
    }
    return { type: "Text", value };
  }
  throw new Error(`Unsupported value type: ${typeof value}`);
}
__name(toWasmValue, "toWasmValue");
function includeRequirementForRelation(relation, requireIncludes) {
  if (!requireIncludes || relation.type !== "forward" || relation.nullable) {
    return void 0;
  }
  return relation.isArray ? "MatchCorrelationCardinality" : "AtLeastOne";
}
__name(includeRequirementForRelation, "includeRequirementForRelation");
function visibleSelectColumns(resolvedSelect, includeProjectionColumns = []) {
  const columns = [...resolvedSelect, ...includeProjectionColumns];
  return columns.length > 0 ? columns : null;
}
__name(visibleSelectColumns, "visibleSelectColumns");
function validateIncludeBuilderSpec(relation, spec, relationName) {
  if (spec.table && spec.table !== relation.toTable) {
    throw new Error(
      `Include builder for relation "${relationName}" must target table "${relation.toTable}", got "${spec.table}".`,
    );
  }
  if (typeof spec.offset === "number" && spec.offset !== 0) {
    throw new Error(`Include builder for relation "${relationName}" does not support offset().`);
  }
  if (spec.hops.length > 0) {
    throw new Error(`Include builder for relation "${relationName}" does not support hopTo(...).`);
  }
  if (spec.gather) {
    throw new Error(`Include builder for relation "${relationName}" does not support gather(...).`);
  }
}
__name(validateIncludeBuilderSpec, "validateIncludeBuilderSpec");
function conditionToArraySubqueryFilter(cond, schema2, table) {
  const column = stripQualifier(cond.column);
  const columnType = getColumnType(schema2, table, column);
  if (!columnType) {
    throw new Error(`Unknown column "${column}" in table "${table}"`);
  }
  if (columnType.type === "Bytea" && ["gt", "gte", "lt", "lte"].includes(cond.op)) {
    throw new Error(`BYTEA column "${column}" only supports eq/ne operators.`);
  }
  if (columnType.type === "Bytea" && cond.op === "contains") {
    throw new Error(`BYTEA column "${column}" does not support contains filters.`);
  }
  if (columnType.type === "Json" && ["gt", "gte", "lt", "lte", "contains"].includes(cond.op)) {
    throw new Error(`JSON column "${column}" only supports eq/ne/in/isNull operators.`);
  }
  const valueTypeForCondition =
    cond.op === "contains" && columnType.type === "Array" ? columnType.element : columnType;
  const literalValue = toWasmValue(cond.value, valueTypeForCondition);
  const isNullValue = cond.value === void 0 ? true : cond.value;
  switch (cond.op) {
    case "eq":
      if (cond.value === null) {
        return { IsNull: { column } };
      }
      return { Eq: { column, value: literalValue } };
    case "ne":
      if (cond.value === null) {
        return { IsNotNull: { column } };
      }
      return { Ne: { column, value: literalValue } };
    case "gt":
      return { Gt: { column, value: literalValue } };
    case "gte":
      return { Ge: { column, value: literalValue } };
    case "lt":
      return { Lt: { column, value: literalValue } };
    case "lte":
      return { Le: { column, value: literalValue } };
    case "isNull":
      if (typeof isNullValue !== "boolean") {
        throw new Error('"isNull" operator requires a boolean value.');
      }
      return isNullValue ? { IsNull: { column } } : { IsNotNull: { column } };
    case "contains":
      return { Contains: { column, value: literalValue } };
    default:
      throw new Error(
        `Include builder for table "${table}" does not support "${cond.op}" filters.`,
      );
  }
}
__name(conditionToArraySubqueryFilter, "conditionToArraySubqueryFilter");
function toArraySubqueries(includes, tableName, relations, schema2, options) {
  const tableRels = relations.get(tableName) || [];
  const subqueries = [];
  const hideCurrentLevelColumnNames = options?.hideCurrentLevelColumnNames === true;
  const requireCurrentLevelIncludes = options?.requireIncludes === true;
  for (const [relName, spec] of Object.entries(includes)) {
    const rel = tableRels.find((r) => r.name === relName);
    if (!rel) {
      throw new Error(`Unknown relation "${relName}" on table "${tableName}"`);
    }
    validateIncludeBuilderSpec(rel, spec, relName);
    const hasExplicitSelect = spec.select.length > 0;
    const resolvedSelectColumns = hasExplicitSelect
      ? resolveSelectedColumns(rel.toTable, schema2, spec.select)
      : [];
    const includeProjectionColumns = hasExplicitSelect
      ? Object.keys(spec.includes).map((relationName) => hiddenIncludeColumnName(relationName))
      : [];
    const filters = spec.conditions.map((condition) =>
      conditionToArraySubqueryFilter(condition, schema2, rel.toTable),
    );
    const orderBy = spec.orderBy.map(([column, direction]) => [
      stripQualifier(column),
      direction === "desc" ? "Descending" : "Ascending",
    ]);
    const nestedArrays = toArraySubqueries(spec.includes, rel.toTable, relations, schema2, {
      hideCurrentLevelColumnNames: hasExplicitSelect,
      requireIncludes: spec.requireIncludes,
    });
    const selectColumns = visibleSelectColumns(resolvedSelectColumns, includeProjectionColumns);
    if (rel.type === "forward") {
      const requirement = includeRequirementForRelation(rel, requireCurrentLevelIncludes);
      subqueries.push({
        column_name: hideCurrentLevelColumnNames ? hiddenIncludeColumnName(relName) : relName,
        table: rel.toTable,
        inner_column: "id",
        outer_column: `${tableName}.${rel.fromColumn}`,
        filters,
        joins: [],
        select_columns: selectColumns,
        order_by: orderBy,
        limit: spec.limit ?? null,
        ...(requirement ? { requirement } : {}),
        nested_arrays: nestedArrays,
      });
    } else {
      subqueries.push({
        column_name: hideCurrentLevelColumnNames ? hiddenIncludeColumnName(relName) : relName,
        table: rel.toTable,
        inner_column: rel.toColumn,
        outer_column: `${tableName}.id`,
        filters,
        joins: [],
        select_columns: selectColumns,
        order_by: orderBy,
        limit: spec.limit ?? null,
        nested_arrays: nestedArrays,
      });
    }
  }
  return subqueries;
}
__name(toArraySubqueries, "toArraySubqueries");
function conditionToRelPredicate(cond, schema2, table, scope) {
  const columnRef = relColumn(stripQualifier(cond.column), scope);
  const column = stripQualifier(cond.column);
  const columnType = getColumnType(schema2, table, column);
  if (!columnType) {
    throw new Error(`Unknown column "${column}" in table "${table}"`);
  }
  const valueTypeForCondition =
    cond.op === "contains" && columnType.type === "Array" ? columnType.element : columnType;
  const rightLiteral =
    isFrontierRowIdToken(cond.value) && cond.op === "eq"
      ? { RowId: "Frontier" }
      : {
          Literal: toWasmValue(cond.value, valueTypeForCondition),
        };
  const isNullValue = cond.value === void 0 ? true : cond.value;
  if (columnType.type === "Bytea" && ["gt", "gte", "lt", "lte"].includes(cond.op)) {
    throw new Error(`BYTEA column "${column}" only supports eq/ne operators.`);
  }
  if (columnType.type === "Bytea" && cond.op === "contains") {
    throw new Error(`BYTEA column "${column}" does not support contains filters.`);
  }
  if (columnType.type === "Json" && ["gt", "gte", "lt", "lte", "contains"].includes(cond.op)) {
    throw new Error(`JSON column "${column}" only supports eq/ne/in/isNull operators.`);
  }
  switch (cond.op) {
    case "eq":
      if (cond.value === null) {
        return { IsNull: { column: columnRef } };
      }
      return { Cmp: { left: columnRef, op: "Eq", right: rightLiteral } };
    case "ne":
      if (cond.value === null) {
        return { IsNotNull: { column: columnRef } };
      }
      return {
        Cmp: {
          left: columnRef,
          op: "Ne",
          right: rightLiteral,
        },
      };
    case "gt":
      return {
        Cmp: {
          left: columnRef,
          op: "Gt",
          right: rightLiteral,
        },
      };
    case "gte":
      return {
        Cmp: {
          left: columnRef,
          op: "Ge",
          right: rightLiteral,
        },
      };
    case "lt":
      return {
        Cmp: {
          left: columnRef,
          op: "Lt",
          right: rightLiteral,
        },
      };
    case "lte":
      return {
        Cmp: {
          left: columnRef,
          op: "Le",
          right: rightLiteral,
        },
      };
    case "isNull":
      if (typeof isNullValue !== "boolean") {
        throw new Error('"isNull" operator requires a boolean value.');
      }
      return isNullValue ? { IsNull: { column: columnRef } } : { IsNotNull: { column: columnRef } };
    case "contains":
      return { Contains: { left: columnRef, right: rightLiteral } };
    case "in":
      if (!Array.isArray(cond.value)) {
        throw new Error('"in" operator requires an array value');
      }
      return {
        In: {
          left: columnRef,
          values: cond.value.map((value) => ({
            Literal: toWasmValue(value, columnType),
          })),
        },
      };
    default:
      throw new Error(`Unknown operator: ${cond.op}`);
  }
}
__name(conditionToRelPredicate, "conditionToRelPredicate");
function isFrontierRowIdToken(value) {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const marker = value;
  return marker.__jazz_ir_frontier_row_id === true;
}
__name(isFrontierRowIdToken, "isFrontierRowIdToken");
function conditionsToRelPredicate(conditions, schema2, table, scope) {
  if (conditions.length === 0) {
    return "True";
  }
  if (conditions.length === 1) {
    return conditionToRelPredicate(conditions[0], schema2, table, scope);
  }
  return {
    And: conditions.map((condition) => conditionToRelPredicate(condition, schema2, table, scope)),
  };
}
__name(conditionsToRelPredicate, "conditionsToRelPredicate");
function applyFilter(input, predicate) {
  if (predicate === "True") {
    return input;
  }
  return { Filter: { input, predicate } };
}
__name(applyFilter, "applyFilter");
function lowerHopsToRelExpr(input, seedTable, hops, relations, schema2) {
  if (hops.length === 0) {
    return input;
  }
  let currentExpr = input;
  let currentTable = seedTable;
  let currentScope = seedTable;
  for (let i = 0; i < hops.length; i += 1) {
    const hopName = hops[i];
    const tableRelations = relations.get(currentTable) ?? [];
    const relation = tableRelations.find((candidate) => candidate.name === hopName);
    if (!relation) {
      throw new Error(`Unknown relation "${hopName}" on table "${currentTable}"`);
    }
    const hopAlias = `__hop_${i}`;
    const joinOn =
      relation.type === "forward"
        ? {
            left: relColumn(relation.fromColumn, currentScope),
            right: relColumn("id", hopAlias),
          }
        : {
            left: relColumn("id", currentScope),
            right: relColumn(relation.toColumn, hopAlias),
          };
    currentExpr = {
      Join: {
        left: currentExpr,
        right: { TableScan: { table: relation.toTable } },
        on: [joinOn],
        join_kind: "Inner",
      },
    };
    currentTable = relation.toTable;
    currentScope = hopAlias;
  }
  return {
    Project: {
      input: currentExpr,
      columns: relationColumnsForTable(currentTable, currentScope, schema2),
    },
  };
}
__name(lowerHopsToRelExpr, "lowerHopsToRelExpr");
function gatherToRelExpr(gather, seedTable, seedExpr, relations, schema2) {
  if (!schema2[gather.step_table]) {
    throw new Error(`Unknown gather step table "${gather.step_table}"`);
  }
  if (!Number.isInteger(gather.max_depth) || gather.max_depth <= 0) {
    throw new Error("gather(...) max_depth must be a positive integer.");
  }
  const stepHops = Array.isArray(gather.step_hops)
    ? gather.step_hops.filter((hop) => typeof hop === "string")
    : [];
  if (stepHops.length !== 1) {
    throw new Error("gather(...) currently requires exactly one hopTo(...) step.");
  }
  const stepRelations = relations.get(gather.step_table) ?? [];
  const hopName = stepHops[0];
  const hopRelation = stepRelations.find((rel) => rel.name === hopName);
  if (!hopRelation) {
    throw new Error(`Unknown relation "${hopName}" on table "${gather.step_table}"`);
  }
  if (hopRelation.type !== "forward") {
    throw new Error("gather(...) currently only supports forward hopTo(...) relations.");
  }
  if (hopRelation.toTable !== seedTable) {
    throw new Error(
      `gather(...) step must hop back to "${seedTable}" rows, got "${hopRelation.toTable}".`,
    );
  }
  const stepBase = { TableScan: { table: gather.step_table } };
  const stepConditions = Array.isArray(gather.step_conditions) ? gather.step_conditions : [];
  const stepScope = gather.step_table;
  const stepPredicateConditions = [
    ...stepConditions,
    {
      column: stripQualifier(gather.step_current_column),
      op: "eq",
      value: { __jazz_ir_frontier_row_id: true },
    },
  ];
  const stepPredicate = conditionsToRelPredicate(
    stepPredicateConditions,
    schema2,
    gather.step_table,
    stepScope,
  );
  const stepFiltered = applyFilter(stepBase, stepPredicate);
  const recursiveHopAlias = "__recursive_hop_0";
  const stepJoined = {
    Join: {
      left: stepFiltered,
      right: { TableScan: { table: hopRelation.toTable } },
      on: [
        {
          left: relColumn(hopRelation.fromColumn, gather.step_table),
          right: relColumn("id", recursiveHopAlias),
        },
      ],
      join_kind: "Inner",
    },
  };
  const stepProjected = {
    Project: {
      input: stepJoined,
      columns: relationColumnsForTable(seedTable, recursiveHopAlias, schema2),
    },
  };
  return {
    Gather: {
      seed: seedExpr,
      step: stepProjected,
      frontier_key: { RowId: "Current" },
      max_depth: gather.max_depth,
      dedupe_key: [{ RowId: "Current" }],
    },
  };
}
__name(gatherToRelExpr, "gatherToRelExpr");
function translateBuilderToRelationIr(builderJson, schema2) {
  const builder = normalizeBuiltQuery(JSON.parse(builderJson), "");
  const relations = analyzeRelations(schema2);
  const hops = builder.hops;
  if (builder.gather && Object.keys(builder.includes).length > 0) {
    throw new Error("gather(...) does not yet support include(...).");
  }
  if (hops.length > 0 && Object.keys(builder.includes).length > 0) {
    throw new Error("hopTo(...) does not yet support include(...).");
  }
  let relation = { TableScan: { table: builder.table } };
  relation = applyFilter(
    relation,
    conditionsToRelPredicate(builder.conditions, schema2, builder.table, builder.table),
  );
  if (builder.gather) {
    relation = gatherToRelExpr(builder.gather, builder.table, relation, relations, schema2);
  }
  relation = lowerHopsToRelExpr(relation, builder.table, hops, relations, schema2);
  if (Array.isArray(builder.orderBy) && builder.orderBy.length > 0) {
    for (const [column] of builder.orderBy) {
      const columnType = getColumnType(schema2, builder.table, stripQualifier(column));
      if (columnType?.type === "Bytea") {
        throw new Error(`BYTEA column "${column}" cannot be used in orderBy().`);
      }
      if (columnType?.type === "Json") {
        throw new Error(`JSON column "${column}" cannot be used in orderBy().`);
      }
    }
    relation = {
      OrderBy: {
        input: relation,
        terms: builder.orderBy.map(([column, direction]) => ({
          column: relColumn(column),
          direction: direction === "desc" ? "Desc" : "Asc",
        })),
      },
    };
  }
  if (typeof builder.offset === "number" && builder.offset > 0) {
    relation = {
      Offset: {
        input: relation,
        offset: builder.offset,
      },
    };
  }
  if (typeof builder.limit === "number") {
    relation = {
      Limit: {
        input: relation,
        limit: builder.limit,
      },
    };
  }
  return relation;
}
__name(translateBuilderToRelationIr, "translateBuilderToRelationIr");
function translateQuery(builderJson, schema2) {
  const builder = normalizeBuiltQuery(JSON.parse(builderJson), "");
  const relations = analyzeRelations(schema2);
  const relation = translateBuilderToRelationIr(builderJson, schema2);
  const hasExplicitSelect = builder.select.length > 0;
  const selectColumns = hasExplicitSelect
    ? resolveSelectedColumns(builder.table, schema2, builder.select)
    : [];
  const includeProjectionColumns = hasExplicitSelect
    ? Object.keys(builder.includes).map((relationName) => hiddenIncludeColumnName(relationName))
    : [];
  const projectedColumns = visibleSelectColumns(selectColumns, includeProjectionColumns);
  const query = {
    table: builder.table,
    array_subqueries: toArraySubqueries(builder.includes, builder.table, relations, schema2, {
      hideCurrentLevelColumnNames: hasExplicitSelect,
      requireIncludes: builder.requireIncludes,
    }),
    relation_ir: relation,
    ...(projectedColumns ? { select_columns: projectedColumns } : {}),
  };
  return JSON.stringify(query);
}
__name(translateQuery, "translateQuery");

// ../../packages/jazz-tools/src/runtime/runtime-config.ts
function isHttpModuleUrl(moduleUrl) {
  const protocol = new URL(moduleUrl).protocol;
  return protocol === "http:" || protocol === "https:";
}
__name(isHttpModuleUrl, "isHttpModuleUrl");
function resolveBrowserAssetBase(locationHref) {
  return new URL("/", locationHref).href;
}
__name(resolveBrowserAssetBase, "resolveBrowserAssetBase");
function resolveConfiguredUrl(url, locationHref) {
  if (locationHref) {
    return new URL(url, locationHref).href;
  }
  return new URL(url).href;
}
__name(resolveConfiguredUrl, "resolveConfiguredUrl");
function resolveConfiguredBaseUrl(baseUrl, locationHref) {
  if (!locationHref) {
    return null;
  }
  return new URL(baseUrl, locationHref).href;
}
__name(resolveConfiguredBaseUrl, "resolveConfiguredBaseUrl");
function resolveRuntimeConfigSyncInitInput(runtime) {
  if (runtime?.wasmModule) {
    return { module: runtime.wasmModule };
  }
  if (runtime?.wasmSource) {
    return { module: runtime.wasmSource };
  }
  return null;
}
__name(resolveRuntimeConfigSyncInitInput, "resolveRuntimeConfigSyncInitInput");
function resolveRuntimeConfigWasmUrl(runtimeModuleUrl, locationHref, runtime) {
  if (runtime?.wasmUrl) {
    return resolveConfiguredUrl(runtime.wasmUrl, locationHref);
  }
  if (runtime?.baseUrl) {
    const baseUrl = resolveConfiguredBaseUrl(runtime.baseUrl, locationHref);
    if (baseUrl) {
      return new URL("jazz_wasm_bg.wasm", baseUrl).href;
    }
  }
  if (!locationHref || isHttpModuleUrl(runtimeModuleUrl)) {
    return null;
  }
  return new URL("jazz_wasm_bg.wasm", resolveBrowserAssetBase(locationHref)).href;
}
__name(resolveRuntimeConfigWasmUrl, "resolveRuntimeConfigWasmUrl");
function resolveRuntimeConfigWorkerUrl(runtimeModuleUrl, locationHref, runtime) {
  if (runtime?.workerUrl) {
    return resolveConfiguredUrl(runtime.workerUrl, locationHref);
  }
  if (runtime?.baseUrl) {
    const baseUrl = resolveConfiguredBaseUrl(runtime.baseUrl, locationHref);
    if (baseUrl) {
      return new URL("worker/jazz-worker.js", baseUrl).href;
    }
  }
  if (!locationHref || isHttpModuleUrl(runtimeModuleUrl)) {
    return new URL("../worker/jazz-worker.js", runtimeModuleUrl).href;
  }
  return new URL("worker/jazz-worker.js", resolveBrowserAssetBase(locationHref)).href;
}
__name(resolveRuntimeConfigWorkerUrl, "resolveRuntimeConfigWorkerUrl");
function appendWorkerRuntimeWasmUrl(workerUrl, wasmUrl) {
  if (!wasmUrl) {
    return workerUrl;
  }
  const url = new URL(workerUrl);
  url.searchParams.set("jazz-wasm-url", wasmUrl);
  return url.href;
}
__name(appendWorkerRuntimeWasmUrl, "appendWorkerRuntimeWasmUrl");

// ../../packages/jazz-tools/src/runtime/client.ts
function resolveDefaultDurabilityTier(context) {
  if (context.defaultDurabilityTier) {
    return context.defaultDurabilityTier;
  }
  if (isBrowserRuntime()) {
    return "worker";
  }
  return context.serverUrl ? "edge" : "worker";
}
__name(resolveDefaultDurabilityTier, "resolveDefaultDurabilityTier");
function resolveEffectiveQueryExecutionOptions(context, options) {
  return {
    tier: options?.tier ?? resolveDefaultDurabilityTier(context),
    localUpdates: options?.localUpdates ?? "immediate",
    propagation: options?.propagation ?? "full",
    visibility: options?.visibility ?? "public",
  };
}
__name(resolveEffectiveQueryExecutionOptions, "resolveEffectiveQueryExecutionOptions");
function resolveQueryJson(query) {
  if (typeof query === "string") {
    return query;
  }
  const builtQuery = query._build();
  const schema2 = query._schema;
  if (!schema2 || typeof schema2 !== "object" || Array.isArray(schema2)) {
    return builtQuery;
  }
  try {
    const parsed = JSON.parse(builtQuery);
    if (parsed && typeof parsed === "object" && "relation_ir" in parsed) {
      return builtQuery;
    }
  } catch {
    return builtQuery;
  }
  return translateQuery(builtQuery, schema2);
}
__name(resolveQueryJson, "resolveQueryJson");
function resolveRelationIrOutputTable(node) {
  if (!node || typeof node !== "object") {
    return null;
  }
  const relation = node;
  if ("TableScan" in relation) {
    const tableScan = relation.TableScan;
    return typeof tableScan?.table === "string" ? tableScan.table : null;
  }
  if ("Filter" in relation) {
    return resolveRelationIrOutputTable(relation.Filter?.input);
  }
  if ("OrderBy" in relation) {
    return resolveRelationIrOutputTable(relation.OrderBy?.input);
  }
  if ("Limit" in relation) {
    return resolveRelationIrOutputTable(relation.Limit?.input);
  }
  if ("Offset" in relation) {
    return resolveRelationIrOutputTable(relation.Offset?.input);
  }
  if ("Project" in relation) {
    return resolveRelationIrOutputTable(relation.Project?.input);
  }
  if ("Gather" in relation) {
    const gather = relation.Gather;
    return resolveRelationIrOutputTable(gather?.seed);
  }
  return null;
}
__name(resolveRelationIrOutputTable, "resolveRelationIrOutputTable");
function parseArraySubqueryPlans(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  const plans = [];
  for (const entry of value) {
    if (typeof entry !== "object" || entry === null) {
      continue;
    }
    const plan = entry;
    if (typeof plan.table !== "string") {
      continue;
    }
    plans.push({
      table: plan.table,
      selectColumns: Array.isArray(plan.select_columns)
        ? plan.select_columns.filter((column) => typeof column === "string")
        : [],
      nested: parseArraySubqueryPlans(plan.nested_arrays),
    });
  }
  return plans;
}
__name(parseArraySubqueryPlans, "parseArraySubqueryPlans");
function resolveQueryAlignmentPlan(queryJson) {
  try {
    const parsed = JSON.parse(queryJson);
    return {
      outputTable:
        typeof parsed.table === "string"
          ? parsed.table
          : resolveRelationIrOutputTable(parsed.relation_ir),
      arraySubqueries: parseArraySubqueryPlans(parsed.array_subqueries),
      selectColumns: Array.isArray(parsed.select_columns)
        ? parsed.select_columns.filter((column) => typeof column === "string")
        : [],
    };
  } catch {
    return {
      outputTable: null,
      arraySubqueries: [],
      selectColumns: [],
    };
  }
}
__name(resolveQueryAlignmentPlan, "resolveQueryAlignmentPlan");
function resolveNodeTier(tier) {
  if (!tier) return void 0;
  if (Array.isArray(tier)) {
    return tier[0];
  }
  return tier;
}
__name(resolveNodeTier, "resolveNodeTier");
function isBrowserRuntime() {
  return typeof window !== "undefined" && typeof document !== "undefined";
}
__name(isBrowserRuntime, "isBrowserRuntime");
function getScheduler() {
  if ("scheduler" in globalThis) {
    return (task) => {
      void globalThis.scheduler.postTask(task, { priority: "user-visible" });
    };
  }
  return (task) => queueMicrotask(task);
}
__name(getScheduler, "getScheduler");
function encodeQueryExecutionOptions(options) {
  const payload = {};
  if ((options.propagation ?? "full") !== "full") {
    payload.propagation = options.propagation;
  }
  if ((options.localUpdates ?? "immediate") !== "immediate") {
    payload.local_updates = options.localUpdates;
  }
  if (!payload.propagation && !payload.local_updates) {
    return void 0;
  }
  return JSON.stringify(payload);
}
__name(encodeQueryExecutionOptions, "encodeQueryExecutionOptions");
function readHeader(request, name) {
  const lower = name.toLowerCase();
  const fromMethod = request.header?.(name) ?? request.header?.(lower);
  if (typeof fromMethod === "string") {
    return fromMethod;
  }
  const headers = request.headers;
  if (!headers) {
    return void 0;
  }
  if (typeof Headers !== "undefined" && headers instanceof Headers) {
    return headers.get(name) ?? headers.get(lower) ?? void 0;
  }
  const record = headers;
  const raw = record[name] ?? record[lower];
  if (Array.isArray(raw)) {
    return raw[0];
  }
  return raw;
}
__name(readHeader, "readHeader");
function normalizeSubscriptionCallbackArgs(args) {
  if (args.length === 1) {
    return args[0];
  }
  if (args.length === 2 && args[0] == null) {
    return args[1];
  }
  console.error("Invalid subscription callback arguments", args);
  return void 0;
}
__name(normalizeSubscriptionCallbackArgs, "normalizeSubscriptionCallbackArgs");
function decodeBase64Url(value) {
  const base64 = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = base64 + "=".repeat((4 - (base64.length % 4)) % 4);
  if (typeof atob === "function") {
    return atob(padded);
  }
  if (typeof Buffer !== "undefined") {
    return Buffer.from(padded, "base64").toString("utf8");
  }
  throw new Error("No base64 decoder available in this runtime");
}
__name(decodeBase64Url, "decodeBase64Url");
function sessionFromRequest(request) {
  const authHeader = readHeader(request, "authorization");
  if (!authHeader?.startsWith("Bearer ")) {
    throw new Error("Missing or invalid Authorization header");
  }
  const token = authHeader.slice("Bearer ".length).trim();
  const parts = token.split(".");
  if (parts.length < 2) {
    throw new Error("Invalid JWT format");
  }
  const payloadPart = parts[1];
  if (payloadPart === void 0) {
    throw new Error("Invalid JWT format");
  }
  let payload;
  try {
    payload = JSON.parse(decodeBase64Url(payloadPart));
  } catch {
    throw new Error("Invalid JWT payload");
  }
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new Error("Invalid JWT payload");
  }
  const typedPayload = payload;
  if (typeof typedPayload.sub !== "string" || typedPayload.sub.length === 0) {
    throw new Error("JWT payload missing sub");
  }
  const claims =
    typedPayload.claims &&
    typeof typedPayload.claims === "object" &&
    !Array.isArray(typedPayload.claims)
      ? typedPayload.claims
      : {};
  return { user_id: typedPayload.sub, claims };
}
__name(sessionFromRequest, "sessionFromRequest");
var SessionClient = class {
  static {
    __name(this, "SessionClient");
  }
  client;
  session;
  constructor(client, session) {
    this.client = client;
    this.session = session;
  }
  /**
   * Create a new row as this session's user.
   */
  async create(table, values) {
    if (!this.client.getServerUrl()) {
      throw new Error("No server connection");
    }
    const response = await this.client.sendRequest(
      this.client.getRequestUrl("/sync/object"),
      "POST",
      {
        table,
        values,
        schema_context: this.client.getSchemaContext(),
      },
      this.session,
    );
    if (!response.ok) {
      throw new Error(`Create failed: ${response.statusText}`);
    }
    const result = await response.json();
    return result.object_id;
  }
  /**
   * Update a row as this session's user.
   */
  async update(objectId, updates) {
    if (!this.client.getServerUrl()) {
      throw new Error("No server connection");
    }
    const updateArray = Object.entries(updates);
    const response = await this.client.sendRequest(
      this.client.getRequestUrl("/sync/object"),
      "PUT",
      {
        object_id: objectId,
        updates: updateArray,
        schema_context: this.client.getSchemaContext(),
      },
      this.session,
    );
    if (!response.ok) {
      throw new Error(`Update failed: ${response.statusText}`);
    }
  }
  /**
   * Delete a row as this session's user.
   */
  async delete(objectId) {
    if (!this.client.getServerUrl()) {
      throw new Error("No server connection");
    }
    const response = await this.client.sendRequest(
      this.client.getRequestUrl("/sync/object/delete"),
      "POST",
      {
        object_id: objectId,
        schema_context: this.client.getSchemaContext(),
      },
      this.session,
    );
    if (!response.ok) {
      throw new Error(`Delete failed: ${response.statusText}`);
    }
  }
  /**
   * Query as this session's user.
   */
  async query(query, options) {
    return this.client.queryInternal(query, this.session, options);
  }
  /**
   * Subscribe to a query as this session's user.
   */
  subscribe(query, callback, options) {
    return this.client.subscribeInternal(query, callback, this.session, options);
  }
};
var JazzClient = class _JazzClient {
  static {
    __name(this, "JazzClient");
  }
  runtime;
  streamController;
  serverClientId = generateClientId();
  scheduler;
  context;
  resolvedSession;
  defaultDurabilityTier;
  useBackendSyncAuth = false;
  constructor(runtime, context, defaultDurabilityTier) {
    this.runtime = runtime;
    this.scheduler = getScheduler();
    this.context = context;
    this.defaultDurabilityTier = defaultDurabilityTier;
    this.resolvedSession = resolveClientSessionSync({
      appId: context.appId,
      jwtToken: context.jwtToken,
      localAuthMode: context.localAuthMode,
      localAuthToken: context.localAuthToken,
    });
    this.streamController = createRuntimeSyncStreamController({
      getRuntime: /* @__PURE__ */ __name(() => this.runtime, "getRuntime"),
      getAuth: /* @__PURE__ */ __name(() => this.getSyncAuth(), "getAuth"),
      getClientId: /* @__PURE__ */ __name(() => this.serverClientId, "getClientId"),
      setClientId: /* @__PURE__ */ __name((clientId) => {
        this.serverClientId = clientId;
      }, "setClientId"),
    });
  }
  /**
   * Connect to Jazz with the given context.
   *
   * @param context Application context with driver and schema
   * @returns Connected JazzClient instance
   */
  static async connect(context) {
    const resolvedContext = resolveLocalAuthDefaults(context);
    const wasmModule2 = await loadWasmModule(resolvedContext.runtime);
    const schemaJson = serializeRuntimeSchema(resolvedContext.schema);
    const runtime = new wasmModule2.WasmRuntime(
      schemaJson,
      resolvedContext.appId,
      resolvedContext.env ?? "dev",
      resolvedContext.userBranch ?? "main",
      resolveNodeTier(resolvedContext.tier),
    );
    const client = new _JazzClient(
      runtime,
      resolvedContext,
      resolveDefaultDurabilityTier(resolvedContext),
    );
    if (resolvedContext.serverUrl) {
      client.setupSync(resolvedContext.serverUrl, resolvedContext.serverPathPrefix);
    }
    return client;
  }
  /**
   * Create client synchronously with a pre-loaded WASM module.
   *
   * Use this after loading WASM via `loadWasmModule()` to avoid
   * async client creation. This enables sync mutations in the Db class.
   *
   * @param wasmModule Pre-loaded WASM module from loadWasmModule()
   * @param context Application context with driver and schema
   * @returns Connected JazzClient instance (created synchronously)
   */
  static connectSync(wasmModule2, context, runtimeOptions) {
    const resolvedContext = resolveLocalAuthDefaults(context);
    const schemaJson = serializeRuntimeSchema(resolvedContext.schema);
    const runtime = new wasmModule2.WasmRuntime(
      schemaJson,
      resolvedContext.appId,
      resolvedContext.env ?? "dev",
      resolvedContext.userBranch ?? "main",
      resolveNodeTier(resolvedContext.tier),
      runtimeOptions?.useBinaryEncoding ?? false,
    );
    const client = new _JazzClient(
      runtime,
      resolvedContext,
      resolveDefaultDurabilityTier(resolvedContext),
    );
    if (resolvedContext.serverUrl) {
      client.setupSync(resolvedContext.serverUrl, resolvedContext.serverPathPrefix);
    }
    return client;
  }
  /**
   * Create client from a pre-constructed runtime (e.g., NapiRuntime).
   *
   * This allows server-side apps to use the native NAPI backend directly
   * without WASM loading.
   *
   * @param runtime A runtime implementing the Runtime interface
   * @param context Application context
   * @returns Connected JazzClient instance
   */
  static connectWithRuntime(runtime, context) {
    const client = new _JazzClient(runtime, context, resolveDefaultDurabilityTier(context));
    if (context.serverUrl) {
      client.setupSync(context.serverUrl, context.serverPathPrefix);
    }
    return client;
  }
  /**
   * Create a session-scoped client for backend operations.
   *
   * This allows backend applications to perform operations as a specific user.
   * Requires `backendSecret` to be configured in the `AppContext`.
   *
   * @param session Session to impersonate
   * @returns SessionClient for performing operations as the given user
   * @throws Error if backendSecret is not configured
   *
   * @example
   * ```typescript
   * const userSession = { user_id: "user-123", claims: {} };
   * const userClient = client.forSession(userSession);
   * const id = await userClient.create("todos", {
   *   title: { type: "Text", value: "Buy milk" },
   *   done: { type: "Boolean", value: false },
   * });
   * ```
   */
  forSession(session) {
    if (!this.context.backendSecret) {
      throw new Error("backendSecret required for session impersonation");
    }
    if (!this.context.serverUrl) {
      throw new Error("serverUrl required for session impersonation");
    }
    return new SessionClient(this, session);
  }
  /**
   * Create a session-scoped client from an authenticated HTTP request.
   *
   * Extracts `Authorization: Bearer <jwt>` and maps payload fields:
   * - `sub` -> `session.user_id`
   * - `claims` -> `session.claims` (defaults to `{}`)
   *
   * This helper only extracts payload fields and does not validate JWT signatures.
   * JWT verification should happen in your auth middleware before request handling.
   */
  forRequest(request) {
    return this.forSession(sessionFromRequest(request));
  }
  /**
   * Enable backend-scoped sync auth for this client.
   *
   * In backend mode, sync/event transport uses `X-Jazz-Backend-Secret` instead
   * of end-user auth headers and intentionally does not send admin headers.
   */
  asBackend() {
    if (!this.context.backendSecret) {
      throw new Error("backendSecret required for backend mode");
    }
    if (!this.context.serverUrl) {
      throw new Error("serverUrl required for backend mode");
    }
    this.useBackendSyncAuth = true;
    this.streamController.updateAuth();
    return this;
  }
  getSyncAuth() {
    if (this.useBackendSyncAuth) {
      return {
        backendSecret: this.context.backendSecret,
      };
    }
    return {
      jwtToken: this.context.jwtToken,
      localAuthMode: this.context.localAuthMode,
      localAuthToken: this.context.localAuthToken,
      adminSecret: this.context.adminSecret,
    };
  }
  normalizeQueryExecutionOptions(options) {
    return resolveEffectiveQueryExecutionOptions(
      { ...this.context, defaultDurabilityTier: this.defaultDurabilityTier },
      options,
    );
  }
  resolveWriteTier(options) {
    return options?.tier ?? this.defaultDurabilityTier;
  }
  encodeWriteContext(session, attribution) {
    if (!session && attribution === void 0) {
      return void 0;
    }
    if (attribution === void 0 && session) {
      return JSON.stringify(session);
    }
    const payload = {};
    if (session) {
      payload.session = session;
    }
    if (attribution !== void 0) {
      payload.attribution = attribution;
    }
    return JSON.stringify(payload);
  }
  resolveWriteSession(session, attribution) {
    if (session) {
      return session;
    }
    if (attribution !== void 0) {
      return void 0;
    }
    return this.resolvedSession ?? void 0;
  }
  requireSessionWriteMethod(method) {
    const runtimeMethod = this.runtime[method];
    if (!runtimeMethod) {
      throw new Error(`${String(method)} is not supported by this runtime`);
    }
    return runtimeMethod.bind(this.runtime);
  }
  alignRowValuesToDeclaredSchema(
    table,
    values,
    runtimeSchema = this.getSchema(),
    arraySubqueries = [],
    selectColumns = [],
  ) {
    const declaredTable = this.context.schema[table];
    const runtimeTable = runtimeSchema[table];
    if (!declaredTable || !runtimeTable) {
      return values;
    }
    const projectedVisibleColumnCount =
      selectColumns.length > 0
        ? resolveSelectedColumns(table, this.context.schema, selectColumns).filter(
            (columnName) => !isHiddenIncludeColumnName(columnName),
          ).length
        : 0;
    if (projectedVisibleColumnCount > 0) {
      if (values.length < projectedVisibleColumnCount) {
        return values;
      }
      const projectedValues = values.slice(0, projectedVisibleColumnCount);
      const trailingValues2 = values.slice(projectedVisibleColumnCount);
      if (arraySubqueries.length === 0) {
        return projectedValues.concat(trailingValues2);
      }
      const alignedTrailingValues2 = trailingValues2.map((value, index) => {
        const plan = arraySubqueries[index];
        if (!plan) {
          return value;
        }
        return this.alignIncludedValueToDeclaredSchema(value, plan, runtimeSchema);
      });
      return projectedValues.concat(alignedTrailingValues2);
    }
    if (values.length < runtimeTable.columns.length) {
      return values;
    }
    const valuesByColumn = /* @__PURE__ */ new Map();
    for (let index = 0; index < runtimeTable.columns.length; index += 1) {
      const column = runtimeTable.columns[index];
      if (!column) {
        return values;
      }
      const value = values[index];
      if (value === void 0) {
        return values;
      }
      valuesByColumn.set(column.name, value);
    }
    const reorderedValues = [];
    for (const column of declaredTable.columns) {
      const value = valuesByColumn.get(column.name);
      if (value === void 0) {
        return values;
      }
      reorderedValues.push(value);
    }
    const trailingValues = values.slice(runtimeTable.columns.length);
    if (arraySubqueries.length === 0) {
      return reorderedValues.concat(trailingValues);
    }
    const alignedTrailingValues = trailingValues.map((value, index) => {
      const plan = arraySubqueries[index];
      if (!plan) {
        return value;
      }
      return this.alignIncludedValueToDeclaredSchema(value, plan, runtimeSchema);
    });
    return reorderedValues.concat(alignedTrailingValues);
  }
  alignIncludedValueToDeclaredSchema(value, plan, runtimeSchema = this.getSchema()) {
    if (value.type !== "Array") {
      return value;
    }
    return {
      ...value,
      value: value.value.map((entry) => {
        if (entry.type !== "Row") {
          return entry;
        }
        return {
          ...entry,
          value: {
            ...entry.value,
            values: this.alignRowValuesToDeclaredSchema(
              plan.table,
              entry.value.values,
              runtimeSchema,
              plan.nested,
              plan.selectColumns,
            ),
          },
        };
      }),
    };
  }
  alignQueryRowsToDeclaredSchema(queryJson, rows, runtimeSchema = this.getSchema()) {
    const { outputTable, arraySubqueries, selectColumns } = resolveQueryAlignmentPlan(queryJson);
    if (!outputTable) {
      return rows;
    }
    return rows.map((row) => ({
      ...row,
      values: this.alignRowValuesToDeclaredSchema(
        outputTable,
        row.values,
        runtimeSchema,
        arraySubqueries,
        selectColumns,
      ),
    }));
  }
  alignSubscriptionDeltaToDeclaredSchema(queryJson, delta, runtimeSchema = this.getSchema()) {
    const { outputTable, arraySubqueries, selectColumns } = resolveQueryAlignmentPlan(queryJson);
    if (!outputTable || !Array.isArray(delta)) {
      return delta;
    }
    return delta.map((change) => {
      if ((change.kind === 0 || change.kind === 2) && change.row) {
        return {
          ...change,
          row: {
            ...change.row,
            values: this.alignRowValuesToDeclaredSchema(
              outputTable,
              change.row.values,
              runtimeSchema,
              arraySubqueries,
              selectColumns,
            ),
          },
        };
      }
      return change;
    });
  }
  /**
   * Insert a new row into a table without waiting for durability.
   */
  create(table, values) {
    return this.createInternal(table, values);
  }
  /**
   * Insert a new row into a table with an optional session for policy checks.
   * @internal
   */
  createInternal(table, values, session, attribution) {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const row =
      effectiveSession || attribution !== void 0
        ? this.requireSessionWriteMethod("insertWithSession")(
            table,
            values,
            this.encodeWriteContext(effectiveSession, attribution),
          )
        : this.runtime.insert(table, values);
    return {
      ...row,
      values: this.alignRowValuesToDeclaredSchema(table, row.values, this.getSchema()),
    };
  }
  /**
   * Insert a new row into a table and wait for durability at the requested tier.
   */
  async createDurable(table, values, options) {
    return this.createDurableInternal(table, values, void 0, void 0, options);
  }
  /**
   * Insert a new row into a table and wait for durability, optionally scoped to a session.
   * @internal
   */
  async createDurableInternal(table, values, session, attribution, options) {
    const tier = this.resolveWriteTier(options);
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const row =
      effectiveSession || attribution !== void 0
        ? await this.requireSessionWriteMethod("insertDurableWithSession")(
            table,
            values,
            this.encodeWriteContext(effectiveSession, attribution),
            tier,
          )
        : await this.runtime.insertDurable(table, values, tier);
    return {
      ...row,
      values: this.alignRowValuesToDeclaredSchema(table, row.values, this.getSchema()),
    };
  }
  /**
   * Execute a query and return all matching rows.
   *
   * @param query Query builder or JSON-encoded query specification
   * @param options Optional read durability options
   * @returns Array of matching rows
   */
  async query(query, options) {
    return this.queryInternal(query, this.resolvedSession ?? void 0, options);
  }
  /**
   * Internal query with optional session and read durability options.
   * @internal
   */
  async queryInternal(query, session, options) {
    const normalizedOptions = this.normalizeQueryExecutionOptions(options);
    const queryJson = resolveQueryJson(query);
    const sessionJson = session ? JSON.stringify(session) : void 0;
    const optionsJson = encodeQueryExecutionOptions(normalizedOptions);
    const runtimeSchema = this.getSchema();
    const results = await this.runtime.query(
      queryJson,
      sessionJson,
      normalizedOptions.tier,
      optionsJson,
    );
    return this.alignQueryRowsToDeclaredSchema(queryJson, results, runtimeSchema);
  }
  /**
   * Update a row by ID without waiting for durability.
   */
  update(objectId, updates) {
    this.updateInternal(objectId, updates);
  }
  /**
   * Update a row by ID without waiting for durability, optionally scoped to a session.
   * @internal
   */
  updateInternal(objectId, updates, session, attribution) {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    if (effectiveSession || attribution !== void 0) {
      this.requireSessionWriteMethod("updateWithSession")(
        objectId,
        updates,
        this.encodeWriteContext(effectiveSession, attribution),
      );
      return;
    }
    this.runtime.update(objectId, updates);
  }
  /**
   * Update a row by ID and wait for durability at the requested tier.
   */
  async updateDurable(objectId, updates, options) {
    await this.updateDurableInternal(objectId, updates, void 0, void 0, options);
  }
  /**
   * Update a row by ID and wait for durability, optionally scoped to a session.
   * @internal
   */
  async updateDurableInternal(objectId, updates, session, attribution, options) {
    const tier = this.resolveWriteTier(options);
    const effectiveSession = this.resolveWriteSession(session, attribution);
    if (effectiveSession || attribution !== void 0) {
      await this.requireSessionWriteMethod("updateDurableWithSession")(
        objectId,
        updates,
        this.encodeWriteContext(effectiveSession, attribution),
        tier,
      );
      return;
    }
    await this.runtime.updateDurable(objectId, updates, tier);
  }
  /**
   * Delete a row by ID without waiting for durability.
   */
  delete(objectId) {
    this.deleteInternal(objectId);
  }
  /**
   * Delete a row by ID without waiting for durability, optionally scoped to a session.
   * @internal
   */
  deleteInternal(objectId, session, attribution) {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    if (effectiveSession || attribution !== void 0) {
      this.requireSessionWriteMethod("deleteWithSession")(
        objectId,
        this.encodeWriteContext(effectiveSession, attribution),
      );
      return;
    }
    this.runtime.delete(objectId);
  }
  /**
   * Delete a row by ID and wait for durability at the requested tier.
   */
  async deleteDurable(objectId, options) {
    await this.deleteDurableInternal(objectId, void 0, void 0, options);
  }
  /**
   * Delete a row by ID and wait for durability, optionally scoped to a session.
   * @internal
   */
  async deleteDurableInternal(objectId, session, attribution, options) {
    const tier = this.resolveWriteTier(options);
    const effectiveSession = this.resolveWriteSession(session, attribution);
    if (effectiveSession || attribution !== void 0) {
      await this.requireSessionWriteMethod("deleteDurableWithSession")(
        objectId,
        this.encodeWriteContext(effectiveSession, attribution),
        tier,
      );
      return;
    }
    await this.runtime.deleteDurable(objectId, tier);
  }
  /**
   * Subscribe to a query and receive updates when results change.
   *
   * @param query Query builder or JSON-encoded query specification
   * @param callback Called with delta whenever results change
   * @param options Optional read durability options
   * @returns Subscription ID for unsubscribing
   */
  subscribe(query, callback, options) {
    return this.subscribeInternal(query, callback, this.resolvedSession ?? void 0, options);
  }
  /**
   * Internal subscribe with optional session and read durability options.
   *
   * Uses the runtime's 2-phase subscribe API: `createSubscription` allocates
   * a handle synchronously (zero work), then `executeSubscription` is deferred
   * via the scheduler so compilation + first tick run outside the caller's
   * synchronous stack (e.g. outside a React render).
   *
   * @internal
   */
  subscribeInternal(query, callback, session, options) {
    const normalizedOptions = this.normalizeQueryExecutionOptions(options);
    const sessionJson = session ? JSON.stringify(session) : void 0;
    const queryJson = resolveQueryJson(query);
    const optionsJson = encodeQueryExecutionOptions(normalizedOptions);
    const runtimeSchema = this.getSchema();
    const handle = this.runtime.createSubscription(
      queryJson,
      sessionJson,
      normalizedOptions.tier,
      optionsJson,
    );
    this.scheduler(() => {
      this.runtime.executeSubscription(handle, (...args) => {
        const deltaJsonOrObject = normalizeSubscriptionCallbackArgs(args);
        if (deltaJsonOrObject === void 0) {
          return;
        }
        const delta =
          typeof deltaJsonOrObject === "string" ? JSON.parse(deltaJsonOrObject) : deltaJsonOrObject;
        callback(this.alignSubscriptionDeltaToDeclaredSchema(queryJson, delta, runtimeSchema));
      });
    });
    return handle;
  }
  /**
   * Unsubscribe from a query.
   *
   * @param subscriptionId ID returned from subscribe()
   */
  unsubscribe(subscriptionId) {
    this.runtime.unsubscribe(subscriptionId);
  }
  /**
   * Get the current schema.
   */
  getSchema() {
    return normalizeRuntimeSchema(this.runtime.getSchema());
  }
  /**
   * Get the underlying runtime (for WorkerBridge).
   * @internal
   */
  getRuntime() {
    return this.runtime;
  }
  /**
   * Get the server URL (for SessionClient).
   * @internal
   */
  getServerUrl() {
    return this.context.serverUrl;
  }
  /**
   * Build a fully-qualified endpoint URL against the configured server.
   * @internal
   */
  getRequestUrl(path) {
    if (!this.context.serverUrl) {
      throw new Error("No server connection");
    }
    return buildEndpointUrl(this.context.serverUrl, path, this.context.serverPathPrefix);
  }
  /**
   * Get schema context for server requests.
   * @internal
   */
  getSchemaContext() {
    return {
      env: this.context.env ?? "dev",
      schema_hash: this.runtime.getSchemaHash(),
      user_branch: this.context.userBranch ?? "main",
    };
  }
  /**
   * Send an HTTP request with appropriate auth headers.
   * @internal
   */
  async sendRequest(url, method, body, session) {
    const headers = {
      "Content-Type": "application/json",
    };
    if (session && this.context.backendSecret) {
      headers["X-Jazz-Backend-Secret"] = this.context.backendSecret;
      headers["X-Jazz-Session"] = btoa(JSON.stringify(session));
    } else {
      applyUserAuthHeaders(headers, {
        jwtToken: this.context.jwtToken,
        localAuthMode: this.context.localAuthMode,
        localAuthToken: this.context.localAuthToken,
      });
    }
    return fetch(url, {
      method,
      headers,
      body: JSON.stringify(body),
    });
  }
  /**
   * Link an anonymous/demo local principal to an external JWT identity.
   *
   * Requires all three auth fields:
   * - `jwtToken`
   * - `localAuthMode`
   * - `localAuthToken`
   *
   * Values default to the current AppContext auth fields unless overridden.
   */
  async linkExternalIdentity(options = {}) {
    if (!this.context.serverUrl) {
      throw new Error("No server connection");
    }
    const jwtToken = options.jwtToken ?? this.context.jwtToken;
    const localAuthMode = options.localAuthMode ?? this.context.localAuthMode;
    const localAuthToken = options.localAuthToken ?? this.context.localAuthToken;
    if (!jwtToken) {
      throw new Error("linkExternalIdentity requires jwtToken");
    }
    if (!localAuthMode) {
      throw new Error("linkExternalIdentity requires localAuthMode");
    }
    if (!localAuthToken) {
      throw new Error("linkExternalIdentity requires localAuthToken");
    }
    return linkExternalIdentity(
      this.context.serverUrl,
      {
        jwtToken,
        localAuthMode,
        localAuthToken,
        pathPrefix: this.context.serverPathPrefix,
      },
      "[client] ",
    );
  }
  /**
   * Shutdown the client and release resources.
   */
  async shutdown() {
    this.streamController.stop();
    if (this.runtime.close) {
      await this.runtime.close();
    }
  }
  setupSync(serverUrl, serverPathPrefix) {
    this.runtime.onSyncMessageToSend(
      createSyncOutboxRouter({
        logPrefix: "[client] ",
        retryServerPayloads: true,
        onServerPayload: /* @__PURE__ */ __name(
          (payload, isCatalogue) => this.sendSyncMessage(payload, isCatalogue),
          "onServerPayload",
        ),
        onServerPayloadError: /* @__PURE__ */ __name((error) => {
          const isExpectedAbort = isExpectedFetchAbortError(error);
          if (!isExpectedAbort) {
            console.error("Sync POST error:", error);
            this.streamController.notifyTransportFailure();
          }
        }, "onServerPayloadError"),
      }),
    );
    this.streamController.start(serverUrl, serverPathPrefix);
  }
  async sendSyncMessage(payloadJson, isCatalogue) {
    const serverUrl = this.streamController.getServerUrl();
    if (!serverUrl) return;
    await sendSyncPayload(
      serverUrl,
      payloadJson,
      isCatalogue,
      {
        ...this.getSyncAuth(),
        clientId: this.serverClientId,
        pathPrefix: this.streamController.getPathPrefix(),
      },
      "[client] ",
    );
  }
};
async function tryLoadNodePackagedWasmBinary() {
  const helperSpecifier = new URL("./node-wasm-init.js", import.meta.url).href;
  const { tryReadPackagedWasmBinary } = await import(
    /* @vite-ignore */
    helperSpecifier
  );
  return tryReadPackagedWasmBinary(import.meta.url);
}
__name(tryLoadNodePackagedWasmBinary, "tryLoadNodePackagedWasmBinary");
async function loadWasmModule(runtime) {
  const wasmModule2 = await Promise.resolve().then(() => (init_jazz_wasm(), jazz_wasm_exports));
  const syncInitInput = resolveRuntimeConfigSyncInitInput(runtime);
  if (syncInitInput) {
    wasmModule2.initSync(syncInitInput);
    return wasmModule2;
  }
  let nodeInitDone = false;
  if (typeof process !== "undefined" && process.versions?.node) {
    try {
      const wasmBinary = await tryLoadNodePackagedWasmBinary();
      if (wasmBinary) {
        wasmModule2.initSync({ module: wasmBinary });
        nodeInitDone = true;
      }
    } catch {}
  }
  if (!nodeInitDone && typeof wasmModule2.default === "function") {
    const wasmUrl =
      typeof location !== "undefined"
        ? resolveRuntimeConfigWasmUrl(import.meta.url, location.href, runtime)
        : null;
    if (wasmUrl) {
      await wasmModule2.default({ module_or_path: wasmUrl });
    } else {
      await wasmModule2.default();
    }
  }
  return wasmModule2;
}
__name(loadWasmModule, "loadWasmModule");

// ../../packages/jazz-tools/src/runtime/worker-bridge.ts
var INIT_RESPONSE_TIMEOUT_MS = 12e3;
var SHUTDOWN_ACK_TIMEOUT_MS = 5e3;
var WorkerBridge = class {
  static {
    __name(this, "WorkerBridge");
  }
  worker;
  runtime;
  state;
  constructor(worker, runtime) {
    this.worker = worker;
    this.runtime = runtime;
    this.state = {
      phase: "idle",
      workerClientId: null,
      initPromise: null,
      pendingSyncPayloadsForWorker: [],
      syncBatchFlushQueued: false,
      peerSyncListener: null,
      serverPayloadForwarder: null,
    };
    this.worker.onmessage = (event) => {
      const msg = event.data;
      if (msg.type === "sync") {
        for (const payload of msg.payload) {
          this.runtime.onSyncMessageReceived(payload);
        }
      } else if (msg.type === "peer-sync") {
        this.state.peerSyncListener?.({
          peerId: msg.peerId,
          term: msg.term,
          payload: msg.payload,
        });
      }
    };
    this.runtime.onSyncMessageToSend(
      createSyncOutboxRouter({
        onServerPayload: /* @__PURE__ */ __name((payload) => {
          if (this.isDisposedLike()) return;
          if (this.state.serverPayloadForwarder) {
            this.state.serverPayloadForwarder(payload);
          } else {
            this.enqueueSyncMessageForWorker(payload);
          }
        }, "onServerPayload"),
      }),
    );
    this.runtime.addServer();
  }
  /**
   * Initialize the worker with schema and config.
   *
   * Waits for the worker to respond with init-ok.
   */
  init(options) {
    if (this.state.initPromise) {
      return this.state.initPromise;
    }
    if (this.isDisposedLike()) {
      const disposedError = Promise.reject(new Error("WorkerBridge has been disposed"));
      this.state.initPromise = disposedError;
      return disposedError;
    }
    this.transition({ type: "INIT_CALLED" });
    const initMsg = {
      type: "init",
      schemaJson: options.schemaJson,
      appId: options.appId,
      env: options.env,
      userBranch: options.userBranch,
      dbName: options.dbName,
      serverUrl: options.serverUrl,
      serverPathPrefix: options.serverPathPrefix,
      jwtToken: options.jwtToken,
      localAuthMode: options.localAuthMode,
      localAuthToken: options.localAuthToken,
      adminSecret: options.adminSecret,
      runtime: options.runtime,
      logLevel: options.logLevel,
      clientId: "",
      // Worker generates its own client ID for main thread
    };
    const responsePromise = waitForMessage(
      this.worker,
      (msg) => msg.type === "init-ok" || msg.type === "error",
      INIT_RESPONSE_TIMEOUT_MS,
      "Worker init timeout",
    );
    this.worker.postMessage(initMsg);
    const initPromise = responsePromise
      .then((response) => {
        if (this.isDisposedLike()) {
          throw new Error("WorkerBridge has been disposed");
        }
        if (response.type === "error") {
          this.transition({ type: "INIT_FAILED" });
          throw new Error(`Worker init failed: ${response.message}`);
        }
        if (response.type === "init-ok") {
          if (this.state.phase !== "initializing") {
            throw new Error("Worker init response arrived after bridge left initializing state");
          }
          this.transition({ type: "INIT_OK", clientId: response.clientId });
          this.flushPendingSyncToWorker();
          return response.clientId;
        }
        throw new Error("Unexpected worker response");
      })
      .catch((error) => {
        if (this.state.phase !== "disposed") {
          this.transition({ type: "INIT_FAILED" });
        }
        throw error;
      });
    this.state.initPromise = initPromise;
    return initPromise;
  }
  /**
   * Update auth credentials in the worker.
   */
  updateAuth(auth) {
    if (this.isDisposedLike()) return;
    this.worker.postMessage({ type: "update-auth", ...auth });
  }
  sendLifecycleHint(event) {
    if (this.isDisposedLike()) return;
    this.worker.postMessage({
      type: "lifecycle-hint",
      event,
      sentAtMs: Date.now(),
    });
  }
  /**
   * Shut down the worker and wait for OPFS handles to be released.
   *
   * @param worker The Worker instance (needed for listening to shutdown-ok)
   */
  async shutdown(worker) {
    if (this.isDisposedLike()) return;
    this.transition({ type: "SHUTDOWN_CALLED" });
    const shutdownAckPromise = waitForMessage(
      worker,
      (msg) => msg.type === "shutdown-ok",
      SHUTDOWN_ACK_TIMEOUT_MS,
      "Worker shutdown timeout",
    );
    this.worker.postMessage({ type: "shutdown" });
    try {
      await shutdownAckPromise;
      this.transition({ type: "SHUTDOWN_FINISHED" });
    } catch {
      this.transition({ type: "SHUTDOWN_FINISHED" });
    }
  }
  /**
   * Get the client ID the worker assigned to the main thread.
   */
  getWorkerClientId() {
    return this.state.workerClientId;
  }
  setServerPayloadForwarder(forwarder) {
    if (this.isDisposedLike()) return;
    this.state.serverPayloadForwarder = forwarder;
  }
  applyIncomingServerPayload(payload) {
    if (this.isDisposedLike()) return;
    this.runtime.onSyncMessageReceived(payload);
  }
  replayServerConnection() {
    if (this.isDisposedLike()) return;
    this.runtime.removeServer();
    this.runtime.addServer();
  }
  onPeerSync(listener) {
    this.state.peerSyncListener = listener;
  }
  openPeer(peerId) {
    if (this.isDisposedLike()) return;
    this.worker.postMessage({ type: "peer-open", peerId });
  }
  sendPeerSync(peerId, term, payload) {
    if (this.isDisposedLike()) return;
    if (payload.length === 0) return;
    const message = {
      type: "peer-sync",
      peerId,
      term,
      payload,
    };
    const transfer = collectPayloadTransferables(payload);
    this.worker.postMessage(message, transfer);
  }
  closePeer(peerId) {
    if (this.isDisposedLike()) return;
    this.worker.postMessage({ type: "peer-close", peerId });
  }
  enqueueSyncMessageForWorker(payload) {
    if (this.isDisposedLike()) return;
    this.state.pendingSyncPayloadsForWorker.push(payload);
    if (this.state.syncBatchFlushQueued) return;
    this.state.syncBatchFlushQueued = true;
    queueMicrotask(() => {
      if (this.isDisposedLike()) {
        this.state.syncBatchFlushQueued = false;
        this.state.pendingSyncPayloadsForWorker = [];
        return;
      }
      this.state.syncBatchFlushQueued = false;
      this.flushPendingSyncToWorker();
    });
  }
  flushPendingSyncToWorker() {
    if (this.state.phase !== "ready" || this.state.pendingSyncPayloadsForWorker.length === 0) {
      return;
    }
    const payloads = this.state.pendingSyncPayloadsForWorker;
    this.state.pendingSyncPayloadsForWorker = [];
    const message = {
      type: "sync",
      payload: payloads,
    };
    const transfer = collectPayloadTransferables(payloads);
    this.worker.postMessage(message, transfer);
  }
  isDisposedLike() {
    return this.state.phase === "disposed" || this.state.phase === "shutting-down";
  }
  transition(event) {
    switch (event.type) {
      case "INIT_CALLED":
        if (this.state.phase === "idle" || this.state.phase === "failed") {
          this.state.phase = "initializing";
        }
        return;
      case "INIT_OK":
        if (this.state.phase !== "initializing") return;
        this.state.workerClientId = event.clientId;
        this.state.phase = "ready";
        return;
      case "INIT_FAILED":
        if (this.state.phase !== "initializing") return;
        this.state.phase = "failed";
        this.state.syncBatchFlushQueued = false;
        return;
      case "SHUTDOWN_CALLED":
        if (this.state.phase === "disposed" || this.state.phase === "shutting-down") return;
        this.state.phase = "shutting-down";
        this.runtime.removeServer();
        return;
      case "SHUTDOWN_FINISHED":
        if (this.state.phase === "disposed") return;
        this.state.phase = "disposed";
        this.disposeInternals();
        return;
    }
  }
  disposeInternals() {
    this.state.pendingSyncPayloadsForWorker = [];
    this.state.serverPayloadForwarder = null;
    this.state.peerSyncListener = null;
    this.state.syncBatchFlushQueued = false;
    this.runtime.onSyncMessageToSend(() => void 0);
  }
};
function collectPayloadTransferables(payloads) {
  return payloads.map((payload) => payload.buffer);
}
__name(collectPayloadTransferables, "collectPayloadTransferables");
function waitForMessage(worker, predicate, timeoutMs, timeoutMessage) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(timeoutMessage));
    }, timeoutMs);
    const handler = /* @__PURE__ */ __name((event) => {
      if (predicate(event.data)) {
        cleanup();
        resolve(event.data);
      }
    }, "handler");
    const cleanup = /* @__PURE__ */ __name(() => {
      clearTimeout(timeout);
      worker.removeEventListener("message", handler);
    }, "cleanup");
    worker.addEventListener("message", handler);
  });
}
__name(waitForMessage, "waitForMessage");

// ../../packages/jazz-tools/src/runtime/row-transformer.ts
function resolveBaseColumns(tableName, schema2, projection) {
  const table = schema2[tableName];
  if (!table) {
    throw new Error(`Unknown table "${tableName}" in schema`);
  }
  return resolveSelectedColumns(tableName, schema2, projection)
    .map((columnName) => {
      const magicType = magicColumnType(columnName);
      if (magicType) {
        return { name: columnName, columnType: magicType };
      }
      const column = table.columns.find((candidate) => candidate.name === columnName);
      return column ? { name: column.name, columnType: column.column_type } : null;
    })
    .filter((column) => column !== null);
}
__name(resolveBaseColumns, "resolveBaseColumns");
function toByteArray(value) {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (Array.isArray(value)) {
    const bytes = value.map((entry) => {
      if (typeof entry !== "number" || !Number.isInteger(entry) || entry < 0 || entry > 255) {
        throw new Error("Invalid Bytea array value. Expected integers in range 0..255.");
      }
      return entry;
    });
    return new Uint8Array(bytes);
  }
  throw new Error("Invalid Bytea value. Expected Uint8Array or byte array.");
}
__name(toByteArray, "toByteArray");
function buildIncludePlans(tableName, includes, relationsByTable) {
  const relations = relationsByTable.get(tableName) || [];
  const plans = [];
  for (const [relationName, spec] of Object.entries(includes)) {
    const relation = relations.find((candidate) => candidate.name === relationName);
    if (!relation) {
      throw new Error(`Unknown relation "${relationName}" on table "${tableName}"`);
    }
    const nested = buildIncludePlans(relation.toTable, spec.includes, relationsByTable);
    plans.push({
      relation,
      nested,
      projection: spec.select.length > 0 ? spec.select : void 0,
    });
  }
  return plans;
}
__name(buildIncludePlans, "buildIncludePlans");
function transformIncludedValue(value, plan, schema2) {
  if (value.type !== "Array") {
    return unwrapValue(value);
  }
  const rows = value.value.map((entry) => {
    if (entry.type !== "Row") {
      return unwrapValue(entry);
    }
    const rowId = entry.value.id;
    const columnValues = entry.value.values;
    return transformRowValues(
      columnValues,
      schema2,
      plan.relation.toTable,
      plan.nested,
      rowId,
      plan.projection,
    );
  });
  return plan.relation.isArray ? rows : (rows[0] ?? null);
}
__name(transformIncludedValue, "transformIncludedValue");
function transformRowValues(values, schema2, tableName, includePlans, rowId, projection) {
  const table = schema2[tableName];
  if (!table) {
    throw new Error(`Unknown table "${tableName}" in schema`);
  }
  const obj = {};
  if (rowId !== void 0) {
    obj.id = rowId;
  }
  const baseColumns = resolveBaseColumns(tableName, schema2, projection);
  for (let i = 0; i < baseColumns.length; i++) {
    const col2 = baseColumns[i];
    if (!col2) continue;
    const value = values[i];
    if (value !== void 0) {
      obj[col2.name] = unwrapValue(value, col2.columnType);
    }
  }
  for (let i = 0; i < includePlans.length; i++) {
    const value = values[baseColumns.length + i];
    if (value === void 0) continue;
    const plan = includePlans[i];
    if (!plan) continue;
    obj[plan.relation.name] = transformIncludedValue(value, plan, schema2);
  }
  return obj;
}
__name(transformRowValues, "transformRowValues");
function unwrapValue(v, columnType) {
  switch (v.type) {
    case "Text":
      if (columnType?.type === "Json") {
        try {
          return JSON.parse(v.value);
        } catch (error) {
          throw new Error(
            `Invalid stored JSON value: ${error instanceof Error ? error.message : String(error)}`,
          );
        }
      }
      return v.value;
    case "Uuid":
      return v.value;
    case "Boolean":
      return v.value;
    case "Integer":
    case "BigInt":
    case "Double":
      return v.value;
    case "Timestamp":
      return new Date(v.value);
    case "Bytea":
      return toByteArray(v.value);
    case "Null":
      return null;
    case "Array":
      if (columnType?.type === "Array") {
        return v.value.map((entry) => unwrapValue(entry, columnType.element));
      }
      return v.value.map((entry) => unwrapValue(entry));
    case "Row":
      if (columnType?.type === "Row") {
        return v.value.values.map((entry, index) =>
          unwrapValue(entry, columnType.columns[index]?.column_type),
        );
      }
      return v.value.values.map((entry) => unwrapValue(entry));
  }
}
__name(unwrapValue, "unwrapValue");
function transformRows(rows, schema2, tableName, includes = {}, projection) {
  if (!schema2[tableName]) {
    throw new Error(`Unknown table "${tableName}" in schema`);
  }
  const includePlans =
    Object.keys(includes).length === 0
      ? []
      : buildIncludePlans(tableName, normalizeIncludeEntries(includes), analyzeRelations(schema2));
  return rows.map((row) => {
    return transformRowValues(row.values, schema2, tableName, includePlans, row.id, projection);
  });
}
__name(transformRows, "transformRows");
function transformRow(row, schema2, tableName, includes = {}, projection) {
  const transformed = transformRows([row], schema2, tableName, includes, projection)[0];
  if (transformed === void 0) {
    throw new Error(`Failed to transform row for table "${tableName}"`);
  }
  return transformed;
}
__name(transformRow, "transformRow");

// ../../packages/jazz-tools/src/runtime/value-converter.ts
function toTimestampMs2(value) {
  const numeric = value instanceof Date ? value.getTime() : Number(value);
  if (!Number.isFinite(numeric)) {
    throw new Error("Invalid timestamp value. Expected Date or finite number.");
  }
  return numeric;
}
__name(toTimestampMs2, "toTimestampMs");
function toValue(value, columnType) {
  if (value === null || value === void 0) {
    return { type: "Null" };
  }
  switch (columnType.type) {
    case "Text":
      return { type: "Text", value: String(value) };
    case "Boolean":
      return { type: "Boolean", value: Boolean(value) };
    case "Integer":
      return { type: "Integer", value: Number(value) };
    case "BigInt":
      return { type: "BigInt", value: Number(value) };
    case "Double":
      return { type: "Double", value: Number(value) };
    case "Timestamp":
      return { type: "Timestamp", value: toTimestampMs2(value) };
    case "Uuid":
      return { type: "Uuid", value: String(value) };
    case "Bytea": {
      if (value instanceof Uint8Array) {
        return { type: "Bytea", value };
      }
      if (Array.isArray(value)) {
        const bytes = value.map((entry) => {
          const n = Number(entry);
          if (!Number.isInteger(n) || n < 0 || n > 255) {
            throw new Error("Bytea arrays must contain integers in range 0..255");
          }
          return n;
        });
        return { type: "Bytea", value: new Uint8Array(bytes) };
      }
      throw new Error("Expected Uint8Array or byte array for Bytea column type");
    }
    case "Json":
      return { type: "Text", value: toJsonText(value) };
    case "Enum": {
      const enumValue = String(value);
      if (!columnType.variants.includes(enumValue)) {
        throw new Error(
          `Invalid enum value "${enumValue}". Expected one of: ${columnType.variants.join(", ")}`,
        );
      }
      return { type: "Text", value: enumValue };
    }
    case "Array": {
      if (!Array.isArray(value)) {
        throw new Error(`Expected array for Array column type, got ${typeof value}`);
      }
      const elementType = columnType.element;
      return {
        type: "Array",
        value: value.map((v) => toValue(v, elementType)),
      };
    }
    case "Row": {
      if (typeof value !== "object" || value === null) {
        throw new Error(`Expected object for Row column type, got ${typeof value}`);
      }
      const rowValue = value;
      const columns = columnType.columns;
      return {
        type: "Row",
        value: { values: columns.map((col2) => toValue(rowValue[col2.name], col2.column_type)) },
      };
    }
    default:
      throw new Error(`Unsupported column type: ${columnType.type}`);
  }
}
__name(toValue, "toValue");
function toInsertRecord(data, schema2, tableName) {
  const table = schema2[tableName];
  if (!table) {
    throw new Error(`Unknown table "${tableName}"`);
  }
  const result = {};
  for (const [key, value] of Object.entries(data)) {
    if (value === void 0) continue;
    const col2 = table.columns.find((c) => c.name === key);
    if (!col2) {
      throw new Error(`Unknown column "${key}" on table "${tableName}"`);
    }
    if (value === null && !col2.nullable) {
      throw new Error(`Cannot set required field '${key}' to null`);
    }
    result[key] = toValue(value, col2.column_type);
  }
  return result;
}
__name(toInsertRecord, "toInsertRecord");
function toUpdateRecord(data, schema2, tableName) {
  const table = schema2[tableName];
  if (!table) {
    throw new Error(`Unknown table "${tableName}"`);
  }
  const result = {};
  for (const [key, value] of Object.entries(data)) {
    if (value === void 0) continue;
    const col2 = table.columns.find((c) => c.name === key);
    if (!col2) {
      throw new Error(`Unknown column "${key}" on table "${tableName}"`);
    }
    if (value === null && !col2.nullable) {
      throw new Error(`Cannot set required field '${key}' to null`);
    }
    result[key] = toValue(value, col2.column_type);
  }
  return result;
}
__name(toUpdateRecord, "toUpdateRecord");

// ../../packages/jazz-tools/src/runtime/subscription-manager.ts
var RowChangeKind = {
  Added: 0,
  Removed: 1,
  Updated: 2,
};
var SubscriptionManager = class {
  static {
    __name(this, "SubscriptionManager");
  }
  currentResults = /* @__PURE__ */ new Map();
  orderedIds = [];
  removeId(id) {
    const index = this.orderedIds.indexOf(id);
    if (index !== -1) {
      this.orderedIds.splice(index, 1);
    }
  }
  insertIdAt(id, index) {
    const clamped = Math.max(0, Math.min(index, this.orderedIds.length));
    this.orderedIds.splice(clamped, 0, id);
  }
  /**
   * Process a row delta and return typed object delta.
   *
   * @param delta Raw row delta from WASM runtime
   * @param transform Function to convert WasmRow to typed object T
   * @returns Typed delta with full state and changes
   */
  handleDelta(delta, transform) {
    delta.sort((a, b) => a.index - b.index);
    for (const change of delta) {
      switch (change.kind) {
        case RowChangeKind.Added:
          this.currentResults.set(change.id, transform(change.row));
          this.insertIdAt(change.id, change.index);
          break;
        case RowChangeKind.Removed:
          this.currentResults.delete(change.id);
          this.removeId(change.id);
          break;
        case RowChangeKind.Updated:
          this.removeId(change.id);
          this.insertIdAt(change.id, change.index);
          if (change.row) {
            this.currentResults.set(change.id, transform(change.row));
          }
          break;
      }
    }
    return {
      all: this.orderedIds
        .map((id) => this.currentResults.get(id))
        .filter((item) => item !== void 0),
      delta,
    };
  }
  /**
   * Clear all tracked state.
   *
   * Called when unsubscribing to free memory.
   */
  clear() {
    this.currentResults.clear();
    this.orderedIds = [];
  }
  /**
   * Get the current number of tracked items.
   */
  get size() {
    return this.currentResults.size;
  }
};

// ../../packages/jazz-tools/src/runtime/file-storage.ts
var DEFAULT_FILE_CHUNK_SIZE_BYTES = 256 * 1024;
var MAX_FILE_PART_BYTES = 1048576;
var DEFAULT_MIME_TYPE = "application/octet-stream";
var FileNotFoundError = class extends Error {
  static {
    __name(this, "FileNotFoundError");
  }
  fileId;
  constructor(fileId) {
    super(`File "${fileId}" was not found.`);
    this.name = "FileNotFoundError";
    this.fileId = fileId;
  }
};
var IncompleteFileDataError = class extends Error {
  static {
    __name(this, "IncompleteFileDataError");
  }
  fileId;
  reason;
  partId;
  partIndex;
  constructor(fileId, reason, message, options = {}) {
    super(message);
    this.name = "IncompleteFileDataError";
    this.fileId = fileId;
    this.reason = reason;
    this.partId = options.partId;
    this.partIndex = options.partIndex;
  }
};
var DEFAULT_COLUMNS = {
  name: "name",
  mimeType: "mimeType",
  partIds: "partIds",
  partSizes: "partSizes",
  data: "data",
};
function createFileStorage(db, options) {
  const columns = {
    ...DEFAULT_COLUMNS,
    ...options.columns,
  };
  const defaultChunkSizeBytes = options.defaultChunkSizeBytes ?? DEFAULT_FILE_CHUNK_SIZE_BYTES;
  validateChunkSize(defaultChunkSizeBytes);
  const insertRow = /* @__PURE__ */ __name(async (table, data, writeOptions) => {
    if (writeOptions?.tier) {
      return db.insertDurable(table, data, { tier: writeOptions.tier });
    }
    return db.insert(table, data);
  }, "insertRow");
  const loadFileRecord = /* @__PURE__ */ __name(async (fileOrId, readOptions) => {
    const queryOptions = toQueryOptions(readOptions);
    if (typeof fileOrId === "string") {
      const file = await db.one(options.files.where({ id: fileOrId }), queryOptions);
      if (!file) {
        throw new FileNotFoundError(fileOrId);
      }
      return normalizeFileRecord(file, columns);
    }
    return normalizeFileRecord(fileOrId, columns);
  }, "loadFileRecord");
  const loadPartBytes = /* @__PURE__ */ __name(async (file, partIndex, readOptions) => {
    const partId = file.partIds[partIndex];
    const expectedSize = file.partSizes[partIndex];
    const queryOptions = toQueryOptions(readOptions);
    const part = await db.one(options.fileParts.where({ id: partId }), queryOptions);
    if (!part) {
      throw new IncompleteFileDataError(
        file.id,
        "missing-part",
        `File "${file.id}" is incomplete: missing part ${partIndex} (${partId}) at the requested query tier.`,
        { partId, partIndex },
      );
    }
    const raw = part[columns.data];
    const bytes = asUint8Array(raw, `File part "${partId}" has invalid "${columns.data}" data.`);
    if (bytes.length !== expectedSize) {
      throw new IncompleteFileDataError(
        file.id,
        "part-size-mismatch",
        `File "${file.id}" is incomplete: part ${partIndex} (${partId}) expected ${expectedSize} bytes, got ${bytes.length}.`,
        { partId, partIndex },
      );
    }
    return bytes;
  }, "loadPartBytes");
  const createReadStream = /* @__PURE__ */ __name((file, readOptions) => {
    let nextIndex = 0;
    let canceled = false;
    return new ReadableStream({
      async pull(controller) {
        if (canceled) {
          controller.close();
          return;
        }
        if (nextIndex >= file.partIds.length) {
          controller.close();
          return;
        }
        const currentIndex = nextIndex;
        nextIndex += 1;
        try {
          const bytes = await loadPartBytes(file, currentIndex, readOptions);
          if (canceled) {
            controller.close();
            return;
          }
          controller.enqueue(bytes);
          if (nextIndex >= file.partIds.length) {
            controller.close();
          }
        } catch (error) {
          controller.error(error);
        }
      },
      cancel() {
        canceled = true;
      },
    });
  }, "createReadStream");
  return {
    async fromBlob(blob, writeOptions = {}) {
      const name = writeOptions.name ?? getFileName(blob);
      const mimeType = writeOptions.mimeType ?? (blob.type || DEFAULT_MIME_TYPE);
      return this.fromStream(blob.stream(), {
        ...writeOptions,
        mimeType,
        ...(name !== void 0 ? { name } : {}),
      });
    },
    async fromStream(stream, writeOptions = {}) {
      const chunkSizeBytes = writeOptions.chunkSizeBytes ?? defaultChunkSizeBytes;
      validateChunkSize(chunkSizeBytes);
      const filepartIds = [];
      const partSizes = [];
      for await (const chunk of chunkReadableStream(stream, chunkSizeBytes)) {
        if (chunk.length > MAX_FILE_PART_BYTES) {
          throw new Error(
            `File chunk exceeded the ${MAX_FILE_PART_BYTES}-byte BYTEA limit: ${chunk.length} bytes.`,
          );
        }
        const part = await insertRow(options.fileParts, { [columns.data]: chunk }, writeOptions);
        if (typeof part.id !== "string") {
          throw new Error(`Inserted file part row is missing a string "id".`);
        }
        filepartIds.push(part.id);
        partSizes.push(chunk.length);
      }
      return insertRow(
        options.files,
        {
          [columns.mimeType]: writeOptions.mimeType ?? DEFAULT_MIME_TYPE,
          [columns.partIds]: filepartIds,
          [columns.partSizes]: partSizes,
          ...(writeOptions.name !== void 0 ? { [columns.name]: writeOptions.name } : {}),
        },
        writeOptions,
      );
    },
    async toStream(fileOrId, readOptions = {}) {
      const file = await loadFileRecord(fileOrId, readOptions);
      return createReadStream(file, readOptions);
    },
    async toBlob(fileOrId, readOptions = {}) {
      const file = await loadFileRecord(fileOrId, readOptions);
      const stream = createReadStream(file, readOptions);
      const reader = stream.getReader();
      const chunks = [];
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }
        chunks.push(value);
      }
      return new Blob(
        chunks.map((chunk) => toBlobPart(chunk)),
        { type: file.mimeType },
      );
    },
  };
  async function* chunkReadableStream(stream, chunkSizeBytes) {
    const reader = stream.getReader();
    const pending = [];
    let pendingBytes = 0;
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }
        const bytes = asUint8Array(value, "ReadableStream chunk must be binary data.");
        if (bytes.length === 0) {
          continue;
        }
        pending.push(bytes);
        pendingBytes += bytes.length;
        while (pendingBytes >= chunkSizeBytes) {
          yield takePendingBytes(pending, chunkSizeBytes);
          pendingBytes -= chunkSizeBytes;
        }
      }
      if (pendingBytes > 0) {
        yield takePendingBytes(pending, pendingBytes);
      }
    } finally {
      try {
        reader.releaseLock();
      } catch {}
    }
  }
  __name(chunkReadableStream, "chunkReadableStream");
  function takePendingBytes(pending, targetLength) {
    const out = new Uint8Array(targetLength);
    let offset = 0;
    while (offset < targetLength) {
      const current = pending[0];
      if (!current) {
        throw new Error("Chunking logic ran out of pending bytes.");
      }
      const remaining = targetLength - offset;
      const consume = Math.min(remaining, current.length);
      out.set(current.subarray(0, consume), offset);
      offset += consume;
      if (consume === current.length) {
        pending.shift();
      } else {
        pending[0] = current.subarray(consume);
      }
    }
    return out;
  }
  __name(takePendingBytes, "takePendingBytes");
  function normalizeFileRecord(file, names) {
    const id = file.id;
    if (typeof id !== "string") {
      throw new Error(`File row is missing a string "id".`);
    }
    const partIds = readStringArray(
      file[names.partIds],
      new IncompleteFileDataError(
        id,
        "invalid-file-record",
        `File "${id}" is incomplete: invalid "${names.partIds}" metadata.`,
      ),
    );
    const partSizes = readIntegerArray(
      file[names.partSizes],
      new IncompleteFileDataError(
        id,
        "invalid-file-record",
        `File "${id}" is incomplete: invalid "${names.partSizes}" metadata.`,
      ),
    );
    if (partIds.length !== partSizes.length) {
      throw new IncompleteFileDataError(
        id,
        "invalid-file-record",
        `File "${id}" is incomplete: "${names.partIds}" and "${names.partSizes}" lengths do not match.`,
      );
    }
    return {
      id,
      name: typeof file[names.name] === "string" ? file[names.name] : void 0,
      mimeType:
        typeof file[names.mimeType] === "string" && file[names.mimeType].length > 0
          ? file[names.mimeType]
          : DEFAULT_MIME_TYPE,
      partIds,
      partSizes,
    };
  }
  __name(normalizeFileRecord, "normalizeFileRecord");
  function readStringArray(value, error) {
    if (!Array.isArray(value) || value.some((entry) => typeof entry !== "string")) {
      throw error;
    }
    return [...value];
  }
  __name(readStringArray, "readStringArray");
  function readIntegerArray(value, error) {
    if (!Array.isArray(value) || value.some((entry) => !Number.isInteger(entry) || entry < 0)) {
      throw error;
    }
    return value.map((entry) => Number(entry));
  }
  __name(readIntegerArray, "readIntegerArray");
  function asUint8Array(value, message) {
    if (value instanceof Uint8Array) {
      return value;
    }
    if (value instanceof ArrayBuffer) {
      return new Uint8Array(value);
    }
    if (ArrayBuffer.isView(value)) {
      return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    }
    if (Array.isArray(value)) {
      const numbers = value.map((entry) => {
        const n = Number(entry);
        if (!Number.isInteger(n) || n < 0 || n > 255) {
          throw new Error(message);
        }
        return n;
      });
      return Uint8Array.from(numbers);
    }
    throw new Error(message);
  }
  __name(asUint8Array, "asUint8Array");
  function toBlobPart(bytes) {
    const copy = new Uint8Array(bytes.byteLength);
    copy.set(bytes);
    return copy.buffer;
  }
  __name(toBlobPart, "toBlobPart");
  function getFileName(blob) {
    if (typeof File !== "undefined" && blob instanceof File) {
      return blob.name;
    }
    return void 0;
  }
  __name(getFileName, "getFileName");
}
__name(createFileStorage, "createFileStorage");
function createConventionalFileStorage(db, app2) {
  return createFileStorage(db, {
    files: app2.files,
    fileParts: app2.file_parts,
  });
}
__name(createConventionalFileStorage, "createConventionalFileStorage");
function validateChunkSize(chunkSizeBytes) {
  if (!Number.isInteger(chunkSizeBytes) || chunkSizeBytes <= 0) {
    throw new Error("chunkSizeBytes must be a positive integer.");
  }
  if (chunkSizeBytes > MAX_FILE_PART_BYTES) {
    throw new Error(
      `chunkSizeBytes must be <= ${MAX_FILE_PART_BYTES} bytes to fit inside a BYTEA file part.`,
    );
  }
}
__name(validateChunkSize, "validateChunkSize");
function toQueryOptions(readOptions) {
  if (!readOptions) {
    return void 0;
  }
  const { propagation, tier, visibility } = readOptions;
  if (propagation === void 0 && tier === void 0 && visibility === void 0) {
    return void 0;
  }
  return { propagation, tier, visibility };
}
__name(toQueryOptions, "toQueryOptions");

// ../../packages/jazz-tools/src/runtime/leader-lock.ts
function resolveNavigatorLocks() {
  const nav = globalThis.navigator;
  if (!nav || !nav.locks) return null;
  const locks = nav.locks;
  if (typeof locks.request !== "function") return null;
  return locks;
}
__name(resolveNavigatorLocks, "resolveNavigatorLocks");
function createNavigatorLocksLeaderLockStrategy(lockManager = resolveNavigatorLocks()) {
  if (!lockManager) return null;
  return {
    async tryAcquire(lockName) {
      let resolveAcquired = null;
      const acquiredPromise = new Promise((resolve) => {
        resolveAcquired = resolve;
      });
      let releaseLock = null;
      const heldUntilReleased = new Promise((resolve) => {
        releaseLock = /* @__PURE__ */ __name(() => resolve(), "releaseLock");
      });
      void lockManager
        .request(lockName, { mode: "exclusive", ifAvailable: true }, async (lock) => {
          if (!lock) {
            resolveAcquired?.(null);
            resolveAcquired = null;
            return;
          }
          resolveAcquired?.({
            release: /* @__PURE__ */ __name(() => {
              if (!releaseLock) return;
              releaseLock();
              releaseLock = null;
            }, "release"),
          });
          resolveAcquired = null;
          await heldUntilReleased;
        })
        .catch(() => {
          resolveAcquired?.(null);
          resolveAcquired = null;
        });
      return await acquiredPromise;
    },
  };
}
__name(createNavigatorLocksLeaderLockStrategy, "createNavigatorLocksLeaderLockStrategy");

// ../../packages/jazz-tools/src/runtime/tab-leader-election.ts
function randomTabId() {
  const cryptoObj = globalThis.crypto;
  if (cryptoObj && typeof cryptoObj.randomUUID === "function") {
    return cryptoObj.randomUUID();
  }
  return `tab-${Math.random().toString(36).slice(2, 12)}`;
}
__name(randomTabId, "randomTabId");
function compareTabIds(a, b) {
  if (a === b) return 0;
  return a < b ? -1 : 1;
}
__name(compareTabIds, "compareTabIds");
function isMessage(value) {
  if (typeof value !== "object" || value === null) return false;
  const msg = value;
  if (msg.type === "leader-heartbeat") {
    return (
      typeof msg.leaderTabId === "string" &&
      typeof msg.term === "number" &&
      typeof msg.sentAtMs === "number"
    );
  }
  if (msg.type === "who-is-leader") {
    return typeof msg.requesterTabId === "string";
  }
  if (msg.type === "leader-claim") {
    return (
      typeof msg.candidateTabId === "string" &&
      typeof msg.term === "number" &&
      typeof msg.sentAtMs === "number"
    );
  }
  return false;
}
__name(isMessage, "isMessage");
function resolveBroadcastChannelCtor() {
  const ctor = globalThis.BroadcastChannel;
  if (typeof ctor !== "function") return null;
  return ctor;
}
__name(resolveBroadcastChannelCtor, "resolveBroadcastChannelCtor");
var TabLeaderElection = class {
  static {
    __name(this, "TabLeaderElection");
  }
  tabId;
  heartbeatMs;
  leaseMs;
  now;
  channelName;
  lockName;
  lockStrategy;
  started = false;
  channel = null;
  role = "follower";
  term = 0;
  leaderTabId = null;
  lastLeaderSeenAtMs = 0;
  heartbeatTimer = null;
  leaseDeadlineTimer = null;
  probeInFlight = false;
  leadershipLockLease = null;
  listeners = /* @__PURE__ */ new Set();
  readyResolve = null;
  readyReject = null;
  readyPromise;
  readySettled = false;
  onMessage = /* @__PURE__ */ __name((event) => {
    this.handleIncomingMessage(event.data);
  }, "onMessage");
  constructor(options) {
    this.tabId = options.tabId ?? randomTabId();
    this.heartbeatMs = Math.max(100, options.heartbeatMs ?? 1e3);
    this.leaseMs = Math.max(this.heartbeatMs * 2, options.leaseMs ?? 5e3);
    this.now = options.now ?? (() => Date.now());
    this.channelName = `jazz-leader:${options.appId}:${options.dbName}`;
    this.lockName = `jazz-leader-lock:${options.appId}:${options.dbName}`;
    this.lockStrategy = options.lockStrategy ?? createNavigatorLocksLeaderLockStrategy();
    this.readyPromise = new Promise((resolve, reject) => {
      this.readyResolve = resolve;
      this.readyReject = reject;
    });
  }
  start() {
    if (this.started) return;
    this.started = true;
    const ChannelCtor = resolveBroadcastChannelCtor();
    if (ChannelCtor) {
      this.channel = new ChannelCtor(this.channelName);
      this.channel.addEventListener("message", this.onMessage);
      this.requestCurrentLeader();
    }
    void this.tryTakeLeadership({ requestLeaderOnFailure: false });
    this.scheduleLeaseDeadlineCheck();
  }
  stop() {
    if (!this.started) return;
    this.started = false;
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    this.clearLeaseDeadlineTimer();
    this.releaseLeadershipLock();
    if (this.channel) {
      this.channel.removeEventListener("message", this.onMessage);
      this.channel.close();
      this.channel = null;
    }
    if (!this.readySettled && this.readyReject) {
      this.readyReject(new Error("Leader election stopped before initial leader was chosen"));
      this.readyReject = null;
      this.readyResolve = null;
      this.readySettled = true;
    }
  }
  onChange(listener) {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }
  snapshot() {
    return {
      role: this.role,
      tabId: this.tabId,
      leaderTabId: this.leaderTabId,
      term: this.term,
    };
  }
  isLeader() {
    return this.role === "leader";
  }
  async waitForInitialLeader(timeoutMs = 2e3) {
    if (this.readySettled) {
      return this.snapshot();
    }
    return await Promise.race([
      this.readyPromise,
      new Promise((_resolve, reject) => {
        setTimeout(() => reject(new Error("Leader election timeout")), timeoutMs);
      }),
    ]);
  }
  handleIncomingMessage(raw) {
    if (!isMessage(raw)) return;
    switch (raw.type) {
      case "who-is-leader":
        if (this.role === "leader") {
          this.sendHeartbeat();
        }
        return;
      case "leader-heartbeat":
        this.handleLeaderHeartbeat(raw);
        return;
      case "leader-claim":
        this.handleLeaderClaim(raw);
        return;
    }
  }
  handleLeaderHeartbeat(message) {
    const shouldAdopt =
      message.term > this.term ||
      (message.term === this.term &&
        (this.leaderTabId === null ||
          message.leaderTabId === this.leaderTabId ||
          compareTabIds(message.leaderTabId, this.leaderTabId) > 0));
    if (!shouldAdopt) {
      return;
    }
    this.setLeader(message.leaderTabId, message.term);
    this.lastLeaderSeenAtMs = this.now();
    this.scheduleLeaseDeadlineCheck();
  }
  handleLeaderClaim(message) {
    const shouldAdopt =
      message.term > this.term ||
      (message.term === this.term &&
        (this.leaderTabId === null || compareTabIds(message.candidateTabId, this.leaderTabId) > 0));
    if (!shouldAdopt) {
      return;
    }
    this.setLeader(message.candidateTabId, message.term);
    this.lastLeaderSeenAtMs = this.now();
    this.scheduleLeaseDeadlineCheck();
  }
  promoteToLeader(nextTerm) {
    const electedTerm = Math.max(this.term + 1, nextTerm);
    this.setLeader(this.tabId, electedTerm);
    this.lastLeaderSeenAtMs = this.now();
    this.postMessage({
      type: "leader-claim",
      candidateTabId: this.tabId,
      term: electedTerm,
      sentAtMs: this.now(),
    });
    this.sendHeartbeat();
  }
  setLeader(leaderTabId, term) {
    const prevLeader = this.leaderTabId;
    const prevRole = this.role;
    const prevTerm = this.term;
    const nextRole = leaderTabId === this.tabId ? "leader" : "follower";
    this.term = term;
    this.leaderTabId = leaderTabId;
    this.role = nextRole;
    if (this.role === "leader") {
      this.ensureHeartbeatTimer();
      this.clearLeaseDeadlineTimer();
    } else {
      if (prevRole === "leader") {
        this.releaseLeadershipLock();
      }
      this.clearHeartbeatTimer();
      this.scheduleLeaseDeadlineCheck();
    }
    this.resolveReadyIfNeeded();
    const changed = prevLeader !== leaderTabId || prevRole !== nextRole || prevTerm !== this.term;
    if (changed) {
      this.emitChange();
    }
  }
  ensureHeartbeatTimer() {
    if (this.heartbeatTimer) return;
    this.heartbeatTimer = setInterval(() => {
      this.sendHeartbeat();
    }, this.heartbeatMs);
  }
  clearHeartbeatTimer() {
    if (!this.heartbeatTimer) return;
    clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
  }
  scheduleLeaseDeadlineCheck() {
    if (!this.started || this.role === "leader") {
      this.clearLeaseDeadlineTimer();
      return;
    }
    const delayMs = this.leaderTabId
      ? Math.max(0, this.lastLeaderSeenAtMs + this.leaseMs - this.now())
      : this.heartbeatMs;
    this.clearLeaseDeadlineTimer();
    this.leaseDeadlineTimer = setTimeout(() => {
      this.leaseDeadlineTimer = null;
      this.onLeaseDeadline();
    }, delayMs);
  }
  clearLeaseDeadlineTimer() {
    if (!this.leaseDeadlineTimer) return;
    clearTimeout(this.leaseDeadlineTimer);
    this.leaseDeadlineTimer = null;
  }
  onLeaseDeadline() {
    if (!this.started || this.role === "leader") return;
    if (!this.leaderTabId) {
      void this.tryTakeLeadership({ requestLeaderOnFailure: true });
      return;
    }
    const elapsed = this.now() - this.lastLeaderSeenAtMs;
    if (elapsed >= this.leaseMs) {
      void this.tryTakeLeadership({ requestLeaderOnFailure: true });
      return;
    }
    this.scheduleLeaseDeadlineCheck();
  }
  sendHeartbeat() {
    if (!this.started || this.role !== "leader") return;
    this.postMessage({
      type: "leader-heartbeat",
      leaderTabId: this.tabId,
      term: this.term,
      sentAtMs: this.now(),
    });
  }
  postMessage(message) {
    this.channel?.postMessage(message);
  }
  requestCurrentLeader() {
    this.postMessage({
      type: "who-is-leader",
      requesterTabId: this.tabId,
    });
  }
  async tryTakeLeadership(options) {
    if (!this.started || this.isLeader()) return;
    if (this.probeInFlight) return;
    this.probeInFlight = true;
    try {
      const acquired = await this.tryAcquireLeadershipLock();
      if (!this.started || this.isLeader()) return;
      if (acquired) {
        this.promoteToLeader(this.term + 1);
        return;
      }
      if (options.requestLeaderOnFailure) {
        this.requestCurrentLeader();
      }
      this.scheduleLeaseDeadlineCheck();
    } finally {
      this.probeInFlight = false;
    }
  }
  async tryAcquireLeadershipLock() {
    if (this.leadershipLockLease) return true;
    if (!this.lockStrategy) return false;
    const lease = await this.lockStrategy.tryAcquire(this.lockName);
    if (!lease) return false;
    this.leadershipLockLease = lease;
    return true;
  }
  releaseLeadershipLock() {
    const lease = this.leadershipLockLease;
    this.leadershipLockLease = null;
    lease?.release();
  }
  emitChange() {
    const snapshot = this.snapshot();
    for (const listener of this.listeners) {
      listener(snapshot);
    }
  }
  resolveReadyIfNeeded() {
    if (this.readySettled || !this.leaderTabId || !this.readyResolve) return;
    this.readySettled = true;
    this.readyResolve(this.snapshot());
    this.readyResolve = null;
    this.readyReject = null;
  }
};

// ../../packages/jazz-tools/src/runtime/db.ts
var DEFAULT_WASM_LOG_LEVEL = "warn";
function setGlobalWasmLogLevel(level) {
  globalThis.__JAZZ_WASM_LOG_LEVEL = level ?? DEFAULT_WASM_LOG_LEVEL;
}
__name(setGlobalWasmLogLevel, "setGlobalWasmLogLevel");
function resolveStorageDriver(driver) {
  return driver ?? { type: "persistent" };
}
__name(resolveStorageDriver, "resolveStorageDriver");
function trimSubscriptionTraceStack(stack) {
  if (!stack) {
    return stack;
  }
  const lines = stack.split("\n");
  if (lines.length <= 1) {
    return stack;
  }
  const isInternalFrame = /* @__PURE__ */ __name((line) => {
    return (
      line.includes("Db.registerActiveQuerySubscriptionTrace") ||
      line.includes("Db.subscribeAll") ||
      line.includes("SubscriptionsOrchestrator.ensureEntryForKey") ||
      line.includes("SubscriptionsOrchestrator.getCacheEntry") ||
      line.includes("/node_modules/") ||
      line.includes("react-dom") ||
      line.includes("react_stack_bottom_frame")
    );
  }, "isInternalFrame");
  const firstOriginIndex = lines.findIndex((line, index) => index > 0 && !isInternalFrame(line));
  if (firstOriginIndex <= 0) {
    return stack;
  }
  return [lines[0], ...lines.slice(firstOriginIndex)].join("\n");
}
__name(trimSubscriptionTraceStack, "trimSubscriptionTraceStack");
function cloneActiveQuerySubscriptionTrace(trace) {
  return {
    ...trace,
    branches: [...trace.branches],
  };
}
__name(cloneActiveQuerySubscriptionTrace, "cloneActiveQuerySubscriptionTrace");
function resolveHopOutputTable(schema2, startTable, hops) {
  if (hops.length === 0) {
    return startTable;
  }
  const relations = analyzeRelations(schema2);
  let currentTable = startTable;
  for (const hopName of hops) {
    const candidates = relations.get(currentTable) ?? [];
    const relation = candidates.find((candidate) => candidate.name === hopName);
    if (!relation) {
      throw new Error(`Unknown relation "${hopName}" on table "${currentTable}"`);
    }
    currentTable = relation.toTable;
  }
  return currentTable;
}
__name(resolveHopOutputTable, "resolveHopOutputTable");
function resolveSchemaWithTable(preferredSchema, fallbackSchema, tableName) {
  return preferredSchema[tableName] ? preferredSchema : fallbackSchema;
}
__name(resolveSchemaWithTable, "resolveSchemaWithTable");
function resolveBroadcastChannelCtor2() {
  const ctor = globalThis.BroadcastChannel;
  if (typeof ctor !== "function") return null;
  return ctor;
}
__name(resolveBroadcastChannelCtor2, "resolveBroadcastChannelCtor");
function isBinaryPayloadArray(value) {
  return Array.isArray(value) && value.every((entry) => entry instanceof Uint8Array);
}
__name(isBinaryPayloadArray, "isBinaryPayloadArray");
function isTabSyncMessage(value) {
  if (typeof value !== "object" || value === null) return false;
  const message = value;
  if (message.type === "follower-sync") {
    return (
      typeof message.fromTabId === "string" &&
      typeof message.toLeaderTabId === "string" &&
      typeof message.term === "number" &&
      isBinaryPayloadArray(message.payload)
    );
  }
  if (message.type === "leader-sync") {
    return (
      typeof message.fromLeaderTabId === "string" &&
      typeof message.toTabId === "string" &&
      typeof message.term === "number" &&
      isBinaryPayloadArray(message.payload)
    );
  }
  if (message.type === "follower-close") {
    return (
      typeof message.fromTabId === "string" &&
      typeof message.toLeaderTabId === "string" &&
      typeof message.term === "number"
    );
  }
  return false;
}
__name(isTabSyncMessage, "isTabSyncMessage");
function isLeaderDebugEnabled() {
  const globalFlag = globalThis.__JAZZ_LEADER_DEBUG__;
  if (globalFlag === true) return true;
  try {
    if (typeof localStorage !== "undefined") {
      return localStorage.getItem("jazz:leader-debug") === "1";
    }
  } catch {}
  return false;
}
__name(isLeaderDebugEnabled, "isLeaderDebugEnabled");
var Db = class _Db {
  static {
    __name(this, "Db");
  }
  clients = /* @__PURE__ */ new Map();
  config;
  wasmModule;
  workerBridge = null;
  worker = null;
  bridgeReady = null;
  primaryDbName = null;
  workerDbName = null;
  leaderElection = null;
  leaderElectionUnsubscribe = null;
  tabRole = "follower";
  tabId = null;
  currentLeaderTabId = null;
  currentLeaderTerm = 0;
  syncChannel = null;
  leaderPeerIds = /* @__PURE__ */ new Set();
  activeRemoteLeaderTabId = null;
  workerReconfigure = Promise.resolve();
  isShuttingDown = false;
  lifecycleHooksAttached = false;
  activeQuerySubscriptionTraces = /* @__PURE__ */ new Map();
  activeQuerySubscriptionTraceListeners = /* @__PURE__ */ new Set();
  nextActiveQuerySubscriptionTraceId = 1;
  onSyncChannelMessage = /* @__PURE__ */ __name((event) => {
    this.handleSyncChannelMessage(event.data);
  }, "onSyncChannelMessage");
  onVisibilityChange = /* @__PURE__ */ __name(() => {
    if (typeof document === "undefined") return;
    const hidden = document.visibilityState === "hidden";
    this.sendLifecycleHint(hidden ? "visibility-hidden" : "visibility-visible");
  }, "onVisibilityChange");
  onPageHide = /* @__PURE__ */ __name(() => {
    this.sendLifecycleHint("pagehide");
  }, "onPageHide");
  onPageFreeze = /* @__PURE__ */ __name(() => {
    this.sendLifecycleHint("freeze");
  }, "onPageFreeze");
  onPageResume = /* @__PURE__ */ __name(() => {
    this.sendLifecycleHint("resume");
  }, "onPageResume");
  /**
   * Protected constructor - use createDb() in regular app code.
   */
  constructor(config, wasmModule2) {
    this.config = config;
    this.wasmModule = wasmModule2;
  }
  /**
   * Create a Db instance with pre-loaded WASM module.
   * @internal Use createDb() instead.
   */
  static async create(config) {
    const wasmModule2 = await loadWasmModule(config.runtime);
    return new _Db(config, wasmModule2);
  }
  /**
   * Create a Db instance backed by a dedicated worker with OPFS persistence.
   *
   * The main thread runs an in-memory WASM runtime.
   * The worker runs a persistent WASM runtime (OPFS).
   * WorkerBridge wires them together via postMessage.
   *
   * @internal Use createDb() instead — it auto-detects browser.
   */
  static async createWithWorker(config) {
    const wasmModule2 = await loadWasmModule(config.runtime);
    const db = new _Db(config, wasmModule2);
    const persistentDriver = resolveStorageDriver(config.driver);
    if (persistentDriver.type !== "persistent") {
      throw new Error("Worker-backed Db requires driver.type='persistent'");
    }
    db.primaryDbName = persistentDriver.dbName ?? config.appId;
    db.workerDbName = db.primaryDbName;
    try {
      const election = new TabLeaderElection({
        appId: config.appId,
        dbName: db.primaryDbName,
      });
      db.leaderElection = election;
      election.start();
      let initialLeader = null;
      try {
        initialLeader = await election.waitForInitialLeader(1600);
      } catch {
        initialLeader = election.snapshot();
      }
      db.adoptLeaderSnapshot(initialLeader);
      db.workerDbName = _Db.resolveWorkerDbNameForSnapshot(db.primaryDbName, initialLeader);
      db.logLeaderDebug("initial-election");
      db.openSyncChannel();
      db.attachLifecycleHooks();
      db.leaderElectionUnsubscribe = election.onChange((snapshot) => {
        db.onLeaderElectionChange(snapshot);
      });
      db.worker = await _Db.spawnWorker(config.runtime);
      return db;
    } catch (error) {
      db.closeSyncChannel();
      db.detachLifecycleHooks();
      if (db.leaderElectionUnsubscribe) {
        db.leaderElectionUnsubscribe();
        db.leaderElectionUnsubscribe = null;
      }
      if (db.leaderElection) {
        db.leaderElection.stop();
        db.leaderElection = null;
      }
      throw error;
    }
  }
  /**
   * Get or create a JazzClient for the given schema.
   * Synchronous because WASM module is pre-loaded.
   *
   * In worker mode, the first call per schema also initializes the
   * WorkerBridge (async). Subsequent calls are sync.
   */
  getClient(schema2) {
    if (!this.wasmModule) {
      throw new Error("Db runtime module is not initialized for this Db implementation");
    }
    const key = serializeRuntimeSchema(schema2);
    if (!this.clients.has(key)) {
      setGlobalWasmLogLevel(this.config.logLevel);
      const client = JazzClient.connectSync(
        this.wasmModule,
        {
          appId: this.config.appId,
          schema: schema2,
          driver: this.config.driver,
          // In worker mode, don't connect to server directly — worker handles it
          serverUrl: this.worker ? void 0 : this.config.serverUrl,
          serverPathPrefix: this.worker ? void 0 : this.config.serverPathPrefix,
          env: this.config.env,
          userBranch: this.config.userBranch,
          jwtToken: this.config.jwtToken,
          localAuthMode: this.config.localAuthMode,
          localAuthToken: this.config.localAuthToken,
          adminSecret: this.config.adminSecret,
          tier: this.worker ? void 0 : "worker",
          // Keep worker-bridged browser clients on worker durability by default.
          // For direct (non-worker) clients connected to a server, default to edge.
          defaultDurabilityTier: this.worker ? void 0 : this.config.serverUrl ? "edge" : void 0,
        },
        {
          // Worker-bridged runtimes exchange postcard payloads with peers;
          // direct browser/server routing keeps JSON payloads.
          useBinaryEncoding: this.worker !== null,
        },
      );
      if (this.worker && !this.workerBridge) {
        this.attachWorkerBridge(key, client);
      }
      this.clients.set(key, client);
    }
    return this.clients.get(key);
  }
  /**
   * Wait for the worker bridge to be initialized (if in worker mode).
   * No-op if not using a worker.
   */
  async ensureBridgeReady() {
    await this.workerReconfigure;
    if (this.bridgeReady) {
      await this.bridgeReady;
    }
  }
  attachWorkerBridge(schemaJson, client) {
    if (!this.worker) {
      throw new Error("Cannot attach worker bridge without an active worker");
    }
    const bridge = new WorkerBridge(this.worker, client.getRuntime());
    this.leaderPeerIds.clear();
    bridge.onPeerSync((batch) => {
      this.handleWorkerPeerSync(batch);
    });
    this.applyBridgeRoutingForCurrentLeader(bridge, false);
    this.workerBridge = bridge;
    this.bridgeReady = bridge.init(this.buildWorkerBridgeOptions(schemaJson)).then(() => void 0);
  }
  buildWorkerBridgeOptions(schemaJson) {
    const driver = resolveStorageDriver(this.config.driver);
    if (driver.type !== "persistent") {
      throw new Error("Worker bridge is only available for driver.type='persistent'");
    }
    return {
      schemaJson,
      appId: this.config.appId,
      env: this.config.env ?? "dev",
      userBranch: this.config.userBranch ?? "main",
      dbName: this.workerDbName ?? driver.dbName ?? this.config.appId,
      serverUrl: this.config.serverUrl,
      serverPathPrefix: this.config.serverPathPrefix,
      jwtToken: this.config.jwtToken,
      localAuthMode: this.config.localAuthMode,
      localAuthToken: this.config.localAuthToken,
      adminSecret: this.config.adminSecret,
      runtime: this.config.runtime,
      logLevel: this.config.logLevel,
    };
  }
  adoptLeaderSnapshot(snapshot) {
    this.tabRole = snapshot.role;
    this.tabId = snapshot.tabId;
    this.currentLeaderTabId = snapshot.leaderTabId;
    this.currentLeaderTerm = snapshot.term;
  }
  openSyncChannel() {
    if (this.syncChannel || !this.primaryDbName) return;
    const ChannelCtor = resolveBroadcastChannelCtor2();
    if (!ChannelCtor) {
      this.logLeaderDebug("sync-channel-unavailable");
      return;
    }
    const channelName = `jazz-tab-sync:${this.config.appId}:${this.primaryDbName}`;
    this.syncChannel = new ChannelCtor(channelName);
    this.syncChannel.addEventListener("message", this.onSyncChannelMessage);
    this.logLeaderDebug("sync-channel-open", {
      channelName,
    });
  }
  closeSyncChannel() {
    if (!this.syncChannel) return;
    this.syncChannel.removeEventListener("message", this.onSyncChannelMessage);
    this.syncChannel.close();
    this.syncChannel = null;
    this.logLeaderDebug("sync-channel-close");
  }
  postSyncChannelMessage(message) {
    this.syncChannel?.postMessage(message);
  }
  attachLifecycleHooks() {
    if (this.lifecycleHooksAttached) return;
    if (typeof window === "undefined" || typeof document === "undefined") return;
    document.addEventListener("visibilitychange", this.onVisibilityChange);
    window.addEventListener("pagehide", this.onPageHide);
    document.addEventListener("freeze", this.onPageFreeze);
    document.addEventListener("resume", this.onPageResume);
    this.lifecycleHooksAttached = true;
  }
  detachLifecycleHooks() {
    if (!this.lifecycleHooksAttached) return;
    if (typeof window === "undefined" || typeof document === "undefined") return;
    document.removeEventListener("visibilitychange", this.onVisibilityChange);
    window.removeEventListener("pagehide", this.onPageHide);
    document.removeEventListener("freeze", this.onPageFreeze);
    document.removeEventListener("resume", this.onPageResume);
    this.lifecycleHooksAttached = false;
  }
  sendLifecycleHint(event) {
    if (this.isShuttingDown || !this.worker) return;
    this.logLeaderDebug("lifecycle-hint", { event });
    if (this.workerBridge) {
      this.workerBridge.sendLifecycleHint(event);
      return;
    }
    this.worker.postMessage({
      type: "lifecycle-hint",
      event,
      sentAtMs: Date.now(),
    });
  }
  logLeaderDebug(event, extra) {
    if (!isLeaderDebugEnabled()) return;
    console.info("[db:leader]", event, {
      tabId: this.tabId,
      role: this.tabRole,
      term: this.currentLeaderTerm,
      leaderTabId: this.currentLeaderTabId,
      workerDbName: this.workerDbName,
      ...extra,
    });
  }
  handleSyncChannelMessage(raw) {
    if (this.isShuttingDown || !this.tabId) return;
    if (!isTabSyncMessage(raw)) return;
    switch (raw.type) {
      case "follower-sync":
        this.handleFollowerSync(raw);
        return;
      case "leader-sync":
        this.handleLeaderSync(raw);
        return;
      case "follower-close":
        this.handleFollowerClose(raw);
        return;
    }
  }
  handleFollowerSync(message) {
    if (this.tabRole !== "leader") return;
    if (!this.workerBridge) return;
    if (!this.tabId || message.toLeaderTabId !== this.tabId) return;
    if (message.term !== this.currentLeaderTerm) return;
    if (!this.leaderPeerIds.has(message.fromTabId)) {
      this.leaderPeerIds.add(message.fromTabId);
      this.workerBridge.openPeer(message.fromTabId);
      this.logLeaderDebug("peer-open", {
        peerId: message.fromTabId,
      });
    }
    this.workerBridge.sendPeerSync(message.fromTabId, message.term, message.payload);
  }
  handleLeaderSync(message) {
    if (this.tabRole !== "follower") return;
    if (!this.workerBridge) return;
    if (!this.tabId || message.toTabId !== this.tabId) return;
    if (!this.currentLeaderTabId || message.fromLeaderTabId !== this.currentLeaderTabId) return;
    if (message.term !== this.currentLeaderTerm) return;
    for (const payload of message.payload) {
      this.workerBridge.applyIncomingServerPayload(payload);
    }
  }
  handleFollowerClose(message) {
    if (this.tabRole !== "leader") return;
    if (!this.workerBridge) return;
    if (!this.tabId || message.toLeaderTabId !== this.tabId) return;
    if (message.term !== this.currentLeaderTerm) return;
    if (!this.leaderPeerIds.has(message.fromTabId)) return;
    this.leaderPeerIds.delete(message.fromTabId);
    this.workerBridge.closePeer(message.fromTabId);
    this.logLeaderDebug("peer-close", {
      peerId: message.fromTabId,
    });
  }
  handleWorkerPeerSync(batch) {
    if (this.isShuttingDown) return;
    if (this.tabRole !== "leader") return;
    if (!this.tabId) return;
    if (batch.term !== this.currentLeaderTerm) return;
    this.postSyncChannelMessage({
      type: "leader-sync",
      fromLeaderTabId: this.tabId,
      toTabId: batch.peerId,
      term: batch.term,
      payload: batch.payload,
    });
  }
  sendFollowerClose(leaderTabId, term) {
    if (!leaderTabId || !this.tabId) return;
    if (leaderTabId === this.tabId) return;
    this.logLeaderDebug("follower-close", {
      toLeaderTabId: leaderTabId,
      closeTerm: term,
    });
    this.postSyncChannelMessage({
      type: "follower-close",
      fromTabId: this.tabId,
      toLeaderTabId: leaderTabId,
      term,
    });
  }
  applyBridgeRoutingForCurrentLeader(bridge, replayConnection) {
    if (this.tabRole === "leader") {
      bridge.setServerPayloadForwarder(null);
      this.activeRemoteLeaderTabId = null;
      this.logLeaderDebug("upstream-mode", {
        mode: "leader-direct",
      });
    } else {
      bridge.setServerPayloadForwarder((payload) => {
        if (!this.tabId || !this.currentLeaderTabId) return;
        if (this.currentLeaderTabId === this.tabId) return;
        this.postSyncChannelMessage({
          type: "follower-sync",
          fromTabId: this.tabId,
          toLeaderTabId: this.currentLeaderTabId,
          term: this.currentLeaderTerm,
          payload: [payload],
        });
      });
      this.activeRemoteLeaderTabId = this.currentLeaderTabId;
      this.logLeaderDebug("upstream-mode", {
        mode: "follower-via-leader",
        upstreamLeaderTabId: this.currentLeaderTabId,
      });
    }
    if (replayConnection) {
      bridge.replayServerConnection();
      this.logLeaderDebug("upstream-replay");
    }
  }
  onLeaderElectionChange(snapshot) {
    if (this.isShuttingDown || !this.primaryDbName) return;
    const previousRole = this.tabRole;
    const previousLeaderTabId = this.currentLeaderTabId;
    const previousTerm = this.currentLeaderTerm;
    this.adoptLeaderSnapshot(snapshot);
    this.logLeaderDebug("leader-change", {
      previousRole,
      previousLeaderTabId,
      previousTerm,
    });
    if (previousRole === "follower" && previousLeaderTabId !== this.currentLeaderTabId) {
      this.sendFollowerClose(previousLeaderTabId, previousTerm);
    }
    const nextDbName = _Db.resolveWorkerDbNameForSnapshot(this.primaryDbName, snapshot);
    const dbNameChanged = nextDbName !== this.workerDbName;
    this.workerDbName = nextDbName;
    if (!this.workerBridge) return;
    this.enqueueWorkerReconfigure(async () => {
      if (this.isShuttingDown) return;
      if (dbNameChanged) {
        this.logLeaderDebug("worker-restart", {
          reason: "db-name-change",
        });
        await this.restartWorkerWithCurrentDbName();
        return;
      }
      if (this.workerBridge) {
        this.applyBridgeRoutingForCurrentLeader(this.workerBridge, true);
      }
    });
  }
  enqueueWorkerReconfigure(task) {
    this.workerReconfigure = this.workerReconfigure.then(task).catch((error) => {
      console.error("[db] Worker reconfigure failed:", error);
    });
  }
  async restartWorkerWithCurrentDbName() {
    const currentWorker = this.worker;
    if (!currentWorker) return;
    if (this.bridgeReady) {
      await this.bridgeReady;
    }
    if (this.workerBridge) {
      try {
        await this.workerBridge.shutdown(currentWorker);
      } catch {}
      this.workerBridge = null;
    }
    this.bridgeReady = null;
    currentWorker.terminate();
    this.worker = await _Db.spawnWorker(this.config.runtime);
    const first = this.clients.entries().next();
    if (!first.done) {
      const [schemaJson, client] = first.value;
      this.attachWorkerBridge(schemaJson, client);
      if (this.bridgeReady) {
        await this.bridgeReady;
      }
    }
  }
  currentWorkerNamespace() {
    const driver = resolveStorageDriver(this.config.driver);
    if (driver.type !== "persistent") {
      throw new Error("Worker namespace is only available for driver.type='persistent'");
    }
    return this.workerDbName ?? driver.dbName ?? this.config.appId;
  }
  async shutdownWorkerAndClientsForStorageReset() {
    const currentWorker = this.worker;
    if (this.workerBridge && currentWorker) {
      try {
        await this.workerBridge.shutdown(currentWorker);
      } catch {}
    }
    this.workerBridge = null;
    this.bridgeReady = null;
    for (const client of this.clients.values()) {
      await client.shutdown();
    }
    this.clients.clear();
    this.leaderPeerIds.clear();
    this.activeRemoteLeaderTabId = null;
    if (currentWorker) {
      currentWorker.terminate();
    }
    this.worker = null;
  }
  async removeOpfsNamespaceFile(namespace) {
    const rootDirectory = await navigator.storage.getDirectory();
    const fileName = `${namespace}.opfsbtree`;
    try {
      await rootDirectory.removeEntry(fileName, { recursive: false });
    } catch (error) {
      const name = error?.name;
      if (name === "NotFoundError") {
        return;
      }
      if (name === "NoModificationAllowedError" || name === "InvalidStateError") {
        throw new Error(
          `Failed to delete browser storage for "${namespace}" because OPFS is locked by another tab. Close other tabs and retry.`,
        );
      }
      throw new Error(
        `Failed to delete browser storage for "${namespace}": ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }
  static resolveWorkerDbNameForSnapshot(primaryDbName, snapshot) {
    if (snapshot.role === "leader") return primaryDbName;
    return `${primaryDbName}__fallback__${snapshot.tabId}`;
  }
  static async spawnWorker(runtime) {
    const locationHref = typeof location !== "undefined" ? location.href : void 0;
    const syncInitInput = resolveRuntimeConfigSyncInitInput(runtime);
    const wasmUrl = syncInitInput
      ? null
      : resolveRuntimeConfigWasmUrl(import.meta.url, locationHref, runtime);
    const workerUrl = appendWorkerRuntimeWasmUrl(
      resolveRuntimeConfigWorkerUrl(import.meta.url, locationHref, runtime),
      wasmUrl,
    );
    const worker = new Worker(workerUrl, {
      type: "module",
    });
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("Worker bootstrap timeout")), 15e3);
      const handler = /* @__PURE__ */ __name((event) => {
        if (event.data.type === "ready") {
          clearTimeout(timeout);
          worker.removeEventListener("message", handler);
          resolve();
        } else if (event.data.type === "error") {
          clearTimeout(timeout);
          worker.removeEventListener("message", handler);
          reject(new Error(event.data.message));
        }
      }, "handler");
      worker.addEventListener("message", handler);
      worker.addEventListener("error", (e) => {
        clearTimeout(timeout);
        reject(new Error(`Worker load error: ${e.message}`));
      });
    });
    return worker;
  }
  getConfig() {
    return structuredClone(this.config);
  }
  setDevMode(enabled) {
    this.config.devMode = enabled;
  }
  getActiveQuerySubscriptions() {
    return Array.from(this.activeQuerySubscriptionTraces.values())
      .filter((trace) => trace.visibility === "public")
      .map(({ visibility: _visibility, ...trace }) => cloneActiveQuerySubscriptionTrace(trace));
  }
  onActiveQuerySubscriptionsChange(listener) {
    this.activeQuerySubscriptionTraceListeners.add(listener);
    listener(this.getActiveQuerySubscriptions());
    return () => {
      this.activeQuerySubscriptionTraceListeners.delete(listener);
    };
  }
  /**
   * Insert a new row into a table without waiting for durability.
   *
   * @param table Table proxy from generated app module
   * @param data Init object with column values
   * @returns Inserted row
   */
  insert(table, data) {
    const client = this.getClient(table._schema);
    const values = toInsertRecord(data, table._schema, table._table);
    const row = client.create(table._table, values);
    return transformRow(row, table._schema, table._table);
  }
  /**
   * Insert a new row into a table and wait for durability at the requested tier.
   *
   * @param table Table proxy from generated app module
   * @param data Init object with column values
   * @param options Durability tier
   * @returns Promise resolving to the inserted row
   */
  async insertDurable(table, data, options) {
    const client = this.getClient(table._schema);
    const inputSchema = resolveSchemaWithTable(
      table._schema,
      normalizeRuntimeSchema(client.getSchema()),
      table._table,
    );
    await this.ensureBridgeReady();
    const values = toInsertRecord(data, inputSchema, table._table);
    const row = await client.createDurable(table._table, values, options);
    return transformRow(row, table._schema, table._table);
  }
  /**
   * Update an existing row without waiting for durability.
   */
  update(table, id, data) {
    const client = this.getClient(table._schema);
    const updates = toUpdateRecord(data, table._schema, table._table);
    client.update(id, updates);
  }
  /**
   * Update an existing row and wait for durability at the requested tier.
   */
  async updateDurable(table, id, data, options) {
    const client = this.getClient(table._schema);
    const inputSchema = resolveSchemaWithTable(
      table._schema,
      normalizeRuntimeSchema(client.getSchema()),
      table._table,
    );
    await this.ensureBridgeReady();
    const updates = toUpdateRecord(data, inputSchema, table._table);
    await client.updateDurable(id, updates, options);
  }
  /**
   * Delete a row without waiting for durability.
   */
  delete(table, id) {
    const client = this.getClient(table._schema);
    client.delete(id);
  }
  /**
   * Delete a row and wait for durability at the requested tier.
   */
  async deleteDurable(table, id, options) {
    const client = this.getClient(table._schema);
    await this.ensureBridgeReady();
    await client.deleteDurable(id, options);
  }
  /**
   * Delete browser OPFS storage for this Db's active namespace and reopen a clean worker.
   *
   * This only deletes `${namespace}.opfsbtree` for the current namespace and does not touch
   * localStorage-based auth or synthetic-user state.
   *
   * Behavior:
   * - Browser worker-backed Db only (throws in non-browser/non-worker runtimes)
   * - Leader tab only (throws on follower tabs and asks to close other tabs)
   * - Serializes with worker reconfigure operations
   * - Tears down worker + clients, deletes OPFS file, respawns worker
   * - If file deletion fails, still respawns worker and then rethrows the deletion error
   */
  async deleteClientStorage() {
    if (resolveStorageDriver(this.config.driver).type !== "persistent") {
      throw new Error("deleteClientStorage() is only available when driver.type='persistent'.");
    }
    if (!isBrowser()) {
      console.error(
        "deleteClientStorage() is only available on browser worker-backed Db instances.",
      );
      return;
    }
    const operation = this.workerReconfigure.then(async () => {
      if (this.tabRole !== "leader") {
        console.error(
          "deleteClientStorage() can only run from the leader tab. Close other tabs and retry.",
        );
        return;
      }
      const namespace = this.currentWorkerNamespace();
      if (this.bridgeReady) {
        await this.bridgeReady;
      }
      await this.shutdownWorkerAndClientsForStorageReset();
      let deleteError = null;
      try {
        await this.removeOpfsNamespaceFile(namespace);
      } catch (error) {
        deleteError = error;
      }
      this.worker = await _Db.spawnWorker(this.config.runtime);
      if (deleteError) {
        throw deleteError;
      }
    });
    this.workerReconfigure = operation.then(
      () => void 0,
      () => void 0,
    );
    await operation;
  }
  /**
   * Execute a query and return all matching rows as typed objects.
   *
   * @param query QueryBuilder instance (e.g., app.todos.where({done: false}))
   * @returns Array of typed objects matching the query
   */
  async all(query, options) {
    const client = this.getClient(query._schema);
    const runtimeSchema = normalizeRuntimeSchema(client.getSchema());
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson), query._table);
    const planningSchema = resolveSchemaWithTable(query._schema, runtimeSchema, builtQuery.table);
    const outputTable =
      builtQuery.hops.length > 0
        ? resolveHopOutputTable(planningSchema, builtQuery.table, builtQuery.hops)
        : query._table;
    const outputSchema = resolveSchemaWithTable(query._schema, runtimeSchema, outputTable);
    const rows = await client.query(translateQuery(builderJson, planningSchema), options);
    const outputIncludes = builtQuery.hops.length > 0 ? {} : builtQuery.includes;
    return transformRows(rows, outputSchema, outputTable, outputIncludes, builtQuery.select);
  }
  /**
   * Execute a query and return the first matching row, or null.
   *
   * @param query QueryBuilder instance
   * @param options Optional read durability options
   * @returns First matching typed object, or null if none found
   */
  async one(query, options) {
    const results = await this.all(query, options);
    return results[0] ?? null;
  }
  /**
   * Create a conventional `files` row by chunking a browser Blob into `file_parts`.
   *
   * Expects `app.files` and `app.file_parts` to follow the built-in file-storage conventions.
   */
  async createFileFromBlob(app2, blob, options) {
    return createConventionalFileStorage(this, app2).fromBlob(blob, options);
  }
  /**
   * Create a conventional `files` row by chunking a browser ReadableStream into `file_parts`.
   *
   * Expects `app.files` and `app.file_parts` to follow the built-in file-storage conventions.
   */
  async createFileFromStream(app2, stream, options) {
    return createConventionalFileStorage(this, app2).fromStream(stream, options);
  }
  /**
   * Load a conventional file as a browser ReadableStream by querying the file row first
   * and then reading each referenced `file_parts` row sequentially.
   */
  async loadFileAsStream(app2, fileOrId, options) {
    return createConventionalFileStorage(this, app2).toStream(fileOrId, options);
  }
  /**
   * Load a conventional file as a Blob using the same sequential part-query path as `loadFileAsStream`.
   */
  async loadFileAsBlob(app2, fileOrId, options) {
    return createConventionalFileStorage(this, app2).toBlob(fileOrId, options);
  }
  /**
   * Subscribe to a query and receive updates when results change.
   *
   * The callback receives a SubscriptionDelta with:
   * - `all`: Complete current result set
   * - `delta`: Ordered list of row-level changes
   *
   * @param query QueryBuilder instance
   * @param callback Called with delta whenever results change
   * @returns Unsubscribe function
   *
   * @example
   * ```typescript
   * const unsubscribe = db.subscribeAll(app.todos, (delta) => {
   *   setTodos(delta.all);
   *   for (const change of delta.delta) {
   *     if (change.kind === 0) {
   *       console.log("New row:", change.row);
   *     }
   *   }
   * });
   *
   * // Later: stop receiving updates
   * unsubscribe();
   * ```
   */
  subscribeAll(query, callback, options, session) {
    const manager = new SubscriptionManager();
    const client = this.getClient(query._schema);
    const runtimeSchema = normalizeRuntimeSchema(client.getSchema());
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson), query._table);
    const planningSchema = resolveSchemaWithTable(query._schema, runtimeSchema, builtQuery.table);
    const outputTable =
      builtQuery.hops.length > 0
        ? resolveHopOutputTable(planningSchema, builtQuery.table, builtQuery.hops)
        : query._table;
    const outputSchema = resolveSchemaWithTable(query._schema, runtimeSchema, outputTable);
    const outputIncludes = builtQuery.hops.length > 0 ? {} : builtQuery.includes;
    const wasmQuery = translateQuery(builderJson, planningSchema);
    const transform = /* @__PURE__ */ __name((row) => {
      return transformRow(row, outputSchema, outputTable, outputIncludes, builtQuery.select);
    }, "transform");
    const subId = client.subscribeInternal(
      wasmQuery,
      (delta) => {
        const typedDelta = manager.handleDelta(delta, transform);
        callback(typedDelta);
      },
      session,
      options,
    );
    const traceId = this.registerActiveQuerySubscriptionTrace(wasmQuery, builtQuery.table, options);
    return () => {
      this.unregisterActiveQuerySubscriptionTrace(traceId);
      client.unsubscribe(subId);
      manager.clear();
    };
  }
  /**
   * Shutdown the Db and release all resources.
   * Closes all memoized JazzClient connections and the worker.
   */
  async shutdown() {
    this.isShuttingDown = true;
    this.clearActiveQuerySubscriptionTraces();
    this.logLeaderDebug("shutdown");
    this.sendFollowerClose(this.activeRemoteLeaderTabId, this.currentLeaderTerm);
    this.activeRemoteLeaderTabId = null;
    this.leaderPeerIds.clear();
    this.closeSyncChannel();
    this.detachLifecycleHooks();
    if (this.leaderElectionUnsubscribe) {
      this.leaderElectionUnsubscribe();
      this.leaderElectionUnsubscribe = null;
    }
    if (this.leaderElection) {
      this.leaderElection.stop();
      this.leaderElection = null;
    }
    await this.workerReconfigure;
    await this.ensureBridgeReady();
    if (this.workerBridge && this.worker) {
      await this.workerBridge.shutdown(this.worker);
      this.workerBridge = null;
    }
    for (const client of this.clients.values()) {
      await client.shutdown();
    }
    this.clients.clear();
    if (this.worker) {
      this.worker.terminate();
      this.worker = null;
    }
  }
  notifyActiveQuerySubscriptionTraceListeners() {
    if (this.activeQuerySubscriptionTraceListeners.size === 0) {
      return;
    }
    const snapshot = this.getActiveQuerySubscriptions();
    for (const listener of this.activeQuerySubscriptionTraceListeners) {
      listener(snapshot);
    }
  }
  registerActiveQuerySubscriptionTrace(queryJson, fallbackTable, options) {
    if (!this.config.devMode) {
      return null;
    }
    const resolvedOptions = resolveEffectiveQueryExecutionOptions(this.config, options);
    const payload = this.parseRuntimeQueryTracePayload(queryJson, fallbackTable);
    const traceId = `sub-${this.nextActiveQuerySubscriptionTraceId++}`;
    this.activeQuerySubscriptionTraces.set(traceId, {
      id: traceId,
      query: queryJson,
      table: payload.table,
      branches: payload.branches,
      tier: resolvedOptions.tier,
      propagation: resolvedOptions.propagation,
      createdAt: /* @__PURE__ */ new Date().toISOString(),
      stack: trimSubscriptionTraceStack(new Error().stack),
      visibility: resolvedOptions.visibility ?? "public",
    });
    this.notifyActiveQuerySubscriptionTraceListeners();
    return traceId;
  }
  unregisterActiveQuerySubscriptionTrace(traceId) {
    if (!traceId) {
      return;
    }
    if (!this.activeQuerySubscriptionTraces.delete(traceId)) {
      return;
    }
    this.notifyActiveQuerySubscriptionTraceListeners();
  }
  clearActiveQuerySubscriptionTraces() {
    if (this.activeQuerySubscriptionTraces.size === 0) {
      return;
    }
    this.activeQuerySubscriptionTraces.clear();
    this.notifyActiveQuerySubscriptionTraceListeners();
  }
  parseRuntimeQueryTracePayload(queryJson, fallbackTable) {
    try {
      const parsed = JSON.parse(queryJson);
      const table = typeof parsed.table === "string" ? parsed.table : fallbackTable;
      const branches = Array.isArray(parsed.branches)
        ? parsed.branches.filter((branch) => typeof branch === "string")
        : [];
      return {
        table,
        branches: branches.length > 0 ? branches : [this.config.userBranch ?? "main"],
      };
    } catch {
      return {
        table: fallbackTable,
        branches: [this.config.userBranch ?? "main"],
      };
    }
  }
};
function isBrowser() {
  return typeof Worker !== "undefined" && typeof window !== "undefined";
}
__name(isBrowser, "isBrowser");
async function createDb(config) {
  const resolvedConfig = resolveLocalAuthDefaults(config);
  const driver = resolveStorageDriver(resolvedConfig.driver);
  if (driver.type === "memory" && !resolvedConfig.serverUrl) {
    throw new Error("driver.type='memory' requires serverUrl.");
  }
  if (isBrowser() && driver.type === "persistent") {
    return Db.createWithWorker(resolvedConfig);
  }
  return Db.create(resolvedConfig);
}
__name(createDb, "createDb");

// src/worker.ts
import jazzWasmModule from "./b3840e24ed4dd8598a122c7ceb9b6fb70aa1f9ce-jazz_wasm_bg.wasm";

// ../../packages/jazz-tools/src/dsl.ts
function normalizeEnumVariants(variants) {
  if (variants.length === 0) {
    throw new Error("Enum columns require at least one variant.");
  }
  for (const variant of variants) {
    if (variant.length === 0) {
      throw new Error("Enum variants cannot be empty strings.");
    }
  }
  const unique = new Set(variants);
  if (unique.size !== variants.length) {
    throw new Error("Enum variants must be unique.");
  }
  return [...unique].sort((a, b) => a.localeCompare(b));
}
__name(normalizeEnumVariants, "normalizeEnumVariants");
function isJsonObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
__name(isJsonObject, "isJsonObject");
function normalizeJsonSchema(schema2) {
  const maybeStandard = schema2["~standard"];
  const converter = maybeStandard?.jsonSchema?.input;
  if (typeof converter === "function") {
    const converted = converter({ target: "draft-07" });
    if (!isJsonObject(converted)) {
      throw new Error(
        "JSON schema conversion failed: expected an object from ~standard.jsonSchema.input(...).",
      );
    }
    return converted;
  }
  if (!isJsonObject(schema2)) {
    throw new Error("JSON schema must be an object or implement ~standard.jsonSchema.");
  }
  return schema2;
}
__name(normalizeJsonSchema, "normalizeJsonSchema");
function jsonColumn(schema2) {
  return new JsonBuilder(schema2);
}
__name(jsonColumn, "jsonColumn");
var ScalarBuilder = class {
  constructor(_sqlType) {
    this._sqlType = _sqlType;
  }
  static {
    __name(this, "ScalarBuilder");
  }
  _nullable = false;
  _default = void 0;
  optional() {
    this._nullable = true;
    return this;
  }
  default(value) {
    this._default = value;
    return this;
  }
  _build(name) {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      ...(this._default === void 0 ? {} : { default: this._default }),
    };
  }
  get _references() {
    return void 0;
  }
};
var EnumBuilder = class {
  static {
    __name(this, "EnumBuilder");
  }
  _nullable = false;
  _default = void 0;
  _sqlType;
  constructor(...variants) {
    this._sqlType = { kind: "ENUM", variants: normalizeEnumVariants(variants) };
  }
  optional() {
    this._nullable = true;
    return this;
  }
  default(value) {
    this._default = value;
    return this;
  }
  _build(name) {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      ...(this._default === void 0 ? {} : { default: this._default }),
    };
  }
  get _references() {
    return void 0;
  }
};
var JsonBuilder = class {
  static {
    __name(this, "JsonBuilder");
  }
  _nullable = false;
  _default = void 0;
  _sqlType;
  constructor(schema2) {
    this._sqlType = schema2
      ? { kind: "JSON", schema: normalizeJsonSchema(schema2) }
      : { kind: "JSON" };
  }
  optional() {
    this._nullable = true;
    return this;
  }
  default(value) {
    this._default = value;
    return this;
  }
  _build(name) {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      ...(this._default === void 0 ? {} : { default: this._default }),
    };
  }
  get _references() {
    return void 0;
  }
};
var RefBuilder = class {
  constructor(_targetTable) {
    this._targetTable = _targetTable;
  }
  static {
    __name(this, "RefBuilder");
  }
  _nullable = false;
  _default = void 0;
  optional() {
    this._nullable = true;
    return this;
  }
  default(value) {
    this._default = value;
    return this;
  }
  _build(name) {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      ...(this._default === void 0 ? {} : { default: this._default }),
      references: this._references,
    };
  }
  get _sqlType() {
    return "UUID";
  }
  get _references() {
    return this._targetTable;
  }
};
var ArrayBuilder = class {
  constructor(_element) {
    this._element = _element;
  }
  static {
    __name(this, "ArrayBuilder");
  }
  _nullable = false;
  _default = void 0;
  optional() {
    this._nullable = true;
    return this;
  }
  default(value) {
    this._default = value;
    return this;
  }
  _build(name) {
    return {
      name,
      sqlType: this._sqlType,
      nullable: this._nullable,
      ...(this._default === void 0 ? {} : { default: this._default }),
      references: this._references,
    };
  }
  get _sqlType() {
    return { kind: "ARRAY", element: this._element._sqlType };
  }
  get _references() {
    return this._element._references;
  }
};
function isTypedColumnBuilder(value) {
  return typeof value === "object" && value !== null && "_build" in value && "_sqlType" in value;
}
__name(isTypedColumnBuilder, "isTypedColumnBuilder");
var AddBuilder = class {
  static {
    __name(this, "AddBuilder");
  }
  string(opts) {
    return { _type: "add", sqlType: "TEXT", default: opts.default };
  }
  int(opts) {
    return { _type: "add", sqlType: "INTEGER", default: opts.default };
  }
  timestamp(opts) {
    return { _type: "add", sqlType: "TIMESTAMP", default: opts.default };
  }
  boolean(opts) {
    return { _type: "add", sqlType: "BOOLEAN", default: opts.default };
  }
  float(opts) {
    return { _type: "add", sqlType: "REAL", default: opts.default };
  }
  bytes(opts) {
    return { _type: "add", sqlType: "BYTEA", default: opts.default };
  }
  ref(_targetTable, opts) {
    return { _type: "add", sqlType: "UUID", default: opts.default };
  }
  json(opts) {
    return {
      _type: "add",
      sqlType: opts.schema
        ? { kind: "JSON", schema: normalizeJsonSchema(opts.schema) }
        : { kind: "JSON" },
      default: opts.default,
    };
  }
  enum(...args) {
    const opts = args[args.length - 1];
    const variants = normalizeEnumVariants(args.slice(0, -1));
    return {
      _type: "add",
      sqlType: { kind: "ENUM", variants },
      default: opts.default,
    };
  }
  array(opts) {
    return {
      _type: "add",
      sqlType: {
        kind: "ARRAY",
        element: isTypedColumnBuilder(opts.of) ? opts.of._sqlType : opts.of,
      },
      default: opts.default,
    };
  }
  optional() {
    return this;
  }
};
var DropBuilder = class {
  static {
    __name(this, "DropBuilder");
  }
  string(opts) {
    return { _type: "drop", sqlType: "TEXT", backwardsDefault: opts.backwardsDefault };
  }
  int(opts) {
    return { _type: "drop", sqlType: "INTEGER", backwardsDefault: opts.backwardsDefault };
  }
  timestamp(opts) {
    return { _type: "drop", sqlType: "TIMESTAMP", backwardsDefault: opts.backwardsDefault };
  }
  boolean(opts) {
    return { _type: "drop", sqlType: "BOOLEAN", backwardsDefault: opts.backwardsDefault };
  }
  float(opts) {
    return { _type: "drop", sqlType: "REAL", backwardsDefault: opts.backwardsDefault };
  }
  bytes(opts) {
    return { _type: "drop", sqlType: "BYTEA", backwardsDefault: opts.backwardsDefault };
  }
  ref(_targetTable, opts) {
    return { _type: "drop", sqlType: "UUID", backwardsDefault: opts.backwardsDefault };
  }
  json(opts) {
    return {
      _type: "drop",
      sqlType: opts.schema
        ? { kind: "JSON", schema: normalizeJsonSchema(opts.schema) }
        : { kind: "JSON" },
      backwardsDefault: opts.backwardsDefault,
    };
  }
  enum(...args) {
    const opts = args[args.length - 1];
    const variants = normalizeEnumVariants(args.slice(0, -1));
    return {
      _type: "drop",
      sqlType: { kind: "ENUM", variants },
      backwardsDefault: opts.backwardsDefault,
    };
  }
  array(opts) {
    return {
      _type: "drop",
      sqlType: {
        kind: "ARRAY",
        element: isTypedColumnBuilder(opts.of) ? opts.of._sqlType : opts.of,
      },
      backwardsDefault: opts.backwardsDefault,
    };
  }
  optional() {
    return this;
  }
};
var col = {
  // Schema context
  string: /* @__PURE__ */ __name(() => new ScalarBuilder("TEXT"), "string"),
  boolean: /* @__PURE__ */ __name(() => new ScalarBuilder("BOOLEAN"), "boolean"),
  int: /* @__PURE__ */ __name(() => new ScalarBuilder("INTEGER"), "int"),
  timestamp: /* @__PURE__ */ __name(() => new ScalarBuilder("TIMESTAMP"), "timestamp"),
  float: /* @__PURE__ */ __name(() => new ScalarBuilder("REAL"), "float"),
  bytes: /* @__PURE__ */ __name(() => new ScalarBuilder("BYTEA"), "bytes"),
  json: jsonColumn,
  enum: /* @__PURE__ */ __name((...variants) => new EnumBuilder(...variants), "enum"),
  ref: /* @__PURE__ */ __name((targetTable) => new RefBuilder(targetTable), "ref"),
  array: /* @__PURE__ */ __name((element) => new ArrayBuilder(element), "array"),
  // Migration context
  add: new AddBuilder(),
  drop: new DropBuilder(),
  rename: /* @__PURE__ */ __name((oldName) => ({ _type: "rename", oldName }), "rename"),
  renameFrom: /* @__PURE__ */ __name(
    (oldName) => ({
      _type: "rename",
      oldName,
    }),
    "renameFrom",
  ),
};

// ../../packages/jazz-tools/src/codegen/schema-reader.ts
var map = {
  TEXT: { type: "Text" },
  BOOLEAN: { type: "Boolean" },
  INTEGER: { type: "Integer" },
  REAL: { type: "Double" },
  TIMESTAMP: { type: "Timestamp" },
  UUID: { type: "Uuid" },
  BYTEA: { type: "Bytea" },
};
function sqlTypeToWasm(sqlType) {
  if (typeof sqlType !== "string") {
    if (sqlType.kind === "ENUM") {
      return { type: "Enum", variants: [...sqlType.variants] };
    }
    if (sqlType.kind === "JSON") {
      return {
        type: "Json",
        schema: sqlType.schema,
      };
    }
    return { type: "Array", element: sqlTypeToWasm(sqlType.element) };
  }
  return map[sqlType];
}
__name(sqlTypeToWasm, "sqlTypeToWasm");
function literalToWasmValue(value) {
  if (value instanceof Uint8Array) {
    return { type: "Bytea", value };
  }
  if (value === null) {
    return { type: "Null" };
  }
  if (typeof value === "string") {
    return { type: "Text", value };
  }
  if (typeof value === "boolean") {
    return { type: "Boolean", value };
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value) || !Number.isInteger(value)) {
      throw new Error("Policy literal numbers must be finite integers");
    }
    if (value >= -2147483648 && value <= 2147483647) {
      return { type: "Integer", value };
    }
    return { type: "BigInt", value };
  }
  if (Array.isArray(value)) {
    return {
      type: "Array",
      value: value.map((inner) => literalToWasmValue(inner)),
    };
  }
  throw new Error(`Unsupported policy literal type: ${typeof value}`);
}
__name(literalToWasmValue, "literalToWasmValue");
function clonePolicyValue(value) {
  if (value.type === "SessionRef") {
    return { type: "SessionRef", path: [...value.path] };
  }
  return { type: "Literal", value: literalToWasmValue(value.value) };
}
__name(clonePolicyValue, "clonePolicyValue");
function clonePolicyLiteralValue(value) {
  return literalToWasmValue(value.value);
}
__name(clonePolicyLiteralValue, "clonePolicyLiteralValue");
function clonePolicyExpr(expr) {
  switch (expr.type) {
    case "Cmp":
      return {
        type: "Cmp",
        column: expr.column,
        op: expr.op,
        value: clonePolicyValue(expr.value),
      };
    case "SessionCmp":
      return {
        type: "SessionCmp",
        path: [...expr.path],
        op: expr.op,
        value: clonePolicyLiteralValue(expr.value),
      };
    case "IsNull":
      return { type: "IsNull", column: expr.column };
    case "SessionIsNull":
      return { type: "SessionIsNull", path: [...expr.path] };
    case "IsNotNull":
      return { type: "IsNotNull", column: expr.column };
    case "SessionIsNotNull":
      return { type: "SessionIsNotNull", path: [...expr.path] };
    case "Contains":
      return {
        type: "Contains",
        column: expr.column,
        value: clonePolicyValue(expr.value),
      };
    case "SessionContains":
      return {
        type: "SessionContains",
        path: [...expr.path],
        value: clonePolicyLiteralValue(expr.value),
      };
    case "In":
      return {
        type: "In",
        column: expr.column,
        session_path: [...expr.session_path],
      };
    case "InList":
      return {
        type: "InList",
        column: expr.column,
        values: expr.values.map(clonePolicyValue),
      };
    case "SessionInList":
      return {
        type: "SessionInList",
        path: [...expr.path],
        values: expr.values.map(clonePolicyLiteralValue),
      };
    case "Exists":
      return {
        type: "Exists",
        table: expr.table,
        condition: clonePolicyExpr(expr.condition),
      };
    case "ExistsRel":
      throw new Error(
        "Policy ExistsRel is not supported in schemaToWasm(). Use definePermissions() relation IR path instead.",
      );
    case "Inherits":
      return {
        type: "Inherits",
        operation: expr.operation,
        via_column: expr.via_column,
        ...(expr.max_depth === void 0 ? {} : { max_depth: expr.max_depth }),
      };
    case "InheritsReferencing":
      return {
        type: "InheritsReferencing",
        operation: expr.operation,
        source_table: expr.source_table,
        via_column: expr.via_column,
        ...(expr.max_depth === void 0 ? {} : { max_depth: expr.max_depth }),
      };
    case "And":
      return { type: "And", exprs: expr.exprs.map(clonePolicyExpr) };
    case "Or":
      return { type: "Or", exprs: expr.exprs.map(clonePolicyExpr) };
    case "Not":
      return { type: "Not", expr: clonePolicyExpr(expr.expr) };
    case "True":
      return { type: "True" };
    case "False":
      return { type: "False" };
  }
}
__name(clonePolicyExpr, "clonePolicyExpr");
function cloneOperationPolicy(policy) {
  const out = {};
  if (!policy) {
    return out;
  }
  if (policy.using) {
    out.using = clonePolicyExpr(policy.using);
  }
  if (policy.with_check) {
    out.with_check = clonePolicyExpr(policy.with_check);
  }
  return out;
}
__name(cloneOperationPolicy, "cloneOperationPolicy");
function clonePolicies(policies) {
  return {
    select: cloneOperationPolicy(policies.select),
    insert: cloneOperationPolicy(policies.insert),
    update: cloneOperationPolicy(policies.update),
    delete: cloneOperationPolicy(policies.delete),
  };
}
__name(clonePolicies, "clonePolicies");
function schemaToWasm(schema2) {
  const tables = {};
  for (const table of schema2.tables) {
    const columns = table.columns.map((col2) => {
      const columnType = sqlTypeToWasm(col2.sqlType);
      const descriptor = {
        name: col2.name,
        column_type: columnType,
        nullable: col2.nullable,
      };
      if (col2.default !== void 0) {
        descriptor.default = toValue(col2.default, columnType);
      }
      if (col2.references) {
        descriptor.references = col2.references;
      }
      return descriptor;
    });
    tables[table.name] = {
      columns,
      policies: table.policies ? clonePolicies(table.policies) : void 0,
    };
  }
  return tables;
}
__name(schemaToWasm, "schemaToWasm");

// ../../packages/jazz-tools/src/typed-app.ts
var DefinedTable = class _DefinedTable {
  constructor(columns, indexes = []) {
    this.columns = columns;
    this.indexes = indexes;
  }
  static {
    __name(this, "DefinedTable");
  }
  __jazzTableDefinition = true;
  index(name, columns) {
    const normalizedName = name.trim();
    if (!normalizedName) {
      throw new Error("table.index(...) requires a non-empty index name.");
    }
    const normalizedColumns = [...columns];
    for (const column of normalizedColumns) {
      if (!(column in this.columns)) {
        throw new Error(`table.index(...) references unknown column "${column}".`);
      }
    }
    return new _DefinedTable(this.columns, [
      ...this.indexes,
      {
        name: normalizedName,
        columns: normalizedColumns,
      },
    ]);
  }
};
function defineTable(columns) {
  return new DefinedTable(columns);
}
__name(defineTable, "defineTable");
var TypedTableQueryBuilder = class _TypedTableQueryBuilder {
  static {
    __name(this, "TypedTableQueryBuilder");
  }
  _table;
  _schema;
  _conditions = [];
  _includes = {};
  _requireIncludes = false;
  _selectColumns;
  _orderBys = [];
  _limitVal;
  _offsetVal;
  _hops = [];
  _gatherVal;
  constructor(table, schema2) {
    this._table = table;
    this._schema = schema2;
  }
  where(conditions) {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === void 0) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== void 0) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }
  select(...columns) {
    const clone = this._clone();
    clone._selectColumns = [...columns];
    return clone;
  }
  include(relations) {
    const clone = this._clone();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }
  requireIncludes() {
    const clone = this._clone();
    clone._requireIncludes = true;
    return clone;
  }
  orderBy(column, direction = "asc") {
    const clone = this._clone();
    clone._orderBys.push([column, direction]);
    return clone;
  }
  limit(n) {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }
  offset(n) {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }
  hopTo(relation) {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }
  gather(options) {
    if (options.start === void 0) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }
    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }
    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof stepOutput._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }
    const stepBuilt = JSON.parse(stepOutput._build());
    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }
    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop) => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }
    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }
    const currentCondition = currentConditions[0];
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );
    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };
    return clone;
  }
  _build() {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || void 0,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }
  toJSON() {
    return JSON.parse(this._build());
  }
  _clone() {
    const clone = new _TypedTableQueryBuilder(this._table, this._schema);
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : void 0;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : void 0;
    return clone;
  }
};
function unwrapTableDefinition(definition) {
  if (definition instanceof DefinedTable) {
    return definition.columns;
  }
  if (typeof definition === "object" && definition !== null) {
    const maybeDefinedTable = definition;
    if (maybeDefinedTable.__jazzTableDefinition === true && maybeDefinedTable.columns) {
      return maybeDefinedTable.columns;
    }
  }
  return definition;
}
__name(unwrapTableDefinition, "unwrapTableDefinition");
function definitionToColumns(definition) {
  const columnsDefinition = unwrapTableDefinition(definition);
  const columns = [];
  for (const [columnName, builder] of Object.entries(columnsDefinition)) {
    assertUserColumnNameAllowed(columnName);
    columns.push(builder._build(columnName));
  }
  return columns;
}
__name(definitionToColumns, "definitionToColumns");
function definitionToSchema(definition) {
  return {
    tables: Object.entries(definition).map(([tableName, tableDefinition]) => ({
      name: tableName,
      columns: definitionToColumns(tableDefinition),
    })),
  };
}
__name(definitionToSchema, "definitionToSchema");
function defineApp(definition) {
  const normalizedDefinition = definition;
  const schema2 = definitionToSchema(normalizedDefinition);
  const wasmSchema = schemaToWasm(schema2);
  const tables = {};
  for (const tableName of Object.keys(normalizedDefinition)) {
    tables[tableName] = new TypedTableQueryBuilder(tableName, wasmSchema);
  }
  return {
    ...tables,
    wasmSchema,
  };
}
__name(defineApp, "defineApp");
var permissionIntrospectionColumns = [...PERMISSION_INTROSPECTION_COLUMNS];
var provenanceMagicColumns = [...PROVENANCE_MAGIC_COLUMNS];

// src/schema.ts
var schema = {
  todos: defineTable({
    title: col.string(),
    done: col.boolean(),
  }),
};
var app = defineApp(schema);

// src/worker.ts
var APP_ID = "cloudflare-worker-runtime-ts";
var LOCAL_AUTH_TOKEN = "cloudflare-worker-runtime-ts";
var dbPromise = null;
function json(body, status = 200) {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
    },
  });
}
__name(json, "json");
function getDb() {
  if (dbPromise) {
    return dbPromise;
  }
  dbPromise = createDb({
    appId: APP_ID,
    env: "dev",
    userBranch: "main",
    localAuthMode: "anonymous",
    localAuthToken: LOCAL_AUTH_TOKEN,
    runtime: {
      wasmModule: jazzWasmModule,
    },
  });
  return dbPromise;
}
__name(getDb, "getDb");
async function listTodos(db) {
  return db.all(app.todos);
}
__name(listTodos, "listTodos");
async function handleSmoke(db) {
  const title = `workerd-${crypto.randomUUID().slice(0, 8)}`;
  const inserted = db.insert(app.todos, {
    title,
    done: false,
  });
  const todos = await listTodos(db);
  return json({
    ok: true,
    runtime: "cloudflare-workers",
    wasmInit: "runtime.wasmModule",
    insertedId: inserted.id,
    todoCount: todos.length,
    todos,
  });
}
__name(handleSmoke, "handleSmoke");
var worker_default = {
  async fetch(request) {
    const url = new URL(request.url);
    const db = await getDb();
    if (url.pathname === "/") {
      return json({
        ok: true,
        example: "cloudflare-worker-runtime-ts",
        verify: {
          smoke: "GET /smoke",
          todos: "GET /todos",
        },
      });
    }
    if (url.pathname === "/smoke") {
      return handleSmoke(db);
    }
    if (url.pathname === "/todos") {
      const todos = await listTodos(db);
      return json({
        ok: true,
        todoCount: todos.length,
        todos,
      });
    }
    return json(
      {
        ok: false,
        error: "Not found",
      },
      404,
    );
  },
};
export { worker_default as default };
//# sourceMappingURL=worker.js.map
