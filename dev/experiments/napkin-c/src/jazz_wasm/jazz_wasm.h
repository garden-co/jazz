/* jazz_wasm.h: crates/jazz-wasm (lib.rs, identity.rs) shared declarations */
#ifndef JAZZ_WASM_JAZZ_WASM_H
#define JAZZ_WASM_JAZZ_WASM_H
#include "jz_rt.h"
#include "jazz_wasm/wbg.h"
#include "jazz_wasm/_ext_wasm-bindings.h"
#include "wasm_tracing/wasm_tracing.h"
#include "idb_tree/idb_tree.h"

/* ---- identity.rs ---- */
extern const char jazz_wasm__identity__LOCAL_FIRST_ISSUER[];
extern const char jazz_wasm__identity__ANONYMOUS_ISSUER[];
jz_uuid jazz_wasm__identity__derive_user_id(const u8 seed[32]);
jz_err jazz_wasm__identity__mint_jazz_self_signed_token_at(const u8 seed[32], const char *issuer, jz_slice audience, u64 ttl_seconds,
                                                           u64 now_seconds, jz_str *out);

/* ---- swb.c: serde_wasm_bindgen (from_value / to_value) for the shapes lib.rs moves ---- */
#define SWB_ERR 0xfe10u
jz_err swb__error(const char *fmt, ...);
/* JsValue -> generic serde data model (jz_json) */
jz_err swb__from_value(jz_js v, jz_json **out);
/* typed Deserialize impls over the decoded value */
jz_err swb__TxId__deserialize(const jz_json *v, jazz_model_TxId *out);
jz_err swb__BranchSelector__deserialize(const jz_json *v, jazz_model_BranchSelector *out);
jz_err swb__SnapshotRef__deserialize(const jz_json *v, jazz_model_SnapshotRef *out);
jz_err swb__BranchViewBase__deserialize(const jz_json *v, jazz_model_BranchViewBase *out);
jz_err swb__ReadViewSpec__deserialize(const jz_json *v, jazz_protocol_ReadViewSpec *out);
jz_err swb__LargeValueUpdateVec__deserialize(const jz_json *v, jazz_db_LargeValueUpdateVec *out);
void swb__LargeValueUpdateVec__drop(jazz_db_LargeValueUpdateVec *v);
/* typed Serialize impls */
jz_err swb__WriteState__serialize(const jazz_db_WriteState *s, jz_js *out);
jz_err swb__MutationErrorEvent__serialize(const jazz_db_MutationErrorEvent *e, jz_js *out);
/* convenience: from_value::<T>(js) */
jz_err swb__from_value_as_ReadViewSpec(jz_js v, jazz_protocol_ReadViewSpec *out);
jz_err swb__from_value_as_BranchSelector(jz_js v, jazz_model_BranchSelector *out);
jz_err swb__from_value_as_BranchViewBase(jz_js v, jazz_model_BranchViewBase *out);
jz_err swb__from_value_as_LargeValueUpdateVec(jz_js v, jazz_db_LargeValueUpdateVec *out);
#endif
