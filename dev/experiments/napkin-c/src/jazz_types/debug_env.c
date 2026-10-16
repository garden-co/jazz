/* crates/jazz/layers/types/src/debug_env.rs
 * OnceLock<bool> caches of std::env::var_os(name).is_some(). On wasm32 the
 * environment is empty; the lookup still goes through the (imported) host. */
#include "jazz_types.h"

JZ_IMPORT("env_var_is_set") bool jazz_types__debug_env__host_env_var_is_set(const char *name, u32 len);

typedef struct { bool init; bool value; } jazz_types_OnceBool;

static bool jazz_types__debug_env__cached(jazz_types_OnceBool *slot, const char *name) {
    if (!slot->init) {
        slot->value = jazz_types__debug_env__host_env_var_is_set(name, (u32)strlen(name));
        slot->init = true;
    }
    return slot->value;
}

bool jazz_types__debug_env__covered_input_trace(void) {
    static jazz_types_OnceBool SLOT;
    return jazz_types__debug_env__cached(&SLOT, "JAZZ_COVERED_INPUT_TRACE");
}
bool jazz_types__debug_env__rehydrate_trace(void) {
    static jazz_types_OnceBool SLOT;
    return jazz_types__debug_env__cached(&SLOT, "JAZZ_REHYDRATE_TRACE");
}
bool jazz_types__debug_env__force_singleton_version_carriers(void) {
    static jazz_types_OnceBool SLOT;
    return jazz_types__debug_env__cached(&SLOT, "JAZZ_FORCE_SINGLETON_VERSION_CARRIERS");
}
