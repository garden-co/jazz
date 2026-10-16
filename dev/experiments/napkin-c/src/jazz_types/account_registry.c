/* crates/jazz/layers/types/src/account_registry.rs (client-reachable part).
 * AccountRegistry / AccountCommand / Assignment / codec.rs / storage.rs are
 * used only by jazz-server and absent from the browser build. */
#include "jazz_types.h"

const jazz_types_AccountId jazz_types__account_registry__SYSTEM_ACCOUNT_ID = {{0}};

bool jazz_types__account_registry__AccountId__is_system(jazz_types_AccountId a) {
    return jz_uuid_eq(&a, &jazz_types__account_registry__SYSTEM_ACCOUNT_ID);
}

/* Deterministic founding account, scoped to the application's registry. */
jazz_types_AccountId jazz_types__account_registry__local_first_account_id(jz_uuid app, jz_slice subject) {
    static const char prefix[] = "jazz-account-founder-v1";
    jz_bytes name = jz_bytes_from((const u8 *)prefix, sizeof prefix); /* includes the trailing \0 */
    jz_bytes_push(&name, subject.ptr, subject.len);
    jazz_types_AccountId id = jz_uuid_v5(&app, name.ptr, name.len);
    jz_bytes_drop(&name);
    return id;
}
