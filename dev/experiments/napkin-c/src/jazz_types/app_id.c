/* crates/jazz/layers/types/src/app_id.rs */
#include "jazz_types.h"

static const jz_uuid UUID_NAMESPACE_DNS = {{0x6b,0xa7,0xb8,0x10,0x9d,0xad,0x11,0xd1,0x80,0xb4,0x00,0xc0,0x4f,0xd4,0x30,0xc8}};

jazz_types_AppId jazz_types__app_id__AppId__random(void) { return jazz_types__object__ObjectId__new(); }

jazz_types_AppId jazz_types__app_id__AppId__from_name(jz_slice name) {
    jz_uuid u = jz_uuid_v5(&UUID_NAMESPACE_DNS, name.ptr, name.len);
    return jazz_types__object__ObjectId__from_uuid(&u);
}

jz_err jazz_types__app_id__AppId__from_string(jz_slice s, jazz_types_AppId *out) {
    jz_uuid u;
    JZ_TRY(jz_uuid_parse(s, &u));
    *out = jazz_types__object__ObjectId__from_uuid(&u);
    return JZ_OK;
}

void jazz_types__app_id__AppId__Display__fmt(jz_str *out, jazz_types_AppId id) {
    char buf[37];
    jz_uuid_to_string(id, buf);
    jz_str_push(out, buf, 36);
}
