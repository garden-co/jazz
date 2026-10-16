/* groove::storage::key_codec (crates/groove/src/storage/key_codec.rs) */
#include "jz_rt.h"
#include "mod.h"

#define KEY_VERSION 1u

jz_err groove__storage__key_codec__encode_column_family_key(jz_slice cf, jz_slice key, jz_bytes *out) {
    JZ_TRY(groove__storage__validate_physical_storage_name(cf));
    jz_bytes encoded = {0};
    jz_vec_reserve(&encoded, 1 + 2 + cf.len + key.len, 1);
    jz_bytes_push_u8(&encoded, KEY_VERSION);
    u8 be[2] = {(u8)(cf.len >> 8), (u8)cf.len};
    jz_bytes_push(&encoded, be, 2);
    jz_bytes_push(&encoded, cf.ptr, cf.len);
    jz_bytes_push(&encoded, key.ptr, key.len);
    *out = encoded;
    return JZ_OK;
}

jz_err groove__storage__key_codec__decode_column_family_key(jz_slice encoded, jz_slice *cf, jz_slice *key) {
    if (encoded.len < 3 || encoded.ptr[0] != KEY_VERSION) {
        return groove__storage__Error__invalid_storage_key("unsupported IndexedDB storage key");
    }
    u32 cf_len = ((u32)encoded.ptr[1] << 8) | encoded.ptr[2];
    u32 key_offset = 3 + cf_len;
    if (encoded.len < key_offset) {
        return groove__storage__Error__invalid_storage_key("truncated IndexedDB storage key");
    }
    if (!jz_utf8_valid(encoded.ptr + 3, cf_len)) {
        return groove__storage__Error__invalid_storage_key("invalid UTF-8 column family name");
    }
    *cf = jz_slice_of(encoded.ptr + 3, cf_len);
    *key = jz_slice_of(encoded.ptr + key_offset, encoded.len - key_offset);
    return JZ_OK;
}

bool groove__storage__key_codec__increment_bytes(jz_slice bytes, jz_bytes *out) {
    jz_bytes next = jz_bytes_from(bytes.ptr, bytes.len);
    for (u32 index = next.len; index-- > 0;) {
        if (next.ptr[index] != 0xff) {
            next.ptr[index] += 1;
            next.len = index + 1; /* truncate */
            *out = next;
            return true;
        }
    }
    jz_bytes_drop(&next);
    return false;
}

bool groove__storage__key_codec__prefix_upper_bound(jz_slice prefix, jz_bytes *out) {
    return groove__storage__key_codec__increment_bytes(prefix, out);
}
