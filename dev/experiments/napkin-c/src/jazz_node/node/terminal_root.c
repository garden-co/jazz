/* Port of crates/jazz/layers/node/src/node/terminal_root.rs */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

typedef enum { JZN_CARRIER_CURRENT_ROW, JZN_CARRIER_LOGICAL } jazz_node_TerminalRootCarrier;
typedef struct jazz_node_TerminalRootPublicField {
    jazz_engine_CurrentRowPublicationField publication;
    jz_str name;
    jz_str descriptor_field_name;
    usize slot;
    jazz_node_TerminalRootCarrier carrier;
} jazz_node_TerminalRootPublicField;
typedef struct jazz_node_TerminalRootLayout {
    jz_str id;
    groove_RecordDescriptor *root_descriptor;
    usize root_key_slot;
    jz_str root_key_field_name;
    JZ_VEC(jazz_node_TerminalRootPublicField) public_fields;
    jazz_node_TerminalRootCarrier carrier;
    bool root_union_arm;
} jazz_node_TerminalRootLayout;

bool jazz_engine__query_engine__publication__CurrentRowPublicationField__public_name(const jazz_engine_CurrentRowPublicationField *f, jz_slice *out);

void jazz_node__node__terminal_root__terminal_root_publication_fields(const jazz_node_TerminalRootLayout *layout, jazz_engine_CurrentRowPublicationFieldVec *out) {
    const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(layout->root_descriptor);
    for (u32 i = 0; i < fields->len; i++) {
        if (!fields->ptr[i].has_name) jz_panic("terminal fields are named");
        jazz_engine_CurrentRowPublicationField f = { JAZZ_PUBF_RESULT_FIELD, 0, jz_str_clone(&fields->ptr[i].name), JAZZ_VIS_HIDDEN_METADATA };
        JZ_VEC_PUSH(*out, f);
    }
    for (u32 i = 0; i < layout->public_fields.len; i++) {
        const jazz_node_TerminalRootPublicField *field = &layout->public_fields.ptr[i];
        jz_slice pn;
        bool has = jazz_engine__query_engine__publication__CurrentRowPublicationField__public_name(&field->publication, &pn);
        if (!has || pn.len != field->name.len || memcmp(pn.ptr, field->name.ptr, pn.len) != 0)
            jz_panic("terminal publication name must match its public slot mapping");
        jazz_engine__query_engine__publication__CurrentRowPublicationField__drop(&out->ptr[field->slot]);
        jazz_engine__query_engine__publication__CurrentRowPublicationField__clone(&out->ptr[field->slot], &field->publication);
    }
}

/* ObjectId = Intern<Uuid>, OutputOccurrenceId opaque (jazz_types::object) */
typedef const jz_uuid *jazz_types_ObjectId;
typedef struct jazz_types_OutputOccurrenceId jazz_types_OutputOccurrenceId;
jazz_types_ObjectId jazz_types__object__ObjectId__from_uuid(jz_uuid u);
typedef struct { usize pos; jz_str label; } union_arm;
typedef JZ_VEC(union_arm) union_arm_vec;
typedef JZ_VEC(jazz_types_ObjectId) object_id_vec;
jazz_types_OutputOccurrenceId *jazz_types__object__OutputOccurrenceId__new(jazz_types_ObjectId root, object_id_vec joined);
jazz_types_OutputOccurrenceId *jazz_types__object__OutputOccurrenceId__with_union_arms(jazz_types_ObjectId root, object_id_vec joined, union_arm_vec arms);

static bool uuid_at(jz_slice enc, usize *cursor, jazz_types_ObjectId *out) {
    if (*cursor >= enc.len || enc.ptr[*cursor] != 10) return false;
    usize start = *cursor + 1, end = start + 16;
    if (end > enc.len) return false;
    jz_uuid u; memcpy(u.b, enc.ptr + start, 16);
    *cursor = end;
    *out = jazz_types__object__ObjectId__from_uuid(u);
    return true;
}

static bool ordered_string_at(jz_slice enc, usize *cursor, jz_str *out) {
    if (*cursor >= enc.len || enc.ptr[*cursor] != 6) return false;
    *cursor += 1;
    jz_bytes decoded = {0};
    for (;;) {
        if (*cursor >= enc.len) { jz_bytes_drop(&decoded); return false; }
        u8 byte = enc.ptr[(*cursor)++];
        if (byte != 0) { jz_bytes_push_u8(&decoded, byte); continue; }
        if (*cursor >= enc.len) { jz_bytes_drop(&decoded); return false; }
        u8 next = enc.ptr[*cursor];
        if (next == 0) { *cursor += 1; break; }
        if (next == 0xff) { *cursor += 1; jz_bytes_push_u8(&decoded, 0); continue; }
        jz_bytes_drop(&decoded);
        return false;
    }
    if (!jz_utf8_valid(decoded.ptr, decoded.len)) { jz_bytes_drop(&decoded); return false; }
    out->ptr = (char *)decoded.ptr; out->len = decoded.len; out->cap = decoded.cap;
    return true;
}

static jz_err proto_err(const char *msg) {
    return jazz_node__node__api_error__Error__into_err(JZN_API_PROTOCOL, jz_str_from(msg));
}

jz_err jazz_node__node__terminal_root__terminal_root_occurrence_id_with_root_union(jz_slice encoded, bool root_union_arm,
                                                                                 jazz_types_OutputOccurrenceId **out) {
    usize cursor = 0;
    jazz_types_ObjectId root;
    if (!uuid_at(encoded, &cursor, &root)) return proto_err("terminal root key must begin with a UUID");
    object_id_vec joined = {0};
    union_arm_vec union_arms = {0};
    jz_err e = JZ_OK;
    if (root_union_arm) {
        jz_str label;
        if (!ordered_string_at(encoded, &cursor, &label)) { e = proto_err("terminal root key contains an invalid root union discriminator"); goto fail; }
        jazz_types_ObjectId actual_root;
        if (!uuid_at(encoded, &cursor, &actual_root)) { jz_str_drop(&label); e = proto_err("terminal root key contains no root union contributor"); goto fail; }
        if (!jz_uuid_eq(actual_root, root)) { jz_str_drop(&label); e = proto_err("terminal root key root union contributor disagrees with root UUID"); goto fail; }
        union_arm a = { 0, label };
        JZ_VEC_PUSH(union_arms, a);
    }
    while (cursor < encoded.len) {
        bool has_disc = false; jz_str discriminator;
        if (encoded.ptr[cursor] == 6) {
            if (!ordered_string_at(encoded, &cursor, &discriminator)) { e = proto_err("terminal root key contains an invalid union discriminator"); goto fail; }
            has_disc = true;
        }
        jazz_types_ObjectId joined_id;
        if (!uuid_at(encoded, &cursor, &joined_id)) {
            if (has_disc) jz_str_drop(&discriminator);
            e = proto_err("terminal root key contains an unsupported component");
            goto fail;
        }
        if (has_disc) { union_arm a = { joined.len + 1, discriminator }; JZ_VEC_PUSH(union_arms, a); }
        JZ_VEC_PUSH(joined, joined_id);
    }
    if (union_arms.len == 0) {
        jz_free(union_arms.ptr);
        *out = jazz_types__object__OutputOccurrenceId__new(root, joined);
        return JZ_OK;
    }
    *out = jazz_types__object__OutputOccurrenceId__with_union_arms(root, joined, union_arms);
    if (!*out) return proto_err("terminal root key contains invalid union discriminators");
    return JZ_OK;
fail:
    jz_free(joined.ptr);
    for (u32 i = 0; i < union_arms.len; i++) jz_str_drop(&union_arms.ptr[i].label);
    jz_free(union_arms.ptr);
    return e;
}
