/* db/encoding.c: port of crates/groove/src/db/encoding.rs.
 * Record-input resolution, primary-key and durable-index key encoding and
 * decoding for the database facade. */
#include "mod.h"

static jz_err descriptor_mismatch(const groove_TableSchema *table, u32 variant_tag) {
    return groove__db__Error__table_version(GROOVE_DB_SchemaVersionDescriptorMismatch, (const char *)table->name.ptr, table->name.len, variant_tag);
}
static void resolved(groove_ResolvedRecord *out, u32 variant_tag, groove_RecordDescriptor descriptor, jz_bytes record) {
    out->variant_tag = variant_tag;
    out->descriptor = descriptor;
    out->record = record;
}

jz_err groove__db__encoding__resolve_record_input(const groove_TableSchema *table, const groove_RecordInput *input, groove_RecordDescriptor descriptor, groove_ResolvedRecord *out) {
    if (input->tag == GROOVE_RECORD_INPUT_VALUES) {
        jz_bytes record;
        JZ_TRY(groove__db__encoding__encode_record(table, descriptor, input->values.ptr, input->values.len, &record));
        resolved(out, 0, descriptor, record);
        return JZ_OK;
    }
    return groove__db__encoding__resolve_variant_record(table, &input->record, descriptor, out);
}

static jz_err resolve_validated_variant_record(const groove_TableSchema *table, const groove_ValidatedVariantRecord *record, groove_RecordDescriptor descriptor, groove_ResolvedRecord *out) {
    u32 variant_tag = record->variant_tag;
    if (!groove__records__RecordDescriptor__registry_compatible_with(record->record.descriptor, descriptor)) return descriptor_mismatch(table, variant_tag);
    resolved(out, variant_tag, descriptor, jz_bytes_clone(&record->record.raw));
    return JZ_OK;
}
static jz_err resolve_owned_validated_variant_record(const groove_TableSchema *table, groove_ValidatedVariantRecord record, groove_RecordDescriptor descriptor, groove_ResolvedRecord *out) {
    u32 variant_tag = record.variant_tag;
    if (!groove__records__RecordDescriptor__registry_compatible_with(record.record.descriptor, descriptor)) {
        groove__records__VariantRecord__drop(&record);
        return descriptor_mismatch(table, variant_tag);
    }
    resolved(out, variant_tag, descriptor, record.record.raw);
    return JZ_OK;
}
static jz_err resolve_owned_variant_record(const groove_TableSchema *table, groove_VariantRecord record, groove_RecordDescriptor descriptor, groove_ResolvedRecord *out) {
    u32 variant_tag = record.variant_tag;
    if (!groove__records__RecordDescriptor__registry_compatible_with(record.record.descriptor, descriptor)) {
        groove__records__VariantRecord__drop(&record);
        return descriptor_mismatch(table, variant_tag);
    }
    jz_err e = groove__records__VariantRecord__validate_record(&record);
    if (e.code) { groove__records__VariantRecord__drop(&record); return e; }
    resolved(out, variant_tag, descriptor, record.record.raw);
    return JZ_OK;
}

jz_err groove__db__encoding__resolve_raw_record_input(const groove_TableSchema *table, const groove_RawRecordInput *input, groove_RecordDescriptor descriptor, groove_ResolvedRecord *out) {
    switch (input->tag) {
    case GROOVE_RAW_INPUT_PAYLOAD: resolved(out, 0, descriptor, jz_bytes_clone(&input->payload)); return JZ_OK;
    case GROOVE_RAW_INPUT_RECORD: return groove__db__encoding__resolve_variant_record(table, &input->record, descriptor, out);
    default: return resolve_validated_variant_record(table, &input->record, descriptor, out);
    }
}
jz_err groove__db__encoding__resolve_owned_record_input(const groove_TableSchema *table, groove_RecordInput input, groove_RecordDescriptor descriptor, groove_ResolvedRecord *out) {
    if (input.tag == GROOVE_RECORD_INPUT_VALUES) {
        jz_bytes record;
        jz_err e = groove__db__encoding__encode_record(table, descriptor, input.values.ptr, input.values.len, &record);
        groove__records__values__ValueVec__drop(&input.values);
        JZ_TRY(e);
        resolved(out, 0, descriptor, record);
        return JZ_OK;
    }
    return resolve_owned_variant_record(table, input.record, descriptor, out);
}
jz_err groove__db__encoding__resolve_owned_raw_record_input(const groove_TableSchema *table, groove_RawRecordInput input, groove_RecordDescriptor descriptor, groove_ResolvedRecord *out) {
    switch (input.tag) {
    case GROOVE_RAW_INPUT_PAYLOAD: resolved(out, 0, descriptor, input.payload); return JZ_OK;
    case GROOVE_RAW_INPUT_RECORD: return resolve_owned_variant_record(table, input.record, descriptor, out);
    default: return resolve_owned_validated_variant_record(table, input.record, descriptor, out);
    }
}
jz_err groove__db__encoding__resolve_variant_record(const groove_TableSchema *table, const groove_VariantRecord *record, groove_RecordDescriptor descriptor, groove_ResolvedRecord *out) {
    u32 variant_tag = record->variant_tag;
    if (!groove__records__RecordDescriptor__registry_compatible_with(record->record.descriptor, descriptor)) return descriptor_mismatch(table, variant_tag);
    JZ_TRY(groove__records__VariantRecord__validate_record(record));
    resolved(out, variant_tag, descriptor, jz_bytes_clone(&record->record.raw));
    return JZ_OK;
}

jz_err groove__db__encoding__encode_record(const groove_TableSchema *table, groove_RecordDescriptor descriptor, const groove_Value *values, u32 n, jz_bytes *out) {
    u32 ncols = table->columns.len;
    if (ncols != n) return groove__records__Error__arity_mismatch(ncols, n);
    u32 nf;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(descriptor, &nf);
    /* Descriptors retain logical declaration order even though fixed fields
     * come first in the physical byte layout. Ordinary table writes therefore
     * already have the encoder's order and need no cloned staging vector. */
    bool same_order = nf == ncols;
    for (u32 i = 0; same_order && i < nf; i++)
        same_order = fields[i].has_name && jz_str_eq(&fields[i].name, &table->columns.ptr[i].name);
    if (same_order) return groove__records__RecordDescriptor__create(descriptor, values, n, out);
    /* Variant descriptors can select or reorder declaration fields. */
    groove_ValueVec ordered = {0};
    jz_vec_reserve(&ordered, nf, sizeof(groove_Value));
    for (u32 i = 0; i < nf; i++) {
        jz_err e = JZ_OK;
        if (!fields[i].has_name) {
            e = groove__records__Error__field_not_found("<unnamed>", 9);
        } else {
            u32 idx = ncols;
            for (u32 c = 0; c < ncols; c++)
                if (jz_str_eq(&table->columns.ptr[c].name, &fields[i].name)) { idx = c; break; }
            if (idx == ncols) e = groove__records__Error__field_not_found((const char *)fields[i].name.ptr, fields[i].name.len);
            else if (idx >= n) e = groove__records__Error__arity_mismatch(ncols, n);
            else {
                groove_Value v;
                groove__records__values__Value__Clone__clone(&v, &values[idx]);
                JZ_VEC_PUSH(ordered, v);
            }
        }
        if (e.code) { groove__records__values__ValueVec__drop(&ordered); return e; }
    }
    jz_err e = groove__records__RecordDescriptor__create(descriptor, ordered.ptr, ordered.len, out);
    groove__records__values__ValueVec__drop(&ordered);
    return e;
}

jz_err groove__db__encoding__primary_key_bytes(const groove_TableSchema *table, u32 variant_tag, groove_RecordDescriptor record_schema, const u8 *record, u32 n, jz_bytes *out) {
    if (!table->has_primary_key) return groove__db__Error__str(GROOVE_DB_MissingPrimaryKey, (const char *)table->name.ptr, table->name.len);
    const groove_PrimaryKey *primary_key = &table->primary_key;
    jz_bytes bytes = {0};
    for (u32 i = 0; i < primary_key->columns.len; i++) {
        const groove_PrimaryKeyColumn *column = &primary_key->columns.ptr[i];
        const jz_str *local_name;
        if (table->variants.len == 0) {
            local_name = &column->column;
        } else {
            const groove_TableVariant *variant = groove__schema__TableSchema__variant(table, variant_tag);
            local_name = variant ? groove__schema__TableVariant__payload_name_for_shared(variant, &column->column) : NULL;
            if (!local_name) {
                jz_bytes_drop(&bytes);
                return groove__db__Error__table_version_field(GROOVE_DB_SchemaVersionMissingPrimaryKey, (const char *)table->name.ptr, table->name.len, variant_tag,
                                                              (const char *)column->column.ptr, column->column.len);
            }
        }
        groove_Value value;
        jz_err e = groove__records__RecordDescriptor__get(record_schema, record, n, (const char *)local_name->ptr, local_name->len, &value);
        if (!e.code) {
            e = groove__db__encoding__ensure_primary_key_value_type(table, column, &value);
            if (!e.code) e = groove__db__encoding__encode_primary_key_part(&bytes, &value);
            groove__records__values__Value__drop(&value);
        }
        if (e.code) { jz_bytes_drop(&bytes); return e; }
    }
    *out = bytes;
    return JZ_OK;
}
jz_err groove__db__encoding__validate_raw_primary_key(const groove_TableSchema *table, u32 variant_tag, groove_RecordDescriptor record_schema, const u8 *record, u32 n, const u8 *key, u32 kn) {
    jz_bytes derived_key;
    JZ_TRY(groove__db__encoding__primary_key_bytes(table, variant_tag, record_schema, record, n, &derived_key));
    bool same = derived_key.len == kn && memcmp(derived_key.ptr, key, kn) == 0;
    jz_bytes_drop(&derived_key);
    if (!same) {
        jz_str msg = jz_format("raw record primary key mismatch for table %.*s", GROOVE_DB_S(table->name));
        jz_err e = groove__db__Error__str(GROOVE_DB_InvalidDirectRecordStoreKey, msg.ptr, msg.len);
        jz_str_drop(&msg);
        return e;
    }
    return JZ_OK;
}

static void str_drop_v(void *p) { jz_str_drop(p); }
jz_err groove__db__encoding__Database__decode_stored_key_value(const groove_Database *db, const groove_TableSchema *table, jz_bytes key, jz_bytes stored, groove_EncodedKeyValue *out) {
    u32 variant_tag;
    jz_slice payload;
    jz_err e = groove__records__split_variant_record(stored.ptr, stored.len, &variant_tag, &payload);
    if (e.code) { jz_bytes_drop(&key); jz_bytes_drop(&stored); return e; }
    jz_btree *descriptors = (jz_btree *)&db->stored_record_descriptors; /* RefCell */
    jz_btree *variants = jz_btree_get(descriptors, &table->name);
    groove_RecordDescriptor *cached = variants ? jz_btree_get(variants, &variant_tag) : NULL;
    groove_RecordDescriptor descriptor;
    if (cached) {
        descriptor = *cached;
    } else {
        if (!groove__schema__TableSchema__record_schema_for_variant(table, variant_tag, &descriptor)) {
            jz_bytes_drop(&key);
            jz_bytes_drop(&stored);
            return groove__db__Error__table_version(GROOVE_DB_UnknownTableVariant, (const char *)table->name.ptr, table->name.len, variant_tag);
        }
        jz_str name = jz_str_clone(&table->name);
        bool inserted;
        jz_btree *entry = jz_btree_entry(descriptors, &name, &inserted);
        if (inserted) jz_btree_init(entry, &groove__db__VARIANT_DESCRIPTORS_DESC);
        else str_drop_v(&name);
        jz_btree_insert(entry, &variant_tag, &descriptor, NULL);
    }
    groove_VariantRecord record = { variant_tag, groove__records__OwnedRecord__new(jz_bytes_from(payload.ptr, payload.len), descriptor) };
    jz_bytes_drop(&stored);
    *out = groove__db__EncodedKeyValue__from_variant(key, record);
    return JZ_OK;
}

static jz_err invalid_index(const char *index_name, u32 inn) { return groove__db__Error__str(GROOVE_DB_InvalidPersistedIndex, index_name, inn); }

static jz_err decode_index_columns(const groove_TableSchema *table, const char *index_name, u32 inn, const groove_IndexSchema *index, jz_slice *remaining, groove_ValueVec *out) {
    groove_ValueVec values = {0};
    for (u32 i = 0; i < index->columns.len; i++) {
        const groove_ColumnSchema *column = NULL;
        for (u32 c = 0; c < table->columns.len; c++)
            if (jz_str_eq(&table->columns.ptr[c].name, &index->columns.ptr[i])) { column = &table->columns.ptr[c]; break; }
        jz_err e;
        groove_Value v;
        if (!column) e = invalid_index(index_name, inn);
        else e = groove__db__encoding__decode_index_key_part(remaining, &column->column_type, index_name, inn, &v);
        if (e.code) { groove__records__values__ValueVec__drop(&values); return e; }
        JZ_VEC_PUSH(values, v);
    }
    *out = values;
    return JZ_OK;
}

/* Rebuild the full encoded primary key of one durable index entry. */
jz_err groove__db__encoding__persisted_index_primary_key(const groove_TableSchema *table, const char *index_name, u32 inn, const groove_IndexSchema *index,
                                                         const u8 *storage_key, u32 skn, const u8 *stored_value, u32 svn, jz_bytes *out) {
    jz_slice logical_key;
    JZ_TRY(groove__db__encoding__persisted_index_logical_key(index_name, inn, storage_key, skn, &logical_key));
    if (!table->has_primary_key) return groove__db__Error__str(GROOVE_DB_MissingPrimaryKey, (const char *)table->name.ptr, table->name.len);
    const groove_PrimaryKey *primary_key = &table->primary_key;
    jz_slice remaining = logical_key;
    groove_ValueVec index_values;
    JZ_TRY(decode_index_columns(table, index_name, inn, index, &remaining, &index_values));
    bool covers_primary_key = true;
    for (u32 i = 0; covers_primary_key && i < primary_key->columns.len; i++) {
        bool found = false;
        for (u32 j = 0; j < index->columns.len; j++) if (jz_str_eq(&index->columns.ptr[j], &primary_key->columns.ptr[i].column)) { found = true; break; }
        covers_primary_key = found;
    }
    jz_err e = JZ_OK;
    jz_bytes bytes = {0};
    jz_slice suffix;
    if (covers_primary_key) {
        if (remaining.len != 0 || svn != 0) { e = invalid_index(index_name, inn); goto done; }
        suffix = jz_slice_of(NULL, 0);
    } else if (index->unique) {
        if (remaining.len != 0) { e = invalid_index(index_name, inn); goto done; }
        suffix = jz_slice_of(stored_value, svn);
    } else {
        if (remaining.len == 0 || remaining.ptr[0] != 0xff || svn != 0) { e = invalid_index(index_name, inn); goto done; }
        suffix = jz_slice_of(remaining.ptr + 1, remaining.len - 1);
    }
    for (u32 i = 0; i < primary_key->columns.len; i++) {
        const groove_PrimaryKeyColumn *column = &primary_key->columns.ptr[i];
        u32 position = index->columns.len;
        for (u32 j = 0; j < index->columns.len; j++) if (jz_str_eq(&index->columns.ptr[j], &column->column)) { position = j; break; }
        if (position < index->columns.len) {
            if (position >= index_values.len) { e = invalid_index(index_name, inn); goto done; }
            const groove_Value *value = &index_values.ptr[position];
            if ((e = groove__db__encoding__ensure_primary_key_value_type(table, column, value)).code) goto done;
            if ((e = groove__db__encoding__encode_primary_key_part(&bytes, value)).code) goto done;
        } else {
            jz_slice before = suffix;
            groove_ColumnType column_type = groove__schema__PrimaryKeyType__column_type(&column->key_type);
            groove_Value decoded;
            jz_err d = groove__db__encoding__decode_primary_key_part(&suffix, &column_type, &decoded);
            groove__records__values__ValueType__drop(&column_type);
            if (d.code) { jz_err_drop(&d); e = invalid_index(index_name, inn); goto done; }
            groove__records__values__Value__drop(&decoded);
            jz_bytes_push(&bytes, before.ptr, before.len - suffix.len);
        }
    }
    if (suffix.len != 0) { e = invalid_index(index_name, inn); goto done; }
done:
    groove__records__values__ValueVec__drop(&index_values);
    if (e.code) { jz_bytes_drop(&bytes); return e; }
    *out = bytes;
    return JZ_OK;
}

/* Decode one covered index or primary-key column without loading its table row. */
jz_err groove__db__encoding__persisted_index_column_value(const groove_TableSchema *table, const char *index_name, u32 inn, const groove_IndexSchema *index,
                                                          const u8 *storage_key, u32 skn, const u8 *stored_value, u32 svn, const char *column_name, u32 cn, groove_Value *out) {
    jz_slice remaining;
    JZ_TRY(groove__db__encoding__persisted_index_logical_key(index_name, inn, storage_key, skn, &remaining));
    for (u32 i = 0; i < index->columns.len; i++) {
        const jz_str *indexed_column = &index->columns.ptr[i];
        const groove_ColumnSchema *column = NULL;
        for (u32 c = 0; c < table->columns.len; c++)
            if (jz_str_eq(&table->columns.ptr[c].name, indexed_column)) { column = &table->columns.ptr[c]; break; }
        if (!column) return invalid_index(index_name, inn);
        groove_Value value;
        JZ_TRY(groove__db__encoding__decode_index_key_part(&remaining, &column->column_type, index_name, inn, &value));
        if (indexed_column->len == cn && memcmp(indexed_column->ptr, column_name, cn) == 0) { *out = value; return JZ_OK; }
        groove__records__values__Value__drop(&value);
    }
    if (!table->has_primary_key) return groove__db__Error__str(GROOVE_DB_MissingPrimaryKey, (const char *)table->name.ptr, table->name.len);
    jz_bytes primary_bytes;
    JZ_TRY(groove__db__encoding__persisted_index_primary_key(table, index_name, inn, index, storage_key, skn, stored_value, svn, &primary_bytes));
    jz_slice rest = jz_bytes_slice(&primary_bytes);
    for (u32 i = 0; i < table->primary_key.columns.len; i++) {
        const groove_PrimaryKeyColumn *column = &table->primary_key.columns.ptr[i];
        groove_ColumnType column_type = groove__schema__PrimaryKeyType__column_type(&column->key_type);
        groove_Value value;
        jz_err e = groove__db__encoding__decode_primary_key_part(&rest, &column_type, &value);
        groove__records__values__ValueType__drop(&column_type);
        if (e.code) { jz_bytes_drop(&primary_bytes); return e; }
        if (column->column.len == cn && memcmp(column->column.ptr, column_name, cn) == 0) { jz_bytes_drop(&primary_bytes); *out = value; return JZ_OK; }
        groove__records__values__Value__drop(&value);
    }
    jz_bytes_drop(&primary_bytes);
    return invalid_index(index_name, inn);
}

/* Strip the numeric index-id prefix from a durable index storage key. */
jz_err groove__db__encoding__persisted_index_logical_key(const char *index_name, u32 inn, const u8 *storage_key, u32 skn, jz_slice *out) {
    u32 id;
    if (!groove__ivm__runtime__split_durable_index_key(storage_key, skn, &id, out)) return invalid_index(index_name, inn);
    return JZ_OK;
}

jz_err groove__db__encoding__ensure_primary_key_value_type(const groove_TableSchema *table, const groove_PrimaryKeyColumn *column, const groove_Value *value) {
    const groove_PrimaryKeyType *t = &column->key_type;
    bool ok = false;
    switch (t->tag) {
    case GROOVE_PK_INTEGER:
        ok = (t->integer == GROOVE_INT_KEY_U8 && value->tag == GROOVE_V_U8) || (t->integer == GROOVE_INT_KEY_U16 && value->tag == GROOVE_V_U16) ||
             (t->integer == GROOVE_INT_KEY_U32 && value->tag == GROOVE_V_U32) || (t->integer == GROOVE_INT_KEY_U64 && value->tag == GROOVE_V_U64);
        break;
    case GROOVE_PK_BOOL: ok = value->tag == GROOVE_V_BOOL; break;
    case GROOVE_PK_STRING: ok = value->tag == GROOVE_V_STRING; break;
    case GROOVE_PK_BYTES: ok = value->tag == GROOVE_V_BYTES; break;
    case GROOVE_PK_UUID: ok = value->tag == GROOVE_V_UUID; break;
    }
    if (ok) return JZ_OK;
    return groove__db__Error__pair(GROOVE_DB_PrimaryKeyTypeMismatch, (const char *)table->name.ptr, table->name.len, (const char *)column->column.ptr, column->column.len);
}

static void push_be(jz_bytes *key, u64 v, u32 width) {
    for (u32 i = width; i-- > 0;) jz_bytes_push_u8(key, (u8)(v >> (8 * i)));
}
jz_err groove__db__encoding__encode_primary_key_part(jz_bytes *key, const groove_Value *value) {
    switch (value->tag) {
    case GROOVE_V_U8: jz_bytes_push_u8(key, 0); jz_bytes_push_u8(key, value->u.u8_); break;
    case GROOVE_V_U16: jz_bytes_push_u8(key, 1); push_be(key, value->u.u16_, 2); break;
    case GROOVE_V_U32: jz_bytes_push_u8(key, 2); push_be(key, value->u.u32_, 4); break;
    case GROOVE_V_U64: jz_bytes_push_u8(key, 3); push_be(key, value->u.u64_, 8); break;
    case GROOVE_V_U48: {
        jz_bytes_push_u8(key, 16);
        u8 b[6];
        JZ_TRY(groove__records__u48_be_bytes(value->u.u64_, b));
        jz_bytes_push(key, b, 6);
        break;
    }
    case GROOVE_V_I32: jz_bytes_push_u8(key, 14); push_be(key, groove__db__encoding__order_preserving_i32_bits(value->u.i32_), 4); break;
    case GROOVE_V_I64: jz_bytes_push_u8(key, 13); push_be(key, groove__db__encoding__order_preserving_i64_bits(value->u.i64_), 8); break;
    case GROOVE_V_BOOL: jz_bytes_push_u8(key, 5); jz_bytes_push_u8(key, value->u.b ? 1 : 0); break;
    case GROOVE_V_STRING: jz_bytes_push_u8(key, 6); groove__db__encoding__encode_ordered_bytes(key, (const u8 *)value->u.s.ptr, value->u.s.len); break;
    case GROOVE_V_ENUM_TAG: jz_bytes_push_u8(key, 0); jz_bytes_push_u8(key, value->u.enum_tag); break;
    case GROOVE_V_BYTES: jz_bytes_push_u8(key, 7); groove__db__encoding__encode_ordered_bytes(key, value->u.bytes.ptr, value->u.bytes.len); break;
    case GROOVE_V_UUID: jz_bytes_push_u8(key, 10); jz_bytes_push(key, value->u.uuid.b, 16); break;
    case GROOVE_V_TUPLE:
        jz_bytes_push_u8(key, 11);
        for (u32 i = 0; i < value->u.items.len; i++) JZ_TRY(groove__db__encoding__encode_primary_key_part(key, &value->u.items.ptr[i]));
        break;
    default:
        /* F64, Array, Nullable, Record, Large, Enum: direct-store keys require
         * a declared total order. */
        return groove__db__Error__str(GROOVE_DB_InvalidDirectRecordStoreKey, "unsupported direct record store key type", 40);
    }
    return JZ_OK;
}
void groove__db__encoding__encode_ordered_bytes(jz_bytes *key, const u8 *value, u32 n) {
    for (u32 i = 0; i < n; i++) {
        if (value[i] == 0) { jz_bytes_push_u8(key, 0); jz_bytes_push_u8(key, 0xff); }
        else jz_bytes_push_u8(key, value[i]);
    }
    jz_bytes_push_u8(key, 0);
    jz_bytes_push_u8(key, 0);
}
u64 groove__db__encoding__order_preserving_i64_bits(i64 value) { return (u64)value ^ (1ull << 63); }
u32 groove__db__encoding__order_preserving_i32_bits(i32 value) { return (u32)value ^ (1u << 31); }

/* ---- direct-store key decoding ---- */
static jz_err dk_err(const char *what) { return groove__db__Error__str(GROOVE_DB_InvalidDirectRecordStoreKey, what, (u32)strlen(what)); }
static jz_err take_key_bytes(jz_slice *bytes, u32 len, const u8 **head) {
    if (bytes->len < len) return dk_err("truncated");
    *head = bytes->ptr;
    bytes->ptr += len;
    bytes->len -= len;
    return JZ_OK;
}
static jz_err expect_key_tag(jz_slice *bytes, u8 expected) {
    const u8 *h;
    JZ_TRY(take_key_bytes(bytes, 1, &h));
    return h[0] == expected ? JZ_OK : dk_err("tag");
}
static u64 be(const u8 *p, u32 n) { u64 v = 0; for (u32 i = 0; i < n; i++) v = (v << 8) | p[i]; return v; }
static jz_err decode_ordered_bytes(jz_slice *bytes, jz_bytes *out) {
    jz_bytes acc = {0};
    for (;;) {
        const u8 *h;
        jz_err e = take_key_bytes(bytes, 1, &h);
        if (e.code) { jz_bytes_drop(&acc); return e; }
        if (h[0] != 0) { jz_bytes_push_u8(&acc, h[0]); continue; }
        e = take_key_bytes(bytes, 1, &h);
        if (e.code) { jz_bytes_drop(&acc); return e; }
        if (h[0] == 0) { *out = acc; return JZ_OK; }
        if (h[0] == 0xff) { jz_bytes_push_u8(&acc, 0); continue; }
        jz_bytes_drop(&acc);
        return dk_err("bytes");
    }
}
static groove_Value mkv(u8 tag) { groove_Value v; memset(&v, 0, sizeof v); v.tag = tag; return v; }

jz_err groove__db__encoding__decode_primary_key_part(jz_slice *bytes, const groove_ValueType *value_type, groove_Value *out) {
    const u8 *h;
    switch (value_type->tag) {
    case GROOVE_VT_U8: JZ_TRY(expect_key_tag(bytes, 0)); JZ_TRY(take_key_bytes(bytes, 1, &h)); *out = mkv(GROOVE_V_U8); out->u.u8_ = h[0]; return JZ_OK;
    case GROOVE_VT_U16: JZ_TRY(expect_key_tag(bytes, 1)); JZ_TRY(take_key_bytes(bytes, 2, &h)); *out = mkv(GROOVE_V_U16); out->u.u16_ = (u16)be(h, 2); return JZ_OK;
    case GROOVE_VT_U32: JZ_TRY(expect_key_tag(bytes, 2)); JZ_TRY(take_key_bytes(bytes, 4, &h)); *out = mkv(GROOVE_V_U32); out->u.u32_ = (u32)be(h, 4); return JZ_OK;
    case GROOVE_VT_U64: JZ_TRY(expect_key_tag(bytes, 3)); JZ_TRY(take_key_bytes(bytes, 8, &h)); *out = mkv(GROOVE_V_U64); out->u.u64_ = be(h, 8); return JZ_OK;
    case GROOVE_VT_U48: JZ_TRY(expect_key_tag(bytes, 16)); JZ_TRY(take_key_bytes(bytes, 6, &h)); *out = mkv(GROOVE_V_U48); out->u.u64_ = groove__records__u48_from_be_bytes(h); return JZ_OK;
    case GROOVE_VT_I32: JZ_TRY(expect_key_tag(bytes, 14)); JZ_TRY(take_key_bytes(bytes, 4, &h)); *out = mkv(GROOVE_V_I32); out->u.i32_ = (i32)((u32)be(h, 4) ^ (1u << 31)); return JZ_OK;
    case GROOVE_VT_I64: JZ_TRY(expect_key_tag(bytes, 13)); JZ_TRY(take_key_bytes(bytes, 8, &h)); *out = mkv(GROOVE_V_I64); out->u.i64_ = (i64)(be(h, 8) ^ (1ull << 63)); return JZ_OK;
    case GROOVE_VT_BOOL:
        JZ_TRY(expect_key_tag(bytes, 5));
        JZ_TRY(take_key_bytes(bytes, 1, &h));
        if (h[0] > 1) return dk_err("bool");
        *out = mkv(GROOVE_V_BOOL); out->u.b = h[0] == 1;
        return JZ_OK;
    case GROOVE_VT_STRING: {
        JZ_TRY(expect_key_tag(bytes, 6));
        jz_bytes raw;
        JZ_TRY(decode_ordered_bytes(bytes, &raw));
        if (!jz_utf8_valid(raw.ptr, raw.len)) { jz_bytes_drop(&raw); return dk_err("string"); }
        *out = mkv(GROOVE_V_STRING);
        out->u.s.ptr = (char *)raw.ptr; out->u.s.len = raw.len; out->u.s.cap = raw.cap;
        return JZ_OK;
    }
    case GROOVE_VT_BYTES: {
        JZ_TRY(expect_key_tag(bytes, 7));
        jz_bytes raw;
        JZ_TRY(decode_ordered_bytes(bytes, &raw));
        *out = mkv(GROOVE_V_BYTES); out->u.bytes = raw;
        return JZ_OK;
    }
    case GROOVE_VT_UUID: JZ_TRY(expect_key_tag(bytes, 10)); JZ_TRY(take_key_bytes(bytes, 16, &h)); *out = mkv(GROOVE_V_UUID); memcpy(out->u.uuid.b, h, 16); return JZ_OK;
    case GROOVE_VT_ENUM_TAG: JZ_TRY(expect_key_tag(bytes, 0)); JZ_TRY(take_key_bytes(bytes, 1, &h)); *out = mkv(GROOVE_V_ENUM_TAG); out->u.enum_tag = h[0]; return JZ_OK;
    case GROOVE_VT_TUPLE: {
        JZ_TRY(expect_key_tag(bytes, 11));
        groove_Value tuple = mkv(GROOVE_V_TUPLE);
        jz_vec_reserve(&tuple.u.items, value_type->u.tuple.len, sizeof(groove_Value));
        for (u32 i = 0; i < value_type->u.tuple.len; i++) {
            groove_Value member;
            jz_err e = groove__db__encoding__decode_primary_key_part(bytes, &value_type->u.tuple.ptr[i], &member);
            if (e.code) { groove__records__values__Value__drop(&tuple); return e; }
            JZ_VEC_PUSH(tuple.u.items, member);
        }
        *out = tuple;
        return JZ_OK;
    }
    default:
        return dk_err("unsupported direct record store key type");
    }
}

/* ---- durable index key decoding ---- */
static jz_err take_persisted_index_key_bytes(jz_slice *bytes, const char *index_name, u32 inn, u32 len, const u8 **head) {
    if (bytes->len < len) return invalid_index(index_name, inn);
    *head = bytes->ptr;
    bytes->ptr += len;
    bytes->len -= len;
    return JZ_OK;
}
static jz_err expect_persisted_index_key_tag(jz_slice *bytes, const char *index_name, u32 inn, u8 expected) {
    const u8 *h;
    JZ_TRY(take_persisted_index_key_bytes(bytes, index_name, inn, 1, &h));
    return h[0] == expected ? JZ_OK : invalid_index(index_name, inn);
}
static jz_err decode_persisted_index_ordered_bytes(jz_slice *bytes, const char *index_name, u32 inn, jz_bytes *out) {
    jz_bytes acc = {0};
    for (;;) {
        const u8 *h;
        jz_err e = take_persisted_index_key_bytes(bytes, index_name, inn, 1, &h);
        if (e.code) { jz_bytes_drop(&acc); return e; }
        if (h[0] != 0) { jz_bytes_push_u8(&acc, h[0]); continue; }
        e = take_persisted_index_key_bytes(bytes, index_name, inn, 1, &h);
        if (e.code) { jz_bytes_drop(&acc); return e; }
        if (h[0] == 0) { *out = acc; return JZ_OK; }
        if (h[0] == 0xff) { jz_bytes_push_u8(&acc, 0); continue; }
        jz_bytes_drop(&acc);
        return invalid_index(index_name, inn);
    }
}
jz_err groove__db__encoding__decode_index_key_part(jz_slice *bytes, const groove_ColumnType *column_type, const char *index_name, u32 inn, groove_Value *out) {
    const u8 *h;
#define TAG(t) JZ_TRY(expect_persisted_index_key_tag(bytes, index_name, inn, (t)))
#define TAKE(n) JZ_TRY(take_persisted_index_key_bytes(bytes, index_name, inn, (n), &h))
    switch (column_type->tag) {
    case GROOVE_VT_U8: TAG(0); TAKE(1); *out = mkv(GROOVE_V_U8); out->u.u8_ = h[0]; return JZ_OK;
    case GROOVE_VT_U16: TAG(1); TAKE(2); *out = mkv(GROOVE_V_U16); out->u.u16_ = (u16)be(h, 2); return JZ_OK;
    case GROOVE_VT_U32: TAG(2); TAKE(4); *out = mkv(GROOVE_V_U32); out->u.u32_ = (u32)be(h, 4); return JZ_OK;
    case GROOVE_VT_U64: TAG(3); TAKE(8); *out = mkv(GROOVE_V_U64); out->u.u64_ = be(h, 8); return JZ_OK;
    case GROOVE_VT_U48: TAG(16); TAKE(6); *out = mkv(GROOVE_V_U48); out->u.u64_ = groove__records__u48_from_be_bytes(h); return JZ_OK;
    case GROOVE_VT_I32: TAG(14); TAKE(4); *out = mkv(GROOVE_V_I32); out->u.i32_ = (i32)((u32)be(h, 4) ^ (1u << 31)); return JZ_OK;
    case GROOVE_VT_I64: TAG(13); TAKE(8); *out = mkv(GROOVE_V_I64); out->u.i64_ = (i64)(be(h, 8) ^ (1ull << 63)); return JZ_OK;
    case GROOVE_VT_F64: {
        TAG(4); TAKE(8);
        u64 ordered = be(h, 8);
        u64 bits = (ordered & (1ull << 63)) ? ordered ^ (1ull << 63) : ~ordered;
        f64 value;
        memcpy(&value, &bits, 8);
        if (value != value) return invalid_index(index_name, inn);
        *out = mkv(GROOVE_V_F64); out->u.f64_ = value;
        return JZ_OK;
    }
    case GROOVE_VT_BOOL:
        TAG(5); TAKE(1);
        if (h[0] > 1) return invalid_index(index_name, inn);
        *out = mkv(GROOVE_V_BOOL); out->u.b = h[0] == 1;
        return JZ_OK;
    case GROOVE_VT_STRING: {
        TAG(6);
        jz_bytes raw;
        JZ_TRY(decode_persisted_index_ordered_bytes(bytes, index_name, inn, &raw));
        if (!jz_utf8_valid(raw.ptr, raw.len)) { jz_bytes_drop(&raw); return invalid_index(index_name, inn); }
        *out = mkv(GROOVE_V_STRING);
        out->u.s.ptr = (char *)raw.ptr; out->u.s.len = raw.len; out->u.s.cap = raw.cap;
        return JZ_OK;
    }
    case GROOVE_VT_BYTES: {
        TAG(7);
        jz_bytes raw;
        JZ_TRY(decode_persisted_index_ordered_bytes(bytes, index_name, inn, &raw));
        *out = mkv(GROOVE_V_BYTES); out->u.bytes = raw;
        return JZ_OK;
    }
    case GROOVE_VT_UUID: TAG(10); TAKE(16); *out = mkv(GROOVE_V_UUID); memcpy(out->u.uuid.b, h, 16); return JZ_OK;
    case GROOVE_VT_ENUM_TAG: {
        TAG(0); TAKE(1);
        u8 discriminant = h[0];
        jz_err e = groove__records__ScalarEnumSchema__variant(column_type->u.enum_tag, discriminant);
        if (e.code) { jz_err_drop(&e); return invalid_index(index_name, inn); }
        *out = mkv(GROOVE_V_ENUM_TAG); out->u.enum_tag = discriminant;
        return JZ_OK;
    }
    case GROOVE_VT_TUPLE: {
        TAG(11);
        groove_Value tuple = mkv(GROOVE_V_TUPLE);
        for (u32 i = 0; i < column_type->u.tuple.len; i++) {
            groove_Value member;
            jz_err e = groove__db__encoding__decode_index_key_part(bytes, &column_type->u.tuple.ptr[i], index_name, inn, &member);
            if (e.code) { groove__records__values__Value__drop(&tuple); return e; }
            JZ_VEC_PUSH(tuple.u.items, member);
        }
        *out = tuple;
        return JZ_OK;
    }
    case GROOVE_VT_NULLABLE:
        TAKE(1);
        if (h[0] == 8) { *out = mkv(GROOVE_V_NULLABLE); out->u.nullable = NULL; return JZ_OK; }
        if (h[0] == 9) {
            groove_Value inner;
            JZ_TRY(groove__db__encoding__decode_index_key_part(bytes, column_type->u.inner, index_name, inn, &inner));
            *out = mkv(GROOVE_V_NULLABLE);
            out->u.nullable = jz_memdup(&inner, sizeof inner);
            return JZ_OK;
        }
        return invalid_index(index_name, inn);
    default:
        /* Internal, Array, Record, Enum */
        return invalid_index(index_name, inn);
    }
#undef TAG
#undef TAKE
}

groove_RecordDescriptor groove__db__encoding__index_record_descriptor(void) {
    static groove_RecordDescriptor descriptor;
    static bool init;
    if (!init) {
        jz_str names[2] = { jz_str_from("key"), jz_str_from("value") };
        groove_ValueType types[2];
        memset(types, 0, sizeof types);
        types[0].tag = GROOVE_VT_BYTES;
        types[1].tag = GROOVE_VT_BYTES;
        descriptor = groove__records__RecordDescriptor__new(names, types, 2);
        jz_str_drop(&names[0]);
        jz_str_drop(&names[1]);
        init = true;
    }
    return descriptor;
}

jz_err groove__db__encoding__encode_index_prefix_part(jz_bytes *key, const groove_Value *value, const groove_ColumnType *column_type) {
    if (value->tag == GROOVE_V_STRING && column_type->tag == GROOVE_VT_ENUM_TAG) {
        u8 discriminant;
        JZ_TRY(groove__records__ScalarEnumSchema__discriminant(column_type->u.enum_tag, &value->u.s, &discriminant));
        groove_Value v = mkv(GROOVE_V_U8);
        v.u.u8_ = discriminant;
        return groove__ivm__runtime__encode_key_part(key, &v);
    }
    if (value->tag == GROOVE_V_ENUM_TAG && column_type->tag == GROOVE_VT_ENUM_TAG) {
        JZ_TRY(groove__records__ScalarEnumSchema__variant(column_type->u.enum_tag, value->u.enum_tag));
        groove_Value v = mkv(GROOVE_V_U8);
        v.u.u8_ = value->u.enum_tag;
        return groove__ivm__runtime__encode_key_part(key, &v);
    }
    if (value->tag == GROOVE_V_NULLABLE && column_type->tag == GROOVE_VT_NULLABLE) {
        if (!value->u.nullable) {
            groove_Value none = mkv(GROOVE_V_NULLABLE);
            return groove__ivm__runtime__encode_key_part(key, &none);
        }
        jz_bytes encoded = {0};
        jz_err e = groove__db__encoding__encode_index_prefix_part(&encoded, value->u.nullable, column_type->u.inner);
        if (e.code) { jz_bytes_drop(&encoded); return e; }
        jz_bytes wrapped = {0};
        jz_bytes_push_u8(&wrapped, 9);
        jz_bytes_push(&wrapped, encoded.ptr, encoded.len);
        jz_bytes_push(key, wrapped.ptr, wrapped.len);
        jz_bytes_drop(&encoded);
        jz_bytes_drop(&wrapped);
        return JZ_OK;
    }
    return groove__ivm__runtime__encode_key_part(key, value);
}
