/* Port of crates/groove/src/db/schema_admission.rs */
#include "jz_rt.h"
#include "../_ext_groove-ivm-db.h"
#include "mod.h"
#include <string.h>

/* HashSet<&str> */
static u64 slice_hash(const void *k) { const jz_slice *s = k; return jz_hash_bytes(s->ptr, s->len); }
static bool slice_eq(const void *a, const void *b) {
    const jz_slice *x = a, *y = b;
    return x->len == y->len && (x->len == 0 || memcmp(x->ptr, y->ptr, x->len) == 0);
}
static const jz_hmap_desc STR_SET_DESC = { sizeof(jz_slice), 0, slice_hash, slice_eq, NULL, NULL };
/* HashSet<u32> */
static u64 u32_hash(const void *k) { return jz_hash_u64(*(const u32 *)k); }
static bool u32_eq(const void *a, const void *b) { return *(const u32 *)a == *(const u32 *)b; }
static const jz_hmap_desc U32_SET_DESC = { sizeof(u32), 0, u32_hash, u32_eq, NULL, NULL };

static inline jz_slice str_slice(const jz_str *s) { return (jz_slice){ (const u8 *)s->ptr, s->len }; }
/* HashSet::insert: true when newly inserted */
static bool set_insert(jz_hmap *set, jz_slice key) { return !jz_hmap_insert(set, &key, NULL, NULL); }
static bool set_contains(const jz_hmap *set, const jz_str *key) { jz_slice k = str_slice(key); return jz_hmap_get(set, &k) != NULL; }

static jz_err check_storage_name(jz_hmap *seen, const jz_str *name) {
    jz_err error = groove__storage__validate_application_storage_name(name->ptr, name->len);
    if (error.code) {
        /* reason: error.to_string() */
        jz_err e = groove__db__Error__pair(GROOVE_DB_InvalidApplicationStorageName, name->ptr, name->len, error.msg ? error.msg->ptr : "", error.msg ? error.msg->len : 0);
        jz_err_drop(&error);
        return e;
    }
    if (!set_insert(seen, str_slice(name)))
        return groove__db__Error__str(GROOVE_DB_DuplicateApplicationStorageName, name->ptr, name->len);
    return JZ_OK;
}

/* Validate application-selected physical names before an open or live schema
 * admission can make a Groove storage mutation. */
jz_err groove__db__schema_admission__validate_application_storage_names(const groove_DatabaseSchema *schema) {
    jz_hmap seen;
    jz_hmap_init(&seen, &STR_SET_DESC); /* BTreeSet<&str>: only membership is observable */
    jz_err e = JZ_OK;
    for (u32 i = 0; i < schema->tables.len && !e.code; i++) e = check_storage_name(&seen, &schema->tables.ptr[i].name);
    for (u32 i = 0; i < schema->direct_record_stores.len && !e.code; i++) e = check_storage_name(&seen, &schema->direct_record_stores.ptr[i].name);
    jz_hmap_drop(&seen);
    return e;
}

static bool is_supported_direct_store_tuple_member(const groove_ValueType *value_type) {
    switch (value_type->tag) {
    case GROOVE_VT_U8: case GROOVE_VT_U16: case GROOVE_VT_U32: case GROOVE_VT_U64:
    case GROOVE_VT_I32: case GROOVE_VT_I64: case GROOVE_VT_BOOL: case GROOVE_VT_UUID:
    case GROOVE_VT_ENUM_TAG:
        return true;
    case GROOVE_VT_TUPLE:
        for (u32 i = 0; i < value_type->u.tuple.len; i++)
            if (!is_supported_direct_store_tuple_member(&value_type->u.tuple.ptr[i])) return false;
        return true;
    default:
        return false;
    }
}

static bool is_supported_direct_store_key_type(const groove_ValueType *value_type) {
    switch (value_type->tag) {
    case GROOVE_VT_STRING: case GROOVE_VT_BYTES:
        return true;
    case GROOVE_VT_TUPLE:
        for (u32 i = 0; i < value_type->u.tuple.len; i++)
            if (!is_supported_direct_store_tuple_member(&value_type->u.tuple.ptr[i])) return false;
        return true;
    default:
        return is_supported_direct_store_tuple_member(value_type);
    }
}

jz_err groove__db__schema_admission__validate_durable_key_schema(const groove_DatabaseSchema *schema) {
    JZ_VEC_FOR(const groove_DirectRecordStoreSchema, store, schema->direct_record_stores) {
        JZ_VEC_FOR(const groove_KeyField, field, store->key) {
            if (!is_supported_direct_store_key_type(&field->value_type))
                return groove__db__Error__str(GROOVE_DB_InvalidDirectRecordStoreKey, store->name.ptr, store->name.len);
        }
    }
    JZ_VEC_FOR(const groove_TableSchema, table, schema->tables)
        JZ_TRY(groove__db__schema_admission__validate_table_schema_variants(table));
    return JZ_OK;
}

static bool table_has_column(const groove_TableSchema *table, const jz_str *name) {
    JZ_VEC_FOR(const groove_ColumnSchema, column, table->columns)
        if (jz_str_eq(&column->name, name)) return true;
    return false;
}

static jz_err invalid_variant_field(const groove_TableSchema *table, u32 tag, const jz_str *field) {
    return groove__db__Error__table_version_field(GROOVE_DB_InvalidTableVariantField, table->name.ptr, table->name.len, (u64)tag, field->ptr, field->len);
}

static jz_err validate_one_variant(const groove_TableSchema *table, const groove_TableVariant *variant_tag, jz_hmap *fields) {
    if (variant_tag->payload_fields.len == 0) {
        JZ_VEC_FOR(const jz_str, field, variant_tag->fields) {
            if (!set_insert(fields, str_slice(field)) || !table_has_column(table, field))
                return invalid_variant_field(table, variant_tag->tag, field);
        }
    } else {
        jz_hmap local;
        jz_hmap_init(&local, &STR_SET_DESC);
        jz_err e = JZ_OK;
        JZ_VEC_FOR(const groove_TableVariantField, field, variant_tag->payload_fields) {
            if (!set_insert(&local, str_slice(&field->name))) { e = invalid_variant_field(table, variant_tag->tag, &field->name); break; }
            if (!field->has_shared_column) continue;
            const jz_str *shared = &field->shared_column;
            bool valid = false;
            JZ_VEC_FOR(const groove_ColumnSchema, column, table->columns) {
                if (jz_str_eq(&column->name, shared) &&
                    groove__records__values__ValueType__registry_compatible_with(&column->column_type, &field->value_type)) {
                    valid = true;
                    break;
                }
            }
            if (!valid || !set_insert(fields, str_slice(shared))) { e = invalid_variant_field(table, variant_tag->tag, shared); break; }
        }
        jz_hmap_drop(&local);
        JZ_TRY(e);
    }
    JZ_VEC_FOR(const groove_PrimaryKeyColumn, column, table->primary_key.columns) {
        if (!set_contains(fields, &column->column))
            return groove__db__Error__table_version_field(GROOVE_DB_SchemaVersionMissingPrimaryKey, table->name.ptr, table->name.len, (u64)variant_tag->tag,
                                                          column->column.ptr, column->column.len);
    }
    return JZ_OK;
}

jz_err groove__db__schema_admission__validate_table_schema_variants(const groove_TableSchema *table) {
    if (!groove__schema__TableSchema__has_variants(table)) return JZ_OK;
    if (table->foreign_keys.len != 0)
        return groove__db__Error__str(GROOVE_DB_UnsupportedSchemaVariantTableFeature, table->name.ptr, table->name.len);
    if (!table->has_primary_key) return groove__db__Error__str(GROOVE_DB_MissingPrimaryKey, table->name.ptr, table->name.len);

    jz_hmap versions;
    jz_hmap_init(&versions, &U32_SET_DESC);
    jz_err e = JZ_OK;
    JZ_VEC_FOR(const groove_TableVariant, variant_tag, table->variants) {
        if (variant_tag->tag == 0) { e = groove__db__Error__str(GROOVE_DB_ReservedTableVariant, table->name.ptr, table->name.len); break; }
        if (jz_hmap_insert(&versions, &variant_tag->tag, NULL, NULL)) {
            e = groove__db__Error__table_version(GROOVE_DB_DuplicateTableVariant, table->name.ptr, table->name.len, (u64)variant_tag->tag);
            break;
        }
        jz_hmap fields;
        jz_hmap_init(&fields, &STR_SET_DESC);
        e = validate_one_variant(table, variant_tag, &fields);
        jz_hmap_drop(&fields);
        if (e.code) break;
    }
    jz_hmap_drop(&versions);
    return e;
}

/* ------------------------------------------------------------------------- */
/* impl Database                                                             */
/* ------------------------------------------------------------------------- */

static void reset_batch_preparation_owner(groove_Database *db) {
    jz_rc_release(db->batch_preparation_owner);
    db->batch_preparation_owner = groove__db__unit_rc_new(); /* Rc::new(()) */
}

static const u8 DECLARED_INDEX_GENERATION_KEY[] = "\0groove-declared-index-generation";

/* Repair schema-declared derived indexes once for a caller-owned generation.
 * A failed repair poisons this instance; reopening retries from the primary records. */
jz_err groove__db__schema_admission__Database__ensure_declared_index_generation(groove_Database *db, u64 generation) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    if (db->resident_publications.len != 0 || !groove__ivm__runtime__IvmRuntime__declared_index_repair_is_idle(db->ivm_runtime))
        return groove__db__Error__invalid_persisted_index("index repair requires an idle startup runtime");
    groove_DynStorage storage = groove__storage__LayoutStorage__as_dyn(db->storage);
    const u32 key_len = (u32)sizeof DECLARED_INDEX_GENERATION_KEY - 1;

    bool found = false;
    jz_bytes bytes = {0};
    JZ_TRY(groove__storage__OrderedKvStorage__get(storage, jz_str_from("indices"), jz_bytes_from(DECLARED_INDEX_GENERATION_KEY, key_len), &found, &bytes));
    if (found) {
        if (bytes.len != 8) {
            jz_bytes_drop(&bytes);
            return groove__db__Error__invalid_persisted_index("malformed declared-index generation (expected BE u64)");
        }
        u64 stored = 0;
        for (u32 i = 0; i < 8; i++) stored = (stored << 8) | bytes.ptr[i];
        jz_bytes_drop(&bytes);
        if (stored > generation)
            return groove__db__Error__invalid_persisted_index("declared-index generation %llu is newer than supported %llu", (unsigned long long)stored,
                                                              (unsigned long long)generation);
        if (stored == generation) return JZ_OK;
    }
    /* Poison before the first destructive write: any storage/decode error
     * requires reopening instead of using a partially rebuilt instance. */
    db->poisoned = true;
    /* Declared indexes' id registrations become durable before any rebuilt entry that uses them. */
    groove_SharedIndexIds index_ids = groove__ivm__runtime__IvmRuntime__index_ids(db->ivm_runtime);
    groove_IndexIdRegistrations *registrations = groove__ivm__runtime__IndexIdRegistry__pending(index_ids);
    if (!groove__ivm__runtime__IndexIdRegistrations__is_empty(registrations)) {
        u32 n = 0;
        const groove_OwnedWriteOperation *ops = groove__ivm__runtime__IndexIdRegistrations__operations(registrations, &n);
        groove_WriteOpVec owned = {0};
        for (u32 i = 0; i < n; i++) {
            groove_OwnedWriteOperation *slot = jz_vec_push_slot(&owned, sizeof *slot);
            groove__storage__OwnedWriteOperation__Clone__clone(slot, &ops[i]);
        }
        jz_err e = groove__storage__OrderedKvStorage__write_many(storage, owned);
        if (e.code) { groove__ivm__runtime__IndexIdRegistrations__drop(registrations); return e; }
        groove__ivm__runtime__IndexIdRegistry__mark_persisted(index_ids, registrations);
    }
    groove__ivm__runtime__IndexIdRegistrations__drop(registrations);
    JZ_TRY(groove__ivm__runtime__IvmRuntime__rebuild_declared_indexes(db->ivm_runtime, storage));
    JZ_TRY(groove__storage__OrderedKvStorage__flush_write_boundary(storage));
    u8 be[8];
    for (u32 i = 0; i < 8; i++) be[i] = (u8)(generation >> (56 - 8 * i));
    JZ_TRY(groove__storage__OrderedKvStorage__set(storage, jz_str_from("indices"), jz_bytes_from(DECLARED_INDEX_GENERATION_KEY, key_len), jz_bytes_from(be, 8)));
    JZ_TRY(groove__storage__OrderedKvStorage__flush_write_boundary(storage));
    db->poisoned = false;
    return JZ_OK;
}

static void str_clone_v(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone((const jz_str *)src); }
static void variant_map_clone_v(void *dst, const void *src) { jz_btree_clone((jz_btree *)dst, (const jz_btree *)src, NULL, NULL); }

/* Capture the complete process-local registry state before a compound schema activation. */
groove_RuntimeRegistryCheckpoint groove__db__schema_admission__Database__runtime_registry_checkpoint(const groove_Database *db) {
    groove_RuntimeRegistryCheckpoint c;
    c.ivm_runtime = groove__ivm__runtime__IvmRuntime__Clone__clone(db->ivm_runtime);
    jz_btree_clone(&c.stored_record_descriptors, &db->stored_record_descriptors, str_clone_v, variant_map_clone_v);
    return c;
}

/* Discard schema-registration changes made since a matching checkpoint.
 * Deliberately does not restore the poison flag. */
void groove__db__schema_admission__Database__restore_runtime_registry(groove_Database *db, groove_RuntimeRegistryCheckpoint checkpoint) {
    reset_batch_preparation_owner(db);
    groove__ivm__runtime__IvmRuntime__drop(db->ivm_runtime);
    db->ivm_runtime = checkpoint.ivm_runtime;
    jz_btree_drop(&db->stored_record_descriptors);
    db->stored_record_descriptors = checkpoint.stored_record_descriptors;
}

/* Return the live schema for a table. */
jz_err groove__db__schema_admission__Database__table_schema(const groove_Database *db, const char *table, u32 n, const groove_TableSchema **out) {
    return groove__db__commit__Database__table(db, table, n, out);
}

/* Append a new table to the live runtime schema. Consumes `table`. */
jz_err groove__db__schema_admission__Database__register_table(groove_Database *db, groove_TableSchema table) {
    reset_batch_preparation_owner(db);
    jz_err e = groove__db__commit__Database__ensure_not_poisoned(db);
    if (e.code) { groove__schema__TableSchema__drop(&table); return e; }
    if (groove__ivm__runtime__IvmRuntime__table(db->ivm_runtime, table.name.ptr, table.name.len)) {
        e = groove__db__Error__str(GROOVE_DB_TableAlreadyExists, table.name.ptr, table.name.len);
        groove__schema__TableSchema__drop(&table);
        return e;
    }
    groove_DatabaseSchema updated;
    groove__schema__DatabaseSchema__Clone__clone(&updated, groove__ivm__runtime__IvmRuntime__schema(db->ivm_runtime));
    groove_TableSchema *pushed = jz_vec_push_slot(&updated.tables, sizeof *pushed);
    groove__schema__TableSchema__Clone__clone(pushed, &table);
    e = groove__db__schema_admission__validate_application_storage_names(&updated);
    if (!e.code) e = groove__db__schema_admission__validate_durable_key_schema(&updated);
    groove__schema__DatabaseSchema__drop(&updated);
    if (e.code) { groove__schema__TableSchema__drop(&table); return e; }
    return groove__ivm__runtime__IvmRuntime__register_table(db->ivm_runtime, table); /* map_err(Error::IvmRuntime): transparent */
}

/* Append stable catalogue fields and one row layout to a live variant table.
 * Borrows `columns` (cloned as consumed); consumes `variant`. */
jz_err groove__db__schema_admission__Database__register_table_variant_with_columns(groove_Database *db, const char *table, u32 tn, const groove_ColumnSchema *columns,
                                                                                    u32 ncolumns, groove_TableVariant variant) {
    reset_batch_preparation_owner(db);
    jz_err e = groove__db__commit__Database__ensure_not_poisoned(db);
    const groove_TableSchema *live = NULL;
    if (!e.code) e = groove__db__commit__Database__table(db, table, tn, &live);
    if (e.code) { groove__schema__TableVariant__drop(&variant); return e; }
    groove_TableSchema updated;
    groove__schema__TableSchema__Clone__clone(&updated, live);
    groove_ColumnSchemaVec added_columns = {0};
    groove_ColumnSchemaVec evolved_columns = {0};
    if (!groove__schema__TableSchema__has_variants(&updated)) {
        e = groove__db__Error__str(GROOVE_DB_CannotPromoteLiveTableToSchemaVariants, table, tn);
        goto fail;
    }
    for (u32 i = 0; i < ncolumns; i++) {
        const groove_ColumnSchema *column = &columns[i];
        groove_ColumnSchema *existing = NULL;
        JZ_VEC_FOR(groove_ColumnSchema, c, updated.columns)
            if (jz_str_eq(&c->name, &column->name)) { existing = c; break; }
        if (existing && groove__schema__ColumnSchema__PartialEq__eq(existing, column)) continue;
        if (existing && groove__records__values__ValueType__can_evolve_registry_to(&existing->column_type, &column->column_type)) {
            groove__schema__ColumnSchema__drop(existing);
            groove__schema__ColumnSchema__Clone__clone(existing, column);
            groove__schema__ColumnSchema__Clone__clone(jz_vec_push_slot(&evolved_columns, sizeof(groove_ColumnSchema)), column);
            continue;
        }
        if (existing) {
            e = groove__db__Error__pair(GROOVE_DB_TableFieldDefinitionMismatch, table, tn, column->name.ptr, column->name.len);
            goto fail;
        }
        groove__schema__ColumnSchema__Clone__clone(jz_vec_push_slot(&updated.columns, sizeof(groove_ColumnSchema)), column);
        groove__schema__ColumnSchema__Clone__clone(jz_vec_push_slot(&added_columns, sizeof(groove_ColumnSchema)), column);
    }
    groove__schema__TableVariant__Clone__clone(jz_vec_push_slot(&updated.variants, sizeof(groove_TableVariant)), &variant);
    e = groove__db__schema_admission__validate_table_schema_variants(&updated);
    if (e.code) goto fail;
    if (evolved_columns.len != 0) {
        e = groove__ivm__runtime__IvmRuntime__evolve_table_variant_registries(db->ivm_runtime, table, tn, evolved_columns.ptr, evolved_columns.len);
        if (e.code) goto fail;
    }
    jz_vec_drop(&evolved_columns, sizeof(groove_ColumnSchema), groove__schema__ColumnSchema__drop);
    groove__schema__TableSchema__drop(&updated);
    return groove__ivm__runtime__IvmRuntime__register_table_variant_with_columns(db->ivm_runtime, table, tn, added_columns, variant);
fail:
    jz_vec_drop(&added_columns, sizeof(groove_ColumnSchema), groove__schema__ColumnSchema__drop);
    jz_vec_drop(&evolved_columns, sizeof(groove_ColumnSchema), groove__schema__ColumnSchema__drop);
    groove__schema__TableSchema__drop(&updated);
    groove__schema__TableVariant__drop(&variant);
    return e;
}

/* Append one whole-row enum case to an already variant table. */
jz_err groove__db__schema_admission__Database__register_table_variant(groove_Database *db, const char *table, u32 tn, groove_TableVariant variant) {
    return groove__db__schema_admission__Database__register_table_variant_with_columns(db, table, tn, NULL, 0, variant);
}

/* Append cases to nested enum registries without changing physical column identity. */
jz_err groove__db__schema_admission__Database__evolve_table_variant_registries(groove_Database *db, const char *table, u32 tn, const groove_ColumnSchema *columns, u32 n) {
    reset_batch_preparation_owner(db);
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    const groove_TableSchema *existing;
    JZ_TRY(groove__db__commit__Database__table(db, table, tn, &existing));
    for (u32 i = 0; i < n; i++) {
        const groove_ColumnSchema *desired = &columns[i];
        JZ_VEC_FOR(const groove_ColumnSchema, current, existing->columns) {
            if (!jz_str_eq(&current->name, &desired->name)) continue;
            if (!groove__records__values__ValueType__can_evolve_registry_to(&current->column_type, &desired->column_type))
                return groove__db__Error__pair(GROOVE_DB_TableFieldDefinitionMismatch, table, tn, desired->name.ptr, desired->name.len);
            break;
        }
    }
    return groove__ivm__runtime__IvmRuntime__evolve_table_variant_registries(db->ivm_runtime, table, tn, columns, n);
}

/* Append and backfill one durable secondary index on a live table. Consumes `index`. */
jz_err groove__db__schema_admission__Database__register_table_index(groove_Database *db, const char *table, u32 tn, groove_IndexSchema index) {
    reset_batch_preparation_owner(db);
    jz_err e = groove__db__commit__Database__ensure_not_poisoned(db);
    const groove_TableSchema *existing = NULL;
    if (!e.code) e = groove__db__commit__Database__table(db, table, tn, &existing);
    if (e.code) goto fail;
    /* Rust clones `existing` to release the borrow; it is only read before the runtime call here. */
    JZ_VEC_FOR(const groove_IndexSchema, registered, existing->indices) {
        if (!jz_str_eq(&registered->name, &index.name)) continue;
        if (!groove__schema__IndexSchema__PartialEq__eq(registered, &index))
            e = groove__db__Error__pair(GROOVE_DB_TableIndexDefinitionMismatch, table, tn, index.name.ptr, index.name.len);
        goto fail; /* Ok(()) when identical */
    }
    JZ_VEC_FOR(const jz_str, column, index.columns) {
        if (!table_has_column(existing, column)) {
            e = groove__db__Error__table_index_field_not_found(table, tn, index.name.ptr, index.name.len, column->ptr, column->len);
            goto fail;
        }
    }
    if (db->resident_publications.len != 0) {
        e = groove__db__Error__pair(GROOVE_DB_TableIndexRegistrationWhilePublicationsResident, table, tn, index.name.ptr, index.name.len);
        goto fail;
    }
    e = groove__ivm__runtime__IvmRuntime__register_table_index(db->ivm_runtime, table, tn, index, groove__storage__LayoutStorage__as_dyn(db->storage));
    if (e.code && e.code != GROOVE_EXT_IVM_UNIQUE_INDEX_VIOLATION) db->poisoned = true;
    return e;
fail:
    groove__schema__IndexSchema__drop(&index);
    return e;
}

/* Define one fixed-output projection family for a heterogeneous table. */
jz_err groove__db__schema_admission__Database__define_variant_projection(groove_Database *db, const char *table, u32 tn, const char *target, u32 gn,
                                                                         groove_RecordDescriptor output) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    return groove__ivm__runtime__IvmRuntime__define_variant_projection(db->ivm_runtime, table, tn, target, gn, output);
}

/* Append one source-version case to a fixed-output variant projection. */
jz_err groove__db__schema_admission__Database__register_variant_projection_case(groove_Database *db, const char *table, u32 tn, const char *target, u32 gn, u32 variant_tag,
                                                                                const groove_ProjectField *fields, u32 n) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    return groove__ivm__runtime__IvmRuntime__register_variant_projection_case(db->ivm_runtime, table, tn, target, gn, variant_tag, fields, n);
}

/* Append a schema-read projection case that omits only rows containing an
 * enum case the target descriptor cannot represent. */
jz_err groove__db__schema_admission__Database__register_variant_projection_case_omitting_unrepresentable_enums(groove_Database *db, const char *table, u32 tn,
                                                                                                               const char *target, u32 gn, u32 variant_tag,
                                                                                                               const groove_ProjectField *fields, u32 n) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    return groove__ivm__runtime__IvmRuntime__register_variant_projection_case_omitting_unrepresentable_enums(db->ivm_runtime, table, tn, target, gn, variant_tag, fields, n);
}

/* Append one generic source-case mapping to a fixed-output projection. */
jz_err groove__db__schema_admission__Database__register_variant_case(groove_Database *db, const char *table, u32 tn, const char *target, u32 gn, u32 variant_tag,
                                                                     const groove_ProjectField *fields, u32 n) {
    return groove__db__schema_admission__Database__register_variant_projection_case(db, table, tn, target, gn, variant_tag, fields, n);
}

/* Refresh an existing raw projection case after append-only enum registry evolution. */
jz_err groove__db__schema_admission__Database__refresh_variant_case_for_registry_evolution(groove_Database *db, const char *table, u32 tn, const char *target, u32 gn,
                                                                                           u32 variant_tag, const groove_ProjectField *fields, u32 n) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    return groove__ivm__runtime__IvmRuntime__refresh_variant_projection_case_for_registry_evolution(db->ivm_runtime, table, tn, target, gn, variant_tag, fields, n);
}

/* Append a physical source case that constructs one stable logical enum value. */
jz_err groove__db__schema_admission__Database__register_variant_projection_enum_case(groove_Database *db, const char *table, u32 tn, const char *target, u32 gn,
                                                                                     u32 variant_tag, const char *enum_field, u32 fln, const groove_EnumSchema *enum_schema,
                                                                                     const char *case_, u32 cn, const groove_ProjectField *fields, u32 n) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    return groove__ivm__runtime__IvmRuntime__register_variant_projection_enum_case(db->ivm_runtime, table, tn, target, gn, variant_tag, enum_field, fln, enum_schema, case_, cn,
                                                                                  fields, n);
}

/* `u32`-tag convenience alias for generic table variants. */
jz_err groove__db__schema_admission__Database__register_variant_enum_case(groove_Database *db, const char *table, u32 tn, const char *target, u32 gn, u32 variant_tag,
                                                                          const char *enum_field, u32 fln, const groove_EnumSchema *enum_schema, const char *case_, u32 cn,
                                                                          const groove_ProjectField *fields, u32 n) {
    return groove__db__schema_admission__Database__register_variant_projection_enum_case(db, table, tn, target, gn, variant_tag, enum_field, fln, enum_schema, case_, cn,
                                                                                        fields, n);
}

/* Mark one source version as intentionally absent from a fixed-output projection. */
jz_err groove__db__schema_admission__Database__register_variant_projection_ignore_case(groove_Database *db, const char *table, u32 tn, const char *target, u32 gn,
                                                                                       u32 variant_tag) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    return groove__ivm__runtime__IvmRuntime__register_variant_projection_ignore_case(db->ivm_runtime, table, tn, target, gn, variant_tag);
}

jz_err groove__db__schema_admission__Database__register_variant_ignore_case(groove_Database *db, const char *table, u32 tn, const char *target, u32 gn, u32 variant_tag) {
    return groove__db__schema_admission__Database__register_variant_projection_ignore_case(db, table, tn, target, gn, variant_tag);
}

/* Apply one registered fixed-output projection to an already decoded
 * heterogeneous row. `Ignore` returns *some = false. */
jz_err groove__db__schema_admission__Database__project_variant_record(const groove_Database *db, const char *table, u32 tn, const char *target, u32 gn,
                                                                      const groove_VariantRecord *record, bool *some, groove_OwnedRecord *out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    return groove__ivm__runtime__IvmRuntime__project_variant_record(db->ivm_runtime, table, tn, target, gn, record, some, out);
}
