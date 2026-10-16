/* groove::ivm::runtime::schema: runtime schema registration, variant
 * projection, and snapshot reads. */
#include "mod.h"

#define RT(name) groove__ivm__runtime__IvmRuntime__##name
#define SCH(name) groove__ivm__runtime__schema__IvmRuntime__##name
#define RTK(name) groove__ivm__runtime__runtime_tick__IvmRuntime__##name

static int str_cmp_v(const void *a, const void *b, void *ctx) { (void)ctx; return jz_str_cmp(a, b); }
static void str_drop_v(void *s) { jz_str_drop(s); }
const jz_btree_desc groove_ivm_rt_STR_COMPILED_MAP = { sizeof(jz_str), sizeof(groove_CompiledNode), str_cmp_v, str_drop_v, NULL };

static jz_str borrowed(const char *p, u32 n) { jz_str s = { (char *)p, n, 0 }; return s; }
static void table_schema_drop_v(void *t) { groove_TableSchema_drop(t); }
static void table_schema_clone_v(void *d, const void *s) { groove_TableSchema_clone(d, s); }

static bool record_descriptors_registry_compatible(groove_RecordDescriptor left, groove_RecordDescriptor right) {
    return groove__records__RecordDescriptor__registry_compatible_with(left, right);
}

static jz_err define_variant_projection_target(groove_IvmRuntime *self, const char *table, u32 len, const groove_VariantProjectionTarget *target, groove_RecordDescriptor output);
static jz_err register_variant_projection_target_case(groove_IvmRuntime *self, const char *table, u32 len, const groove_VariantProjectionTarget *target, u32 variant_tag,
                                                      const groove_ProjectField *fields, u32 nfields, bool omit_unrepresentable_enum_rows, bool allow_registry_refresh);
static jz_err register_schema_index_variant_case(groove_IvmRuntime *self, const groove_TableSchema *table, const groove_IndexSchema *index, u32 variant_tag);

bool RT(declared_index_repair_is_idle)(const groove_IvmRuntime *self) {
    return self->multisink_subscriptions.len == 0
        && self->prepared_shapes.len == 0
        && !groove__ivm__runtime__runtime_tick__PendingIncrementalEvaluation__is_pending(&self->pending_incremental)
        && !self->pending_incremental_polling
        && self->pending_binding_retractions.len == 0
        && self->deferred_notifications.len == 0
        && !*self->persistence_indeterminate;
}

/* Resolve the single-input Persist node of a schema index. */
static jz_err persist_parts(const groove_IvmRuntime *self, groove_NodeId persist, groove_OpType *op, groove_NodeId *input) {
    const groove_GraphNode *node = groove__ivm__graph__IvmGraph__node(self->graph, persist);
    if (!node) return GROOVE_IVM_ERR_N(GROOVE_IVM_GraphNodeNotFound, persist);
    if (node->descriptor.operator_.tag != groove_Op_Persist) return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator);
    if (node->descriptor.inputs.len != 1) return GROOVE_IVM_ERR_N(GROOVE_IVM_GraphInputArityMismatch, persist);
    groove_OpType_clone(op, &node->descriptor.operator_);
    *input = node->descriptor.inputs.ptr[0];
    return JZ_OK;
}

/* Replay only schema-owned TableSource -> IndexBy -> Persist graphs.
 * Startup callers must exclude publications and queries for the duration. */
jz_err RT(rebuild_declared_indexes)(groove_IvmRuntime *self, groove_Storage *storage) {
    enum { BATCH_SIZE = 1024 };
    JZ_VEC(groove_TableSchema) tables = {0};
    jz_vec_clone(&tables, &self->schema.tables, sizeof(groove_TableSchema), table_schema_clone_v);
    jz_err e = JZ_OK;
    JZ_VEC_FOR(const groove_TableSchema, table, tables) {
        JZ_VEC_FOR(const groove_IndexSchema, index, table->indices) {
            groove_NodeId persist;
            e = groove__ivm__runtime__compilation__IvmRuntime__add_dedup_schema_index(self, table, index, &persist);
            if (JZ_IS_ERR(e)) goto done;
            groove_OpType op; groove_NodeId input;
            e = persist_parts(self, persist, &op, &input);
            if (JZ_IS_ERR(e)) goto done;
            const groove_PersistOp *p = &op.v.persist;
            /* Repeated bounded prefix scans avoid retaining all old keys and
             * never touch primary rows or another derived object's prefix. */
            for (;;) {
                groove_ScanRequest req = groove__storage__ScanRequest__prefix(jz_str_clone(&p->storage.column_family), jz_bytes_clone(&p->storage.key_prefix));
                groove__storage__ScanRequest__with_max_items(&req, BATCH_SIZE);
                groove_KvPairVec entries = {0};
                e = storage->vt->scan(storage->self, &req, &entries);
                groove_ScanRequest_drop(&req);
                if (JZ_IS_ERR(e)) { groove_OpType_drop(&op); goto done; }
                if (entries.len == 0) { groove_KvPairVec_drop(&entries); break; }
                groove_WriteOpVec ops = {0};
                jz_vec_reserve(&ops, entries.len, sizeof(groove_OwnedWriteOperation));
                JZ_VEC_FOR(groove_KvPair, kv, entries) {
                    groove_OwnedWriteOperation del = { groove_WriteOp_Delete, jz_str_clone(&p->storage.column_family), kv->key, {0} };
                    kv->key.ptr = NULL; kv->key.len = kv->key.cap = 0;
                    JZ_VEC_PUSH(ops, del);
                }
                groove_KvPairVec_drop(&entries);
                e = storage->vt->write_batch(storage->self, &ops);
                jz_vec_drop(&ops, sizeof(groove_OwnedWriteOperation), groove_OwnedWriteOperation_drop);
                if (JZ_IS_ERR(e)) { groove_OpType_drop(&op); goto done; }
            }
            SCH(invalidate_table_inputs)(self, table->name.ptr, table->name.len);
            groove_RecordDeltas snapshot;
            e = RTK(hydration_snapshot)(self, input, storage, groove_HydrationMode_Ordinary, &snapshot);
            if (JZ_IS_ERR(e)) { groove_OpType_drop(&op); goto done; }
            /* Hydration currently materializes one index snapshot. Bound write
             * batches independently of that existing read machinery. */
            for (u32 start = 0; start < snapshot.deltas.len; start += BATCH_SIZE) {
                u32 n = snapshot.deltas.len - start < BATCH_SIZE ? snapshot.deltas.len - start : BATCH_SIZE;
                groove_RecordDeltas chunk = { snapshot.descriptor, {0} };
                jz_vec_reserve(&chunk.deltas, n, sizeof(groove_RecordDelta));
                for (u32 i = 0; i < n; i++) groove_RecordDelta_clone(jz_vec_push_slot(&chunk.deltas, sizeof(groove_RecordDelta)), &snapshot.deltas.ptr[start + i]);
                e = groove__ivm__runtime__persist__apply_persist_delta(*storage, &p->storage, p->key_fields.ptr, p->key_fields.len, p->unique, &chunk);
                groove_RecordDeltas_drop(&chunk);
                if (JZ_IS_ERR(e)) break;
            }
            groove_RecordDeltas_drop(&snapshot);
            groove_OpType_drop(&op);
            if (JZ_IS_ERR(e)) goto done;
        }
        if (table->indices.len) {
            /* Repair hydration is temporary startup work, not a query warmup.
             * Release its primary/index snapshots even when no later write
             * advances this table's frontier (notably empty deletion tables),
             * before recovery or user reads begin. */
            SCH(invalidate_table_inputs)(self, table->name.ptr, table->name.len);
        }
    }
done:
    jz_vec_drop(&tables, sizeof(groove_TableSchema), table_schema_drop_v);
    return e;
}

static void put_desc(jz_hmap *m, const jz_str *name, groove_RecordDescriptor d) {
    jz_str key = jz_str_clone(name);
    if (jz_hmap_insert(m, &key, &d, NULL)) jz_str_drop(&key);
}

static jz_hmap *variant_descs_entry(groove_IvmRuntime *self, const jz_str *name) {
    jz_str key = jz_str_clone(name);
    bool inserted;
    jz_hmap *descriptors = jz_hmap_entry(&self->variant_descriptors, &key, &inserted);
    if (inserted) jz_hmap_init(descriptors, &groove_ivm_rt_U32_DESC_MAP);
    else jz_str_drop(&key);
    return descriptors;
}

jz_err RT(register_table)(groove_IvmRuntime *self, groove_TableSchema table) {
    if (groove__schema__DatabaseSchema__table(&self->schema, table.name.ptr, table.name.len)) {
        jz_err e = GROOVE_IVM_ERR_S(GROOVE_IVM_TableAlreadyExists, &table.name);
        groove_TableSchema_drop(&table);
        return e;
    }
    put_desc(&self->table_storage_descriptors, &table.name, groove__schema__TableSchema__record_schema(&table));
    if (!groove__schema__TableSchema__has_variants(&table)) {
        put_desc(&self->table_descriptors, &table.name, groove__schema__TableSchema__record_schema(&table));
    } else {
        jz_hmap *descriptors = variant_descs_entry(self, &table.name);
        JZ_VEC_FOR(const groove_TableVariant, variant, table.variants) {
            groove_RecordDescriptor d;
            if (groove__schema__TableSchema__record_schema_for_variant(&table, variant->tag, &d))
                jz_hmap_insert(descriptors, &variant->tag, &d, NULL);
        }
    }
    JZ_VEC_PUSH(self->schema.tables, table);
    JZ_TRY(SCH(define_schema_index_variant_projections)(self));
    JZ_TRY(groove__ivm__runtime__subscriptions__IvmRuntime__add_dedup_schema_indices(self));
    return JZ_OK;
}

static groove_TableSchema *find_table_mut(groove_IvmRuntime *self, const char *table, u32 len) {
    JZ_VEC_FOR(groove_TableSchema, candidate, self->schema.tables)
        if (candidate->name.len == len && memcmp(candidate->name.ptr, table, len) == 0) return candidate;
    return NULL;
}

jz_err RT(register_table_variant_with_columns)(groove_IvmRuntime *self, const char *table, u32 len, groove_ColumnSchema *columns, u32 ncols, groove_TableVariant variant_tag) {
    jz_str tname = borrowed(table, len);
    jz_err e = JZ_OK;
    u32 consumed = 0;
    groove_TableSchema *table_schema = find_table_mut(self, table, len);
    if (!table_schema) { e = GROOVE_IVM_ERR_S(GROOVE_IVM_TableNotFound, &tname); goto fail; }
    if (groove__schema__TableSchema__variant(table_schema, variant_tag.tag)) {
        e = groove__ivm__runtime__IvmRuntimeError__table_target_version(GROOVE_IVM_DuplicateTableVariant, &tname, "", 0, variant_tag.tag);
        goto fail;
    }
    for (; consumed < ncols; consumed++) {
        groove_ColumnSchema *column = &columns[consumed];
        bool exists = false;
        JZ_VEC_FOR(const groove_ColumnSchema, existing, table_schema->columns)
            if (jz_str_eq(&existing->name, &column->name)) { exists = true; break; }
        if (exists) {
            e = groove__ivm__runtime__IvmRuntimeError__table_target_field(GROOVE_IVM_TableFieldAlreadyExists, &tname, "", 0, column->name.ptr, column->name.len);
            goto fail;
        }
        JZ_VEC_PUSH(table_schema->columns, *column);
    }
    u32 version = variant_tag.tag;
    JZ_VEC_FOR(const groove_TableVariantField, field, variant_tag.payload_fields)
        groove__records__values__ValueType__collect_variant_registries(&field->value_type, &table_schema->value_variant_registries);
    JZ_VEC_PUSH(table_schema->variants, variant_tag);
    {
        jz_str key = jz_str_from_slice(jz_slice_of(table, len));
        put_desc(&self->table_storage_descriptors, &key, groove__schema__TableSchema__record_schema(table_schema));
        groove_RecordDescriptor descriptor;
        if (!groove__schema__TableSchema__record_schema_for_variant(table_schema, version, &descriptor))
            jz_panic("the newly registered variant exists");
        jz_hmap *descriptors = variant_descs_entry(self, &key);
        jz_hmap_insert(descriptors, &version, &descriptor, NULL);
        jz_str_drop(&key);
    }
    groove_TableSchema snapshot; groove_TableSchema_clone(&snapshot, table_schema);
    JZ_VEC_FOR(const groove_IndexSchema, index, snapshot.indices) {
        e = register_schema_index_variant_case(self, &snapshot, index, version);
        if (JZ_IS_ERR(e)) { groove_TableSchema_drop(&snapshot); return e; }
    }
    groove_TableSchema_drop(&snapshot);
    SCH(invalidate_table_inputs)(self, table, len);
    return JZ_OK;
fail:
    for (u32 i = consumed; i < ncols; i++) groove_ColumnSchema_drop(&columns[i]);
    groove_TableVariant_drop(&variant_tag);
    return e;
}

static bool shared_column_is(const groove_TableVariantField *field, const jz_str *name) {
    return field->has_shared && jz_str_eq(&field->shared_column, name);
}

jz_err RT(evolve_table_variant_registries)(groove_IvmRuntime *self, const char *table, u32 len, const groove_ColumnSchema *columns, u32 n) {
    jz_str tname = borrowed(table, len);
    groove_TableSchema *table_schema = find_table_mut(self, table, len);
    if (!table_schema) return GROOVE_IVM_ERR_S(GROOVE_IVM_TableNotFound, &tname);
    for (u32 i = 0; i < n; i++) {
        const groove_ColumnSchema *desired = &columns[i];
        const groove_ColumnSchema *existing = NULL;
        JZ_VEC_FOR(const groove_ColumnSchema, column, table_schema->columns)
            if (jz_str_eq(&column->name, &desired->name)) { existing = column; break; }
        if (!existing) continue;
        if (!groove__records__values__ValueType__can_evolve_registry_to(&existing->column_type, &desired->column_type))
            return groove__ivm__runtime__IvmRuntimeError__table_target_field(GROOVE_IVM_TableFieldAlreadyExists, &tname, "", 0, desired->name.ptr, desired->name.len);
        JZ_VEC_FOR(const groove_TableVariant, variant, table_schema->variants) {
            JZ_VEC_FOR(const groove_TableVariantField, field, variant->payload_fields) {
                if (!shared_column_is(field, &desired->name)) continue;
                if (!groove__records__values__ValueType__can_evolve_registry_to(&field->value_type, &desired->column_type))
                    return groove__ivm__runtime__IvmRuntimeError__table_target_field(GROOVE_IVM_TableFieldAlreadyExists, &tname, "", 0, desired->name.ptr, desired->name.len);
            }
        }
    }
    for (u32 i = 0; i < n; i++) {
        const groove_ColumnSchema *desired = &columns[i];
        groove_ColumnSchema *existing = NULL;
        JZ_VEC_FOR(groove_ColumnSchema, column, table_schema->columns)
            if (jz_str_eq(&column->name, &desired->name)) { existing = column; break; }
        if (!existing) continue;
        groove_ValueType_drop(&existing->column_type);
        groove_ValueType_clone(&existing->column_type, &desired->column_type);
        JZ_VEC_FOR(groove_TableVariant, variant, table_schema->variants) {
            JZ_VEC_FOR(groove_TableVariantField, field, variant->payload_fields) {
                if (shared_column_is(field, &desired->name)) {
                    groove_ValueType_drop(&field->value_type);
                    groove_ValueType_clone(&field->value_type, &desired->column_type);
                }
            }
        }
        groove__records__values__ValueType__collect_variant_registries(&desired->column_type, &table_schema->value_variant_registries);
    }
    jz_str key = jz_str_from_slice(jz_slice_of(table, len));
    put_desc(&self->table_storage_descriptors, &key, groove__schema__TableSchema__record_schema(table_schema));
    if (!groove__schema__TableSchema__has_variants(table_schema)) {
        /* Registry evolution changes column types; the plain-table descriptor
         * must follow, as registration keeps it. */
        put_desc(&self->table_descriptors, &key, groove__schema__TableSchema__record_schema(table_schema));
    }
    jz_hmap descriptors; jz_hmap_init(&descriptors, &groove_ivm_rt_U32_DESC_MAP);
    JZ_VEC_FOR(const groove_TableVariant, variant, table_schema->variants) {
        groove_RecordDescriptor descriptor;
        if (!groove__schema__TableSchema__record_schema_for_variant(table_schema, variant->tag, &descriptor))
            jz_panic("registered variants have valid descriptors");
        jz_hmap_insert(&descriptors, &variant->tag, &descriptor, NULL);
    }
    jz_hmap old;
    if (jz_hmap_insert(&self->variant_descriptors, &key, &descriptors, &old)) { jz_hmap_drop(&old); jz_str_drop(&key); }
    SCH(invalidate_table_inputs)(self, table, len);
    return JZ_OK;
}

static jz_err register_table_index_staged(groove_IvmRuntime *self, const char *table, u32 len, groove_IndexSchema index, groove_Storage *storage);

jz_err RT(register_table_index)(groove_IvmRuntime *self, const char *table, u32 len, groove_IndexSchema index, groove_Storage *storage) {
    groove_IvmRuntime staged;
    groove_IvmRuntime_clone(&staged, self);
    jz_err e = register_table_index_staged(&staged, table, len, index, storage);
    if (JZ_IS_ERR(e)) {
        groove_IvmRuntime *tmp = JZ_NEW(groove_IvmRuntime);
        *tmp = staged;
        groove_IvmRuntime_drop(tmp);
        return e;
    }
    /* *self = staged */
    groove_IvmRuntime *old = JZ_NEW(groove_IvmRuntime);
    *old = *self;
    *self = staged;
    groove_IvmRuntime_drop(old);
    return JZ_OK;
}

static jz_err register_table_index_staged(groove_IvmRuntime *self, const char *table, u32 len, groove_IndexSchema index, groove_Storage *storage) {
    jz_str tname = borrowed(table, len);
    groove_TableSchema *slot = find_table_mut(self, table, len);
    if (!slot) { groove_IndexSchema_drop(&index); return GROOVE_IVM_ERR_S(GROOVE_IVM_TableNotFound, &tname); }
    groove_IndexSchema copy; groove_IndexSchema_clone(&copy, &index);
    JZ_VEC_PUSH(slot->indices, copy);
    groove_TableSchema table_schema; groove_TableSchema_clone(&table_schema, slot);
    jz_err e = JZ_OK;
    /* A runtime (re)creation always backfills under a fresh durable id:
     * entries a dropped incarnation left behind are never read. */
    groove__ivm__runtime__index_ids__IndexIdRegistry__retire(self->index_ids, table, len, &index.name);
    if (groove__schema__TableSchema__has_variants(&table_schema)) {
        groove_VariantProjectionTarget target = { groove_VPT_SchemaIndex, jz_str_clone(&index.name) };
        groove_RecordDescriptor input;
        e = groove__ivm__runtime__record_projection__schema_index_input_descriptor(&table_schema, &index, &input);
        if (!JZ_IS_ERR(e)) e = define_variant_projection_target(self, table, len, &target, input);
        jz_str_drop(&target.name);
        if (JZ_IS_ERR(e)) goto done;
        JZ_VEC_FOR(const groove_TableVariant, variant, table_schema.variants) {
            e = register_schema_index_variant_case(self, &table_schema, &index, variant->tag);
            if (JZ_IS_ERR(e)) goto done;
        }
    }
    groove_NodeId persist;
    e = groove__ivm__runtime__compilation__IvmRuntime__add_dedup_schema_index(self, &table_schema, &index, &persist);
    if (JZ_IS_ERR(e)) goto done;
    SCH(invalidate_table_inputs)(self, table, len);
    groove_OpType op; groove_NodeId input;
    e = persist_parts(self, persist, &op, &input);
    if (JZ_IS_ERR(e)) goto done;
    groove_RecordDeltas snapshot;
    e = RTK(hydration_snapshot)(self, input, storage, groove_HydrationMode_Ordinary, &snapshot);
    if (JZ_IS_ERR(e)) { groove_OpType_drop(&op); goto done; }
    /* The new index's id registration becomes durable in the same atomic
     * write as its backfilled entries, never after them. */
    groove_PendingIndexIds registrations;
    groove__ivm__runtime__index_ids__IndexIdRegistry__pending(self->index_ids, &registrations);
    groove_WriteOpVec leading = {0};
    jz_vec_reserve(&leading, registrations.operations.len, sizeof(groove_OwnedWriteOperation));
    JZ_VEC_FOR(const groove_OwnedWriteOperation, w, registrations.operations) {
        groove_OwnedWriteOperation c = { w->tag, jz_str_clone(&w->cf), jz_bytes_clone(&w->key), jz_bytes_clone(&w->value) };
        JZ_VEC_PUSH(leading, c);
    }
    const groove_PersistOp *p = &op.v.persist;
    e = groove__ivm__runtime__persist__apply_persist_delta_with(*storage, &p->storage, p->key_fields.ptr, p->key_fields.len, p->unique, &snapshot, leading);
    groove_RecordDeltas_drop(&snapshot);
    groove_OpType_drop(&op);
    if (!JZ_IS_ERR(e)) groove__ivm__runtime__index_ids__IndexIdRegistry__mark_persisted(self->index_ids, &registrations);
    groove_PendingIndexIds_drop(&registrations);
done:
    groove_TableSchema_drop(&table_schema);
    groove_IndexSchema_drop(&index);
    return e;
}

jz_err RT(define_variant_projection)(groove_IvmRuntime *self, const char *table, u32 len, const char *target, u32 tlen, groove_RecordDescriptor output) {
    groove_VariantProjectionTarget t = { groove_VPT_Named, borrowed(target, tlen) };
    return define_variant_projection_target(self, table, len, &t, output);
}

static jz_err define_variant_projection_target(groove_IvmRuntime *self, const char *table, u32 len, const groove_VariantProjectionTarget *target, groove_RecordDescriptor output) {
    jz_str tname = borrowed(table, len);
    if (!groove__schema__DatabaseSchema__table(&self->schema, table, len)) return GROOVE_IVM_ERR_S(GROOVE_IVM_TableNotFound, &tname);
    groove_VariantProjectionKey key;
    key.table = jz_str_from_slice(jz_slice_of(table, len));
    groove_VariantProjectionTarget_clone(&key.target, target);
    bool inserted;
    groove_VariantProjection *entry = jz_hmap_entry(&self->variant_projections, &key, &inserted);
    if (inserted) {
        entry->output = output;
        jz_hmap_init(&entry->cases, &groove_ivm_rt_VARIANT_CASES_MAP);
        return JZ_OK;
    }
    jz_str_drop(&key.table);
    jz_str_drop(&key.target.name);
    if (record_descriptors_registry_compatible(entry->output, output)) {
        entry->output = output;
        return JZ_OK;
    }
    const jz_str *name = groove__ivm__runtime__record_projection__variant_projection_target_name(target);
    return groove__ivm__runtime__IvmRuntimeError__table_target(GROOVE_IVM_VariantProjectionOutputMismatch, &tname, name->ptr, name->len);
}

jz_err RT(register_variant_projection_case)(groove_IvmRuntime *self, const char *table, u32 len, const char *target, u32 tlen, u32 variant_tag, const groove_ProjectField *fields, u32 n) {
    groove_VariantProjectionTarget t = { groove_VPT_Named, borrowed(target, tlen) };
    return register_variant_projection_target_case(self, table, len, &t, variant_tag, fields, n, false, false);
}

/* Refresh a raw variant source descriptor after append-only enum registry
 * evolution. The existing mapping must remain byte-for-byte identical. */
jz_err RT(refresh_variant_projection_case_for_registry_evolution)(groove_IvmRuntime *self, const char *table, u32 len, const char *target, u32 tlen, u32 variant_tag, const groove_ProjectField *fields, u32 n) {
    groove_VariantProjectionTarget t = { groove_VPT_Named, borrowed(target, tlen) };
    return register_variant_projection_target_case(self, table, len, &t, variant_tag, fields, n, false, true);
}

/* Register a schema-read case whose unrepresentable enum values are
 * source-local row exclusions. This keeps old-client compatibility at the
 * source boundary rather than swallowing errors downstream. */
jz_err RT(register_variant_projection_case_omitting_unrepresentable_enums)(groove_IvmRuntime *self, const char *table, u32 len, const char *target, u32 tlen, u32 variant_tag, const groove_ProjectField *fields, u32 n) {
    groove_VariantProjectionTarget t = { groove_VPT_Named, borrowed(target, tlen) };
    return register_variant_projection_target_case(self, table, len, &t, variant_tag, fields, n, true, false);
}

/* Lower ProjectFields over source into a MapProjectOp (shared by the Project
 * and Enum case constructors, inlined twice in Rust). */
static jz_err build_map_project(groove_RecordDescriptor source, const groove_ProjectField *fields, u32 n, groove_MapProjectOp *out) {
    groove_MapProjectOp project; memset(&project, 0, sizeof project);
    jz_err e = JZ_OK;
    for (u32 i = 0; i < n; i++) {
        const groove_FieldRef *source_field = groove__ivm__graph__ProjectField__source(&fields[i]);
        if (!source_field) continue;
        u32 idx;
        e = groove__ivm__runtime__record_projection__resolve_field_ref(source, source_field, &idx);
        if (JZ_IS_ERR(e)) goto fail;
        groove_UsizePair pair = { 0, idx };
        JZ_VEC_PUSH(project.mapping, pair);
    }
    for (u32 i = 0; i < n; i++) {
        groove_ProjectionExpr x; memset(&x, 0, sizeof x);
        e = groove__ivm__runtime__record_projection__project_field_expr(source, &fields[i], &x.expression);
        if (JZ_IS_ERR(e)) goto fail;
        x.has_output_name = true;
        x.output_name = jz_str_clone(&fields[i].output_name);
        x.has_identity = fields[i].has_identity;
        if (x.has_identity) groove_FieldIdentity_clone(&x.output_identity, &fields[i].output_identity);
        JZ_VEC_PUSH(project.expressions, x);
    }
    *out = project;
    return JZ_OK;
fail:
    groove_MapProjectOp_drop(&project);
    return e;
}

static jz_err variant_source(const groove_IvmRuntime *self, const char *table, u32 len, u32 variant_tag, groove_RecordDescriptor *source) {
    jz_str tname = borrowed(table, len);
    const groove_TableSchema *t = groove__schema__DatabaseSchema__table(&self->schema, table, len);
    if (!t) return GROOVE_IVM_ERR_S(GROOVE_IVM_TableNotFound, &tname);
    if (!groove__schema__TableSchema__record_schema_for_variant(t, variant_tag, source))
        return groove__ivm__runtime__IvmRuntimeError__table_target_version(GROOVE_IVM_UnknownTableVariant, &tname, "", 0, variant_tag);
    return JZ_OK;
}

/* Append a physical source case that constructs one stable logical enum
 * value. The outer projection descriptor stays fixed; the selected enum case
 * owns the dense payload descriptor used for this source layout. */
jz_err RT(register_variant_projection_enum_case)(groove_IvmRuntime *self, const char *table, u32 len, const char *target, u32 tlen, u32 variant_tag,
    const char *enum_field, u32 flen, const groove_EnumSchema *enum_schema, const char *enum_case, u32 clen, const groove_ProjectField *fields, u32 n) {
    jz_str tname = borrowed(table, len);
    groove_RecordDescriptor source;
    JZ_TRY(variant_source(self, table, len, variant_tag, &source));
    groove_VariantProjectionKey key = { tname, { groove_VPT_Named, borrowed(target, tlen) } };
    groove_VariantProjection *projection = jz_hmap_get(&self->variant_projections, &key);
    if (!projection) return groove__ivm__runtime__IvmRuntimeError__table_target(GROOVE_IVM_VariantProjectionNotFound, &tname, target, tlen);
    u32 nout;
    const groove_DescriptorField *output_fields = groove__records__RecordDescriptor__fields(projection->output, &nout);
    u32 union_idx;
    if (!groove__records__RecordDescriptor__field_index(projection->output, enum_field, flen, &union_idx))
        return groove__ivm__runtime__IvmRuntimeError__table_target_field(GROOVE_IVM_VariantProjectionEnumFieldNotFound, &tname, target, tlen, enum_field, flen);
    if (nout != 1)
        return groove__ivm__runtime__IvmRuntimeError__table_target(GROOVE_IVM_VariantProjectionEnumOutputMustBeSingleField, &tname, target, tlen);
    const groove_ValueType *vt = &output_fields[union_idx].value_type;
    if (vt->tag != groove_ValueType_Enum)
        return groove__ivm__runtime__IvmRuntimeError__table_target_field(GROOVE_IVM_VariantProjectionEnumFieldTypeMismatch, &tname, target, tlen, enum_field, flen);
    if (!groove_EnumSchema_eq(vt->v.enm, enum_schema))
        return groove__ivm__runtime__IvmRuntimeError__table_target_field(GROOVE_IVM_VariantProjectionEnumSchemaMismatch, &tname, target, tlen, enum_field, flen);
    u32 tag;
    JZ_TRY(groove__records__values__EnumSchema__tag(enum_schema, enum_case, clen, &tag));
    const groove_EnumCase *ecase;
    JZ_TRY(groove__records__values__EnumSchema__case(enum_schema, tag, &ecase));
    groove_RecordDescriptor payload = ecase->payload;
    groove_RecordDescriptor projected;
    JZ_TRY(groove__ivm__runtime__record_projection__project_descriptor(source, fields, n, &projected));
    if (projected != payload)
        return groove__ivm__runtime__IvmRuntimeError__table_target_field(GROOVE_IVM_VariantProjectionEnumPayloadMismatch, &tname, target, tlen, enum_case, clen);
    groove_VariantProjectionCase c; memset(&c, 0, sizeof c);
    JZ_TRY(build_map_project(source, fields, n, &c.project));
    jz_err e = groove__ivm__runtime__record_projection__raw_projection_fields(&c.project, source, payload, &c.raw_projection);
    if (JZ_IS_ERR(e)) { groove_MapProjectOp_drop(&c.project); return e; }
    c.tag = groove_VPC_Enum;
    c.source = source;
    c.enum_tag = tag;
    c.payload = payload;
    /* Neither outcome replaces a case, so no retained result is stale; see
     * register_variant_projection_target_case. */
    bool inserted;
    groove_VariantProjectionCase *slot = jz_hmap_entry(&projection->cases, &variant_tag, &inserted);
    if (inserted) { *slot = c; return JZ_OK; }
    bool same = groove_VariantProjectionCase_eq(slot, &c);
    groove_VariantProjectionCase_drop(&c);
    if (same) return JZ_OK;
    return groove__ivm__runtime__IvmRuntimeError__table_target_version(GROOVE_IVM_VariantProjectionCaseAlreadyRegistered, &tname, target, tlen, variant_tag);
}

jz_err RT(register_variant_projection_ignore_case)(groove_IvmRuntime *self, const char *table, u32 len, const char *target, u32 tlen, u32 variant_tag) {
    groove_VariantProjectionTarget t = { groove_VPT_Named, borrowed(target, tlen) };
    return register_variant_projection_target_case(self, table, len, &t, variant_tag, NULL, 0, false, false);
}

jz_err RT(project_variant_record)(const groove_IvmRuntime *self, const char *table, u32 len, const char *target, u32 tlen, const groove_VariantRecordB *record, bool *some, groove_OwnedRecord *out) {
    jz_str tname = borrowed(table, len);
    groove_VariantProjectionKey key = { tname, { groove_VPT_Named, borrowed(target, tlen) } };
    const groove_VariantProjection *projection = jz_hmap_get(&self->variant_projections, &key);
    if (!projection) return groove__ivm__runtime__IvmRuntimeError__table_target(GROOVE_IVM_VariantProjectionNotFound, &tname, target, tlen);
    u32 variant_tag = record->variant_tag;
    const groove_VariantProjectionCase *c = jz_hmap_get(&projection->cases, &variant_tag);
    if (!c) return groove__ivm__runtime__IvmRuntimeError__table_target_version(GROOVE_IVM_VariantProjectionCaseNotFound, &tname, target, tlen, variant_tag);
    if (!groove__records__RecordDescriptor__registry_compatible_with(groove__ivm__runtime__VariantProjectionCase__source(c), record->record.descriptor))
        return groove__ivm__runtime__IvmRuntimeError__table_target_version(GROOVE_IVM_VariantProjectionSourceMismatch, &tname, target, tlen, variant_tag);
    groove_RecordDeltas input = { record->record.descriptor, {0} };
    groove_RecordDelta d = { jz_bytes_from(record->record.raw.ptr, record->record.raw.len), 1 };
    JZ_VEC_PUSH(input.deltas, d);
    groove_RecordDeltas projected;
    jz_err e;
    switch (c->tag) {
    case groove_VPC_Project:
        e = groove__ivm__runtime__operator_updates__NodeState__update_map_project(&c->project, projection->output, &input, c->raw_projection, c->omit_unrepresentable_enum_rows, &projected);
        break;
    case groove_VPC_Enum:
        e = groove__ivm__runtime__operator_updates__NodeState__update_variant_enum_project(c->enum_tag, c->payload, &c->project, projection->output, &input, c->raw_projection, &projected);
        break;
    default: /* Ignore */
        groove_RecordDeltas_drop(&input);
        *some = false;
        return JZ_OK;
    }
    groove_RecordDeltas_drop(&input);
    if (JZ_IS_ERR(e)) return e;
    if (projected.deltas.len == 0) { groove_RecordDeltas_drop(&projected); *some = false; return JZ_OK; }
    jz_bytes raw = jz_bytes_clone(&projected.deltas.ptr[0].record); /* record.record.to_vec() */
    groove_RecordDeltas_drop(&projected);
    *out = groove__records__OwnedRecord__new(raw, projection->output);
    *some = true;
    return JZ_OK;
}

static jz_err register_variant_projection_target_case(groove_IvmRuntime *self, const char *table, u32 len, const groove_VariantProjectionTarget *target, u32 variant_tag,
                                                      const groove_ProjectField *fields, u32 nfields, bool omit_unrepresentable_enum_rows, bool allow_registry_refresh) {
    jz_str tname = borrowed(table, len);
    groove_RecordDescriptor source;
    JZ_TRY(variant_source(self, table, len, variant_tag, &source));
    groove_VariantProjectionKey key = { tname, *target };
    groove_VariantProjection *projection = jz_hmap_get(&self->variant_projections, &key);
    const jz_str *target_name = groove__ivm__runtime__record_projection__variant_projection_target_name(target);
    if (!projection) return groove__ivm__runtime__IvmRuntimeError__table_target(GROOVE_IVM_VariantProjectionNotFound, &tname, target_name->ptr, target_name->len);
    /* An explicitly typed literal is also a descriptor boundary: it does not
     * copy the source cell at all, so a physical enum registry in the input
     * cannot leak through it. Jazz uses this for requirement-none auxiliary
     * sources, whose enum cells are deliberately typed-null. */
    bool allow_recursive_replacement = false;
    for (u32 i = 0; fields && i < nfields; i++)
        if (fields[i].expression.tag == groove_ProjectExpr_RecursiveEnumRemap || fields[i].expression.tag == groove_ProjectExpr_TypedLiteral) { allow_recursive_replacement = true; break; }
    groove_VariantProjectionCase c; memset(&c, 0, sizeof c);
    if (fields) {
        groove_RecordDescriptor projected;
        JZ_TRY(groove__ivm__runtime__record_projection__project_descriptor(source, fields, nfields, &projected));
        /* A recursive enum projector is an explicit descriptor boundary: its
         * source and output may intentionally name different enum registries.
         * The operation validates and re-encodes the values against the fixed
         * output descriptor at execution time, so the usual raw-descriptor
         * compatibility shortcut is inapplicable. */
        if (!allow_recursive_replacement && !record_descriptors_registry_compatible(projected, projection->output))
            return groove__ivm__runtime__IvmRuntimeError__table_target(GROOVE_IVM_VariantProjectionOutputMismatch, &tname, target_name->ptr, target_name->len);
        JZ_TRY(build_map_project(source, fields, nfields, &c.project));
        jz_err e = groove__ivm__runtime__record_projection__raw_projection_fields(&c.project, source, projection->output, &c.raw_projection);
        if (JZ_IS_ERR(e)) { groove_MapProjectOp_drop(&c.project); return e; }
        c.tag = groove_VPC_Project;
        c.source = source;
        c.omit_unrepresentable_enum_rows = omit_unrepresentable_enum_rows;
    } else {
        c.tag = groove_VPC_Ignore;
        c.source = source;
    }
    /* Only replacing a case can make a retained result stale. Every reader of
     * a projection fails on a row whose tag has no case, so no result that
     * exists was computed from one: adding a case for a new tag (in particular
     * the first case of a new target) leaves them all valid. Invalidating the
     * whole table here dropped every hydration memo over it each time a query
     * registered its own projection target (#3797). */
    bool replaced;
    bool inserted;
    groove_VariantProjectionCase *slot = jz_hmap_entry(&projection->cases, &variant_tag, &inserted);
    if (inserted) {
        *slot = c;
        replaced = false;
    } else if (groove_VariantProjectionCase_eq(slot, &c)) {
        groove_VariantProjectionCase_drop(&c);
        replaced = false;
    } else if (allow_recursive_replacement) {
        /* The physical registry can append while this authored target stays
         * fixed. Refresh its non-total tag map in place. */
        groove_VariantProjectionCase_drop(slot);
        *slot = c;
        replaced = true;
    } else if (allow_registry_refresh && groove__ivm__runtime__VariantProjectionCase__can_refresh_registries_to(slot, &c)) {
        groove_VariantProjectionCase_drop(slot);
        *slot = c;
        replaced = true;
    } else {
        groove_VariantProjectionCase_drop(&c);
        return groove__ivm__runtime__IvmRuntimeError__table_target_version(GROOVE_IVM_VariantProjectionCaseAlreadyRegistered, &tname, target_name->ptr, target_name->len, variant_tag);
    }
    if (replaced) SCH(invalidate_table_inputs)(self, table, len);
    return JZ_OK;
}

jz_err SCH(define_schema_index_variant_projections)(groove_IvmRuntime *self) {
    JZ_VEC(groove_TableSchema) tables = {0};
    jz_vec_clone(&tables, &self->schema.tables, sizeof(groove_TableSchema), table_schema_clone_v);
    jz_err e = JZ_OK;
    JZ_VEC_FOR(const groove_TableSchema, table, tables) {
        if (!groove__schema__TableSchema__has_variants(table)) continue;
        JZ_VEC_FOR(const groove_IndexSchema, index, table->indices) {
            groove_VariantProjectionTarget target = { groove_VPT_SchemaIndex, borrowed(index->name.ptr, index->name.len) };
            groove_RecordDescriptor input;
            e = groove__ivm__runtime__record_projection__schema_index_input_descriptor(table, index, &input);
            if (JZ_IS_ERR(e)) goto done;
            e = define_variant_projection_target(self, table->name.ptr, table->name.len, &target, input);
            if (JZ_IS_ERR(e)) goto done;
            JZ_VEC_FOR(const groove_TableVariant, variant, table->variants) {
                e = register_schema_index_variant_case(self, table, index, variant->tag);
                if (JZ_IS_ERR(e)) goto done;
            }
        }
    }
done:
    jz_vec_drop(&tables, sizeof(groove_TableSchema), table_schema_drop_v);
    return e;
}

static void project_field_drop_v(void *f) { groove_ProjectField_drop(f); }

static jz_err register_schema_index_variant_case(groove_IvmRuntime *self, const groove_TableSchema *table, const groove_IndexSchema *index, u32 variant_tag) {
    const groove_TableVariant *version = groove__schema__TableSchema__variant(table, variant_tag);
    if (!version) return groove__ivm__runtime__IvmRuntimeError__table_target_version(GROOVE_IVM_UnknownTableVariant, &table->name, "", 0, variant_tag);
    groove_StrVec shared_fields = {0};
    JZ_TRY(groove__ivm__runtime__record_projection__schema_index_input_fields(table, index, &shared_fields));
    /* collect::<Option<Vec<_>>>: any unmapped shared column makes this an Ignore case. */
    groove_ProjectFieldVec fields = {0};
    bool all = true;
    JZ_VEC_FOR(const jz_str, shared, shared_fields) {
        const jz_str *local = groove__schema__TableVariant__payload_name_for_shared(version, shared);
        if (!local) { all = false; break; }
        JZ_VEC_PUSH(fields, groove__ivm__graph__ProjectField__renamed(jz_str_clone(local), jz_str_clone(shared)));
    }
    jz_vec_drop(&shared_fields, sizeof(jz_str), str_drop_v);
    groove_VariantProjectionTarget target = { groove_VPT_SchemaIndex, borrowed(index->name.ptr, index->name.len) };
    jz_err e = register_variant_projection_target_case(self, table->name.ptr, table->name.len, &target, variant_tag,
                                                       all ? fields.ptr : NULL, all ? fields.len : 0, false, false);
    jz_vec_drop(&fields, sizeof(groove_ProjectField), project_field_drop_v);
    return e;
}

void SCH(invalidate_table_inputs)(groove_IvmRuntime *self, const char *table, u32 len) {
    jz_str key = jz_str_from_slice(jz_slice_of(table, len));
    bool inserted;
    u64 *frontier = jz_hmap_entry(&self->table_frontiers, &key, &inserted);
    if (inserted) *frontier = 0; else jz_str_drop(&key);
    *frontier += 1;
    jz_hmap invalidated_nodes; jz_hmap_init(&invalidated_nodes, &groove_ivm_rt_NODE_SET);
    groove_NodeId *node; void *keys;
    JZ_HMAP_FOR(&self->eval_memo_keys_by_node, node, keys) {
        const groove_NodeRuntimeMeta *meta = jz_hmap_get(&self->node_meta, node);
        if (!meta || !meta->input_signature) continue;
        const groove_NodeInputSignature *signature = meta->input_signature;
        for (u32 i = 0; i < signature->tables_len; i++) {
            if (signature->tables[i].len == len && memcmp(signature->tables[i].ptr, table, len) == 0) {
                jz_hmap_insert(&invalidated_nodes, node, NULL, NULL);
                break;
            }
        }
    }
    (void)keys;
    groove__ivm__runtime__IvmRuntime__remove_retained_eval_memos_for_nodes(self, &invalidated_nodes);
    jz_hmap_drop(&invalidated_nodes);
}

const groove_IndexSchema *RT(index)(const groove_IvmRuntime *self, const char *table, u32 len, const char *index_name, u32 ilen) {
    const groove_TableSchema *t = RT(table)(self, table, len);
    if (!t) return NULL;
    JZ_VEC_FOR(const groove_IndexSchema, index, t->indices)
        if (index->name.len == ilen && memcmp(index->name.ptr, index_name, ilen) == 0) return index;
    return NULL;
}

const groove_DirectRecordStoreSchema *RT(direct_record_store)(const groove_IvmRuntime *self, const char *store, u32 len) {
    return groove__schema__DatabaseSchema__direct_record_store(&self->schema, store, len);
}

jz_err RT(query_snapshot)(groove_IvmRuntime *self, groove_GraphBuilder graph, groove_Storage *storage, groove_RecordDeltas *out) {
    groove_RootIndirectValuesB materialize = { GROOVE_RIV_MATERIALIZE_B, NULL };
    return RT(query_snapshot_with_root_values)(self, graph, storage, materialize, out);
}

/* Like query_snapshot, choosing how the result presents indirect root values;
 * see RootIndirectValues. */
jz_err RT(query_snapshot_with_root_values)(groove_IvmRuntime *self, groove_GraphBuilder graph, groove_Storage *storage, groove_RootIndirectValuesB root_indirect_values, groove_RecordDeltas *out) {
    jz_err e = RTK(flush_pending_binding_retractions)(self, storage);
    if (JZ_IS_ERR(e)) goto done;
    if (groove__ivm__runtime__subscriptions__builder_contains_binding_source(&graph)) {
        e = GROOVE_IVM_ERR(GROOVE_IVM_BindingSourceRequiresPrepare);
        goto done;
    }
    {
        groove_EphemeralGraphInstall query = groove__ivm__runtime__graph_lifecycle__EphemeralGraphInstall__new(self);
        groove_IvmRuntime *runtime = groove__ivm__runtime__graph_lifecycle__EphemeralGraphInstall__runtime(&query);
        runtime->logical_nodes_requested += (u64)groove__ivm__runtime__subscriptions__count_builder_nodes(&graph);
        groove_CompiledNode compiled;
        e = groove__ivm__runtime__compilation__IvmRuntime__add_dedup_graph(runtime, &graph, &compiled);
        if (!JZ_IS_ERR(e)) {
            groove_RecordDeltas records;
            e = RTK(hydration_snapshot_with_root_values)(runtime, compiled.node, storage, groove_HydrationMode_Ordinary, root_indirect_values, &records);
            root_indirect_values.physical = NULL; /* moved */
            if (!JZ_IS_ERR(e)) {
                if (!groove__records__RecordDescriptor__registry_compatible_with(records.descriptor, compiled.output)) {
                    groove_RecordDeltas_drop(&records);
                    e = GROOVE_IVM_ERR(GROOVE_IVM_GraphOutputMismatch);
                } else {
                    *out = records;
                }
            }
        }
        groove_EphemeralGraphInstall_drop(&query);
    }
done:
    if (root_indirect_values.physical) jz_rc_release(root_indirect_values.physical);
    groove__ivm__graph__GraphBuilder__drop(&graph);
    return e;
}

static void str_set_clone_drop(jz_hmap *m) { jz_hmap_drop(m); }

jz_err RT(query_snapshots)(groove_IvmRuntime *self, const jz_str *names, groove_GraphBuilder *graphs, u32 n, groove_Storage *storage, groove_MultisinkDeltasB *out) {
    jz_err e = RTK(flush_pending_binding_retractions)(self, storage);
    jz_hmap sink_names; jz_hmap_init(&sink_names, &groove_ivm_rt_STR_U64_MAP); /* HashSet<String> (unit values) */
    if (JZ_IS_ERR(e)) goto done;
    if (n == 0) { e = GROOVE_IVM_ERR(GROOVE_IVM_EmptyMultisinkSubscription); goto done; }
    for (u32 i = 0; i < n; i++) {
        jz_str sink = jz_str_clone(&names[i]);
        u64 unit = 0;
        if (jz_hmap_get(&sink_names, &sink)) {
            jz_str_drop(&sink);
            e = GROOVE_IVM_ERR_S(GROOVE_IVM_DuplicateMultisinkSink, &names[i]);
            goto done;
        }
        jz_hmap_insert(&sink_names, &sink, &unit, NULL);
        if (groove__ivm__runtime__subscriptions__builder_contains_binding_source(&graphs[i])) {
            e = GROOVE_IVM_ERR_S(GROOVE_IVM_MultisinkSinkRequiresPrepare, &names[i]);
            goto done;
        }
    }
    {
        groove_EphemeralGraphInstall query = groove__ivm__runtime__graph_lifecycle__EphemeralGraphInstall__new(self);
        groove_IvmRuntime *runtime = groove__ivm__runtime__graph_lifecycle__EphemeralGraphInstall__runtime(&query);
        u64 total = 0;
        for (u32 i = 0; i < n; i++) total += groove__ivm__runtime__subscriptions__count_builder_nodes(&graphs[i]);
        runtime->logical_nodes_requested += total;
        jz_btree outputs; jz_btree_init(&outputs, &groove_ivm_rt_STR_COMPILED_MAP);
        for (u32 i = 0; i < n && !JZ_IS_ERR(e); i++) {
            groove_CompiledNode compiled;
            e = groove__ivm__runtime__compilation__IvmRuntime__add_dedup_graph(runtime, &graphs[i], &compiled);
            if (JZ_IS_ERR(e)) break;
            jz_str sink = jz_str_clone(&names[i]);
            groove_CompiledNode old;
            if (jz_btree_insert(&outputs, &sink, &compiled, &old)) jz_str_drop(&sink);
        }
        if (!JZ_IS_ERR(e)) e = RTK(hydration_snapshots)(runtime, &outputs, storage, groove_HydrationMode_Ordinary, out);
        jz_btree_drop(&outputs);
        groove_EphemeralGraphInstall_drop(&query);
    }
done:
    str_set_clone_drop(&sink_names);
    for (u32 i = 0; i < n; i++) groove__ivm__graph__GraphBuilder__drop(&graphs[i]);
    return e;
}
