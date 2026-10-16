/* Port of crates/jazz/layers/node/src/node/migration_validation.rs */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

typedef jazz_model_ColumnSchema Col;
static void col_drop_v(void *p) { jazz_model__schema__ColumnSchema__drop(p); }
static const jz_btree_desc desc_columns = { sizeof(jz_str), sizeof(Col), jz_cmp_str, jzn_str_drop_v, col_drop_v };

static const jazz_model_TableSchema *find_table(const jazz_model_JazzSchema *s, const jz_str *name) {
    const jazz_model_TableSchemaVec *tables = jazz_model__schema__JazzSchema__tables(s);
    for (u32 i = 0; i < tables->len; i++) if (jz_str_eq(&tables->ptr[i].name, name)) return &tables->ptr[i];
    return NULL;
}
static const Col *find_col(const jazz_model_TableSchema *t, const jz_str *name) {
    for (u32 i = 0; i < t->columns.len; i++) if (jz_str_eq(&t->columns.ptr[i].name, name)) return &t->columns.ptr[i];
    return NULL;
}
static void str_set_from(jz_btree *set, const jz_str *items, u32 n) {
    jz_btree_init(set, &jzn_desc_str_set);
    for (u32 i = 0; i < n; i++) { jz_str c = jz_str_clone(&items[i]); jz_btree_insert(set, &c, NULL, NULL); }
}
static void columns_from(jz_btree *m, const jazz_model_TableSchema *t) {
    jz_btree_init(m, &desc_columns);
    for (u32 i = 0; i < t->columns.len; i++) {
        Col c; jazz_model__schema__ColumnSchema__clone(&c, &t->columns.ptr[i]);
        jz_str k = jz_str_clone(&c.name);
        jz_btree_insert(m, &k, &c, NULL);
    }
}

static jz_err jazz_node__node__migration_validation__validate_transform_column(const Col *column, const jz_str *transform) {
    JZ_TRY(jazz_node__node__validate_registered_transform(transform));
    if (!column) return JZN_INVALID_CATALOGUE("transform column is unknown");
    return JZ_OK;
}

static bool col_eq_v(const void *a, const void *b) { return jazz_model__schema__ColumnSchema__eq(a, b); }

static jz_err validate_table_lens(const jazz_protocol_TableLens *table_lens, const jazz_protocol_SchemaVersion *source, const jazz_protocol_SchemaVersion *target) {
    const jazz_model_TableSchema *source_table = find_table(source->schema, &table_lens->source_table);
    if (!source_table) return JZN_INVALID_CATALOGUE("table lens is unknown");
    const jazz_model_TableSchema *target_table = find_table(target->schema, &table_lens->target_table);
    if (!target_table) return JZN_INVALID_CATALOGUE("table lens is unknown");
    jz_btree target_bindings, branch_columns, columns, target_columns;
    str_set_from(&target_bindings, target_table->branch_by.ptr, target_table->branch_by.len);
    str_set_from(&branch_columns, source_table->branch_by.ptr, source_table->branch_by.len);
    columns_from(&columns, source_table);
    jz_btree_init(&target_columns, &desc_columns);
    bool saw_table_rename = jz_str_eq(&source_table->name, &target_table->name);
    jz_err e = JZ_OK;
    for (u32 i = 0; i < table_lens->ops.len; i++) {
        const jazz_protocol_LensOp *op = &table_lens->ops.ptr[i];
        switch (op->tag) {
        case JAZZ_LENSOP_RENAME_TABLE:
            if (saw_table_rename || !jz_str_eq(&op->a, &source_table->name) || !jz_str_eq(&op->b, &target_table->name)) {
                e = JZN_INVALID_CATALOGUE("table rename does not match lens endpoints"); goto out;
            }
            saw_table_rename = true;
            break;
        case JAZZ_LENSOP_RENAME_COLUMN: {
            if (jz_btree_contains(&columns, &op->b)) { e = JZN_INVALID_CATALOGUE("column rename collides with existing column"); goto out; }
            jz_str ok; Col column;
            if (!jz_btree_remove(&columns, &op->a, &ok, &column)) { e = JZN_INVALID_CATALOGUE("column rename source is unknown"); goto out; }
            jz_str_drop(&ok);
            jz_str_drop(&column.name);
            column.name = jz_str_clone(&op->b);
            jz_str k = jz_str_clone(&op->b);
            jz_btree_insert(&columns, &k, &column, NULL);
            jz_str bk;
            if (jz_btree_remove(&branch_columns, &op->a, &bk, NULL)) {
                jz_str_drop(&bk);
                jz_str to = jz_str_clone(&op->b);
                jz_btree_insert(&branch_columns, &to, NULL, NULL);
            }
            break;
        }
        case JAZZ_LENSOP_COPY_COLUMN: {
            if (jz_btree_contains(&columns, &op->b)) { e = JZN_INVALID_CATALOGUE("column copy collides with existing column"); goto out; }
            const Col *src = jz_btree_get(&columns, &op->a);
            if (!src) { e = JZN_INVALID_CATALOGUE("column copy source is unknown"); goto out; }
            Col column; jazz_model__schema__ColumnSchema__clone(&column, src);
            jz_str_drop(&column.name);
            column.name = jz_str_clone(&op->b);
            jz_str k = jz_str_clone(&op->b);
            jz_btree_insert(&columns, &k, &column, NULL);
            break;
        }
        case JAZZ_LENSOP_ADD_COLUMN: {
            if (jz_btree_contains(&columns, &op->a)) { e = JZN_INVALID_CATALOGUE("added column already exists"); goto out; }
            const Col *tc = find_col(target_table, &op->a);
            if (!tc) { e = JZN_INVALID_CATALOGUE("added column is absent from target"); goto out; }
            Col column; jazz_model__schema__ColumnSchema__clone(&column, tc);
            jz_str k = jz_str_clone(&op->a);
            jz_btree_insert(&columns, &k, &column, NULL);
            const Col *inserted = jz_btree_get(&columns, &op->a);
            if (jz_btree_contains(&target_bindings, &op->a) && !inserted->has_default) {
                e = JZN_INVALID_CATALOGUE("added branch column requires a migration default"); goto out;
            }
            break;
        }
        case JAZZ_LENSOP_DROP_COLUMN: {
            if (jz_btree_contains(&branch_columns, &op->a)) { e = JZN_INVALID_CATALOGUE("table branch columns cannot be removed"); goto out; }
            jz_str ok; Col oc;
            if (!jz_btree_remove(&columns, &op->a, &ok, &oc)) { e = JZN_INVALID_CATALOGUE("dropped column is absent from source"); goto out; }
            jz_str_drop(&ok); jazz_model__schema__ColumnSchema__drop(&oc);
            break;
        }
        case JAZZ_LENSOP_TRANSFORM_COLUMN: {
            if (jz_btree_contains(&branch_columns, &op->a)) { e = JZN_INVALID_CATALOGUE("branch column type and migration default are immutable"); goto out; }
            e = jazz_node__node__migration_validation__validate_transform_column(jz_btree_get(&columns, &op->a), &op->b);
            if (JZ_IS_ERR(e)) goto out;
            const Col *source_column = jz_btree_get(&columns, &op->a);
            if (!source_column) { e = JZN_INVALID_CATALOGUE("transformed column is absent from source"); goto out; }
            const Col *target_column = find_col(target_table, &op->a);
            if (!target_column) { e = JZN_INVALID_CATALOGUE("transformed column is absent from target"); goto out; }
            if (!jazz_model__schema__physical_value_epoch_is_compatible(&source_column->column_type, &target_column->column_type) ||
                source_column->large_value_kind != target_column->large_value_kind) {
                e = JZN_INVALID_CATALOGUE("column transform changes physical value or large-value semantic kind"); goto out;
            }
            Col column; jazz_model__schema__ColumnSchema__clone(&column, target_column);
            jz_str k = jz_str_clone(&op->a);
            jz_btree_insert(&columns, &k, &column, NULL);
            break;
        }
        case JAZZ_LENSOP_REJECT_SOURCE_DELTA:
        default: break;
        }
    }
    if (!saw_table_rename) { e = JZN_INVALID_CATALOGUE("renamed table requires an explicit RenameTable operation"); goto out; }
    {
        jz_str *k; void *v;
        JZ_BTREE_FOR(&branch_columns, k, v)
            if (!jz_btree_contains(&target_bindings, k)) { e = JZN_INVALID_CATALOGUE("table branch columns cannot be removed"); goto out; }
    }
    jz_btree_drop(&target_columns);
    columns_from(&target_columns, target_table);
    {
        jz_str *branch_column; void *v;
        JZ_BTREE_FOR(&branch_columns, branch_column, v) {
            const Col *source_column = jz_btree_get(&columns, branch_column);
            if (!source_column) continue;
            const Col *target_column = jz_btree_get(&target_columns, branch_column);
            if (!target_column) continue;
            bool def_eq = source_column->has_default == target_column->has_default &&
                          (!source_column->has_default || groove__records__values__Value__eq(&source_column->deflt, &target_column->deflt));
            if (!groove__records__values__ValueType__eq(&source_column->column_type, &target_column->column_type) || !def_eq) {
                e = JZN_INVALID_CATALOGUE("branch column type and migration default are immutable"); goto out;
            }
        }
    }
    {
        jz_str *k; Col *c;
        JZ_BTREE_FOR(&columns, k, c) if (c->has_default) { groove__records__values__Value__drop(&c->deflt); c->has_default = false; }
        JZ_BTREE_FOR(&target_columns, k, c) if (c->has_default) { groove__records__values__Value__drop(&c->deflt); c->has_default = false; }
    }
    if (!jz_btree_eq(&columns, &target_columns, col_eq_v)) e = JZN_INVALID_CATALOGUE("lens operations do not reproduce target columns");
out:
    jz_btree_drop(&target_bindings);
    jz_btree_drop(&branch_columns);
    jz_btree_drop(&columns);
    jz_btree_drop(&target_columns);
    return e;
}

jz_err jazz_node__node__migration_validation__validate_migration_lens_between(const jazz_protocol_MigrationLens *lens,
                                                                             const jazz_protocol_SchemaVersion *source,
                                                                             const jazz_protocol_SchemaVersion *target) {
    for (u32 i = 0; i < lens->table_lenses.len; i++) JZ_TRY(validate_table_lens(&lens->table_lenses.ptr[i], source, target));
    return JZ_OK;
}

static bool set_disjoint(const jz_btree *a, const jz_btree *b) {
    void *k, *v;
    JZ_BTREE_FOR(a, k, v) if (jz_btree_contains(b, k)) return false;
    return true;
}
/* (a ∪ b) == c */
static bool union_eq(const jz_btree *a, const jz_btree *b, const jz_btree *c) {
    void *k, *v;
    JZ_BTREE_FOR(a, k, v) if (!jz_btree_contains(c, k)) return false;
    JZ_BTREE_FOR(b, k, v) if (!jz_btree_contains(c, k)) return false;
    JZ_BTREE_FOR(c, k, v) if (!jz_btree_contains(a, k) && !jz_btree_contains(b, k)) return false;
    return true;
}

jz_err jazz_node__node__migration_validation__validate_lineage_table_partition(const jazz_model_JazzSchema *source, const jazz_model_JazzSchema *target,
                                                                              const jazz_protocol_MigrationLens *lens, const jz_str *new_tables, u32 n_new,
                                                                              const jz_str *dropped_tables, u32 n_dropped) {
    jz_btree source_tables, target_tables, related_source, related_target, new_set, dropped;
    jz_btree_init(&source_tables, &jzn_desc_str_set);
    jz_btree_init(&target_tables, &jzn_desc_str_set);
    jz_btree_init(&related_source, &jzn_desc_str_set);
    jz_btree_init(&related_target, &jzn_desc_str_set);
    const jazz_model_TableSchemaVec *st = jazz_model__schema__JazzSchema__tables(source), *tt = jazz_model__schema__JazzSchema__tables(target);
    for (u32 i = 0; i < st->len; i++) { jz_str c = jz_str_clone(&st->ptr[i].name); jz_btree_insert(&source_tables, &c, NULL, NULL); }
    for (u32 i = 0; i < tt->len; i++) { jz_str c = jz_str_clone(&tt->ptr[i].name); jz_btree_insert(&target_tables, &c, NULL, NULL); }
    for (u32 i = 0; i < lens->table_lenses.len; i++) {
        jz_str a = jz_str_clone(&lens->table_lenses.ptr[i].source_table); jz_btree_insert(&related_source, &a, NULL, NULL);
        jz_str b = jz_str_clone(&lens->table_lenses.ptr[i].target_table); jz_btree_insert(&related_target, &b, NULL, NULL);
    }
    str_set_from(&new_set, new_tables, n_new);
    str_set_from(&dropped, dropped_tables, n_dropped);
    bool bad = related_source.len != lens->table_lenses.len || related_target.len != lens->table_lenses.len ||
               new_set.len != n_new || dropped.len != n_dropped || !set_disjoint(&related_source, &dropped) ||
               !set_disjoint(&related_target, &new_set) || !union_eq(&related_source, &dropped, &source_tables) ||
               !union_eq(&related_target, &new_set, &target_tables);
    jz_btree_drop(&source_tables); jz_btree_drop(&target_tables); jz_btree_drop(&related_source);
    jz_btree_drop(&related_target); jz_btree_drop(&new_set); jz_btree_drop(&dropped);
    if (bad) return JZN_INVALID_CATALOGUE("lineage table declarations do not partition schemas");
    return JZ_OK;
}
