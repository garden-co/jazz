/* binding_codec.c: port of crates/jazz/layers/db/src/binding_codec.rs.
 * (BINDING_CODEC_GOLDEN_FIXTURE is an include_str! test corpus; not shipped.) */
#include "db.h"
#include "binding_codec/publication_type.h"

/* postcard::Error::SerdeSerCustom */
enum { JAZZ_DB_PC_SERDE_SER_CUSTOM = 0x210 };

void jazz_types__object__ResultKey__from_occurrence(jazz_types_ResultKey *out, jazz_types_OutputOccurrenceId occ);
void jazz_types__object__ResultKey__Serialize__serialize(const jazz_types_ResultKey *k, jz_pc_w *w);
void jazz_types__object__ResultKey__drop(jazz_types_ResultKey *k);

typedef enum { ROW_FIELD_STORED_COLUMN = 0, ROW_FIELD_RESULT_FIELD = 1, ROW_FIELD_HIDDEN_METADATA = 2 } RowDescriptorFieldNameTag;
typedef struct jazz_db_RowDescriptorField {
    u32 name_tag;
    jz_uuid id;            /* StoredColumn */
    jz_slice name;         /* output_name / name (borrowed) */
    groove_ValueType value_type;
} jazz_db_RowDescriptorField;
typedef struct jazz_db_Row { jazz_types_RowUuid row_id; bool deleted; jz_slice raw; bool raw_owned; } jazz_db_Row;
typedef struct jazz_db_RowBatch {
    jz_slice table;
    JZ_VEC(jazz_db_RowDescriptorField) descriptor;
    JZ_VEC(jazz_db_Row) rows;
} jazz_db_RowBatch;
typedef JZ_VEC(jazz_db_RowBatch) jazz_db_RowBatchVec;

typedef struct jazz_db_PublishedRecord {
    const groove_RecordDescriptor *descriptor;
    jz_slice raw;
    const jazz_engine_CurrentRowPublicationField *fields;
    u32 fields_len;
} jazz_db_PublishedRecord;

static void row_descriptor_field_drop(void *p) { groove__records__values__ValueType__drop(&((jazz_db_RowDescriptorField *)p)->value_type); }
static void row_batch_drop(void *p) {
    jazz_db_RowBatch *b = p;
    jz_vec_drop(&b->descriptor, sizeof(jazz_db_RowDescriptorField), row_descriptor_field_drop);
    jz_vec_drop(&b->rows, sizeof(jazz_db_Row), NULL);
}
void jazz_db__binding_codec__RowBatchVec__drop(jazz_db_RowBatchVec *v) { jz_vec_drop(v, sizeof(jazz_db_RowBatch), row_batch_drop); }

static bool row_descriptor_field_eq(const jazz_db_RowDescriptorField *a, const jazz_db_RowDescriptorField *b) {
    if (a->name_tag != b->name_tag) return false;
    if (a->name_tag == ROW_FIELD_STORED_COLUMN && !jz_uuid_eq(&a->id, &b->id)) return false;
    if (jz_slice_cmp(a->name, b->name) != 0) return false;
    return groove__records__values__ValueType__eq(&a->value_type, &b->value_type);
}

/* ---- Serialize impls (postcard) ---- */
static void jazz_db__binding_codec__RowDescriptorField__Serialize__serialize(const jazz_db_RowDescriptorField *f, jz_pc_w *w) {
    jz_pc_variant(w, f->name_tag);
    switch (f->name_tag) {
    case ROW_FIELD_STORED_COLUMN:
        jz_pc_uuid(w, &f->id);
        jz_pc_bytes(w, f->name.ptr, f->name.len);
        break;
    case ROW_FIELD_RESULT_FIELD:
    case ROW_FIELD_HIDDEN_METADATA:
        jz_pc_bytes(w, f->name.ptr, f->name.len);
        break;
    }
    jazz_db__binding_codec__publication_type__serialize(&f->value_type, w);
}

static void jazz_db__binding_codec__RowBatch__Serialize__serialize(const jazz_db_RowBatch *b, jz_pc_w *w) {
    jz_pc_bytes(w, b->table.ptr, b->table.len);
    jz_pc_seq(w, b->descriptor.len);
    JZ_VEC_FOR(jazz_db_RowDescriptorField, f, b->descriptor) jazz_db__binding_codec__RowDescriptorField__Serialize__serialize(f, w);
    jz_pc_seq(w, b->rows.len);
    JZ_VEC_FOR(jazz_db_Row, r, b->rows) {
        jz_pc_uuid(w, &r->row_id);
        jz_pc_bool(w, r->deleted);
        jz_pc_bytes(w, r->raw.ptr, r->raw.len);
    }
}

static void serialize_batches(const jazz_db_RowBatchVec *batches, jz_pc_w *w) {
    jz_pc_seq(w, batches->len);
    JZ_VEC_FOR(jazz_db_RowBatch, b, *batches) jazz_db__binding_codec__RowBatch__Serialize__serialize(b, w);
}

/* ---- named cells ---- */
jz_err jazz_db__binding_codec__encode_named_cell_descriptor(const groove_RecordDescriptor *descriptor, jz_bytes *out) {
    memset(out, 0, sizeof *out);
    jz_pc_w w = { out };
    jazz_db__binding_codec__publication_type__NativeDescriptor__Serialize__serialize(descriptor, &w);
    return JZ_OK;
}

jz_err jazz_db__binding_codec__encode_named_cells(const groove_OwnedRecord *record, jz_bytes *out) {
    memset(out, 0, sizeof *out);
    jz_pc_w w = { out };
    jazz_db__binding_codec__publication_type__NativeDescriptor__Serialize__serialize(record->descriptor, &w);
    jz_pc_bytes(&w, record->raw.ptr, record->raw.len);
    return JZ_OK;
}

static void value_vec_drop(void *v) { jz_vec_drop(v, sizeof(groove_Value), groove__records__Value__drop); }

jz_err jazz_db__binding_codec__decode_named_cells(jz_slice bytes, jz_btree *cells_out) {
    jz_pc_r remaining = { bytes.ptr, bytes.ptr + bytes.len, 0 };
    const groove_RecordDescriptor *descriptor;
    JZ_TRY(jazz_db__binding_codec__publication_type__read_descriptor(&remaining, &descriptor));
    jz_bytes raw;
    JZ_TRY(jz_pc_r_bytes(&remaining, &raw));
    if (remaining.p != remaining.end) {
        jz_bytes_drop(&raw);
        return jz_err_new(JAZZ_DB_STRING_ERROR, "trailing bytes in named-cell envelope");
    }
    groove_OwnedRecord record;
    groove__records__OwnedRecord__new(&record, raw, descriptor);
    jz_bytes re;
    jz_err e = jazz_db__binding_codec__encode_named_cells(&record, &re);
    if (JZ_IS_ERR(e)) { groove__records__OwnedRecord__drop(&record); return e; }
    bool canonical = re.len == bytes.len && memcmp(re.ptr, bytes.ptr, bytes.len) == 0;
    jz_bytes_drop(&re);
    if (!canonical) { groove__records__OwnedRecord__drop(&record); return jz_err_new(JAZZ_DB_STRING_ERROR, "noncanonical named-cell envelope"); }
    JZ_VEC(groove_Value) values = {0};
    e = groove__records__OwnedRecord__to_values(&record, (void *)&values);
    if (JZ_IS_ERR(e)) { groove__records__OwnedRecord__drop(&record); return e; }
    jz_bytes created;
    e = groove__records__RecordDescriptor__create(descriptor, values.ptr, values.len, &created);
    if (JZ_IS_ERR(e)) { value_vec_drop(&values); groove__records__OwnedRecord__drop(&record); return e; }
    bool same = jz_bytes_eq(&created, &record.raw);
    jz_bytes_drop(&created);
    groove__records__OwnedRecord__drop(&record);
    if (!same) { value_vec_drop(&values); return jz_err_new(JAZZ_DB_STRING_ERROR, "noncanonical named-cell record"); }
    jz_btree_init(cells_out, &jazz_db_str_value_desc);
    u32 n;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(descriptor, &n);
    for (u32 i = 0; i < n && i < values.len; i++) {
        if (!fields[i].has_name) {
            for (u32 j = i; j < values.len; j++) groove__records__Value__drop(&values.ptr[j]);
            jz_free(values.ptr);
            jz_btree_drop(cells_out);
            return jz_err_new(JAZZ_DB_STRING_ERROR, "encoded cells must use named fields");
        }
        jz_str name = jz_str_clone(&fields[i].name);
        if (jz_btree_insert(cells_out, &name, &values.ptr[i], NULL)) {
            for (u32 j = i + 1; j < values.len; j++) groove__records__Value__drop(&values.ptr[j]);
            jz_free(values.ptr);
            jz_btree_drop(cells_out);
            return jz_err_new(JAZZ_DB_STRING_ERROR, "encoded cells contain duplicate names");
        }
    }
    jz_free(values.ptr);
    return JZ_OK;
}

/* ---- rows ---- */
jz_err jazz_db__binding_codec__published_record(const jazz_node_CurrentRow *row, jazz_db_PublishedRecord *out) {
    jazz_node__node__CurrentRow__encoded_record(row, &out->descriptor, &out->raw);
    out->fields = jazz_node__node__CurrentRow__publication_fields(row, &out->fields_len);
    return JZ_OK;
}

jz_err jazz_db__binding_codec__row_batches(const jazz_node_CurrentRow *rows, u32 n, jazz_db_RowBatchVec *out) {
    jazz_db_RowBatchVec batches = {0};
    for (u32 r = 0; r < n; r++) {
        const jazz_node_CurrentRow *row = &rows[r];
        jazz_db_PublishedRecord published;
        JZ_TRY(jazz_db__binding_codec__published_record(row, &published));
        u32 nf;
        const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(published.descriptor, &nf);
        JZ_VEC(jazz_db_RowDescriptorField) binding_descriptor = {0};
        u32 count = nf < published.fields_len ? nf : published.fields_len;
        for (u32 i = 0; i < count; i++) {
            const jazz_engine_CurrentRowPublicationField *binding = &published.fields[i];
            jazz_db_RowDescriptorField f;
            memset(&f, 0, sizeof f);
            switch (binding->tag) {
            case JAZZ_ENGINE_PUB_STORED_COLUMN:
                f.name_tag = ROW_FIELD_STORED_COLUMN;
                f.id = binding->id;
                f.name = jz_str_slice(&binding->name);
                break;
            case JAZZ_ENGINE_PUB_RESULT_FIELD:
                switch (binding->visibility) {
                case JAZZ_ENGINE_VIS_HIDDEN_METADATA: f.name_tag = ROW_FIELD_HIDDEN_METADATA; break;
                case JAZZ_ENGINE_VIS_APPLICATION_CELL:
                case JAZZ_ENGINE_VIS_PUBLIC_PROVENANCE: f.name_tag = ROW_FIELD_RESULT_FIELD; break;
                }
                f.name = jz_str_slice(&binding->name);
                break;
            case JAZZ_ENGINE_PUB_UNRESOLVED_SOURCE_CELL:
                jz_vec_drop(&binding_descriptor, sizeof(jazz_db_RowDescriptorField), row_descriptor_field_drop);
                jazz_db__binding_codec__RowBatchVec__drop(&batches);
                return jz_err_new(JAZZ_DB_PC_SERDE_SER_CUSTOM, "Serde Serialization Error");
            }
            groove__records__values__ValueType__clone(&f.value_type, &fields[i].value_type);
            jz_vec_push(&binding_descriptor, &f, sizeof f);
        }
        jz_slice table = jazz_node__node__CurrentRow__table(row);
        jazz_db_Row out_row = { jazz_node__node__CurrentRow__row_uuid(row), jazz_node__node__CurrentRow__is_deleted(row), published.raw, false };
        jazz_db_RowBatch *last = batches.len ? &batches.ptr[batches.len - 1] : NULL;
        bool same = false;
        if (last && jz_slice_cmp(last->table, table) == 0 && last->descriptor.len == binding_descriptor.len) {
            same = true;
            for (u32 i = 0; i < binding_descriptor.len; i++)
                if (!row_descriptor_field_eq(&last->descriptor.ptr[i], &binding_descriptor.ptr[i])) { same = false; break; }
        }
        if (same) {
            jz_vec_push(&last->rows, &out_row, sizeof out_row);
            jz_vec_drop(&binding_descriptor, sizeof(jazz_db_RowDescriptorField), row_descriptor_field_drop);
        } else {
            jazz_db_RowBatch b;
            b.table = table;
            b.descriptor.ptr = binding_descriptor.ptr; b.descriptor.len = binding_descriptor.len; b.descriptor.cap = binding_descriptor.cap;
            memset(&b.rows, 0, sizeof b.rows);
            jz_vec_push(&b.rows, &out_row, sizeof out_row);
            jz_vec_push(&batches, &b, sizeof b);
        }
    }
    *out = batches;
    return JZ_OK;
}

jz_err jazz_db__binding_codec__encode_rows(const jazz_node_CurrentRow *rows, u32 n, jz_bytes *out) {
    jazz_db_RowBatchVec batches;
    JZ_TRY(jazz_db__binding_codec__row_batches(rows, n, &batches));
    memset(out, 0, sizeof *out);
    jz_pc_w w = { out };
    serialize_batches(&batches, &w);
    jazz_db__binding_codec__RowBatchVec__drop(&batches);
    return JZ_OK;
}

jz_err jazz_db__binding_codec__encode_relation_snapshot(const jazz_node_RelationSnapshot *snapshot, jz_bytes *out) {
    jazz_db_RowBatchVec batches;
    JZ_TRY(jazz_db__binding_codec__row_batches(snapshot->rows.ptr, snapshot->rows.len, &batches));
    memset(out, 0, sizeof *out);
    jz_pc_w w = { out };
    jz_pc_varint(&w, (u64)snapshot->root_count);
    serialize_batches(&batches, &w);
    jazz_db__binding_codec__RowBatchVec__drop(&batches);
    return JZ_OK;
}

static void row_drop(void *p) { jazz_node__node__CurrentRow__drop(p); }

jz_err jazz_db__binding_codec__encode_subscription_delta(const jazz_db_SubscriptionOutputRow *added, u32 n_added,
                                                         const jazz_db_SubscriptionOutputRow *updated, u32 n_updated,
                                                         const jazz_db_RemovedRow *removed, u32 n_removed, jz_bytes *out) {
    JZ_VEC(jazz_node_CurrentRow) added_rows = {0}, updated_rows = {0};
    for (u32 i = 0; i < n_added; i++) jazz_node__node__CurrentRow__clone(jz_vec_push_slot(&added_rows, sizeof(jazz_node_CurrentRow)), &added[i].row);
    for (u32 i = 0; i < n_updated; i++) jazz_node__node__CurrentRow__clone(jz_vec_push_slot(&updated_rows, sizeof(jazz_node_CurrentRow)), &updated[i].row);
    jazz_db_RowBatchVec added_batches, updated_batches;
    jz_err e = jazz_db__binding_codec__row_batches(added_rows.ptr, added_rows.len, &added_batches);
    if (JZ_IS_ERR(e)) goto out_rows;
    e = jazz_db__binding_codec__row_batches(updated_rows.ptr, updated_rows.len, &updated_batches);
    if (JZ_IS_ERR(e)) { jazz_db__binding_codec__RowBatchVec__drop(&added_batches); goto out_rows; }
    memset(out, 0, sizeof *out);
    jz_pc_w w = { out };
    serialize_batches(&added_batches, &w);
    serialize_batches(&updated_batches, &w);
    /* removed: Vec<RemovedRowPayload { table, row_id }> */
    jz_pc_seq(&w, n_removed);
    for (u32 i = 0; i < n_removed; i++) {
        jz_str table = jz_str_clone(&removed[i].table);
        jz_pc_str(&w, &table);
        jz_pc_uuid(&w, &removed[i].row_uuid);
        jz_str_drop(&table);
    }
    /* occurrence keys */
    const struct { const void *base; u32 n; usize stride; usize off; } key_sets[3] = {
        { added, n_added, sizeof(jazz_db_SubscriptionOutputRow), offsetof(jazz_db_SubscriptionOutputRow, occurrence_id) },
        { updated, n_updated, sizeof(jazz_db_SubscriptionOutputRow), offsetof(jazz_db_SubscriptionOutputRow, occurrence_id) },
        { removed, n_removed, sizeof(jazz_db_RemovedRow), offsetof(jazz_db_RemovedRow, occurrence_id) },
    };
    for (u32 s = 0; s < 3; s++) {
        jz_pc_seq(&w, key_sets[s].n);
        for (u32 i = 0; i < key_sets[s].n; i++) {
            const jazz_types_OutputOccurrenceId *occ = (const void *)((const u8 *)key_sets[s].base + i * key_sets[s].stride + key_sets[s].off);
            jazz_types_OutputOccurrenceId cloned;
            jazz_types__object__OutputOccurrenceId__clone(&cloned, occ);
            jazz_types_ResultKey key;
            jazz_types__object__ResultKey__from_occurrence(&key, cloned);
            jazz_types__object__ResultKey__Serialize__serialize(&key, &w);
            jazz_types__object__ResultKey__drop(&key);
        }
    }
    jz_pc_seq(&w, n_added);
    for (u32 i = 0; i < n_added; i++) jz_pc_varint(&w, added[i].index);
    jz_pc_seq(&w, n_updated);
    for (u32 i = 0; i < n_updated; i++) jz_pc_varint(&w, updated[i].has_previous_index ? updated[i].previous_index : updated[i].index);
    jz_pc_seq(&w, n_updated);
    for (u32 i = 0; i < n_updated; i++) jz_pc_varint(&w, updated[i].index);
    jz_pc_seq(&w, n_removed);
    for (u32 i = 0; i < n_removed; i++) jz_pc_varint(&w, removed[i].index);
    jazz_db__binding_codec__RowBatchVec__drop(&added_batches);
    jazz_db__binding_codec__RowBatchVec__drop(&updated_batches);
out_rows:
    jz_vec_drop(&added_rows, sizeof(jazz_node_CurrentRow), row_drop);
    jz_vec_drop(&updated_rows, sizeof(jazz_node_CurrentRow), row_drop);
    return e;
}

jz_err jazz_db__binding_codec__terminal_operations_to_json(const groove_TerminalOperation *operations, u32 n, jz_json **out) {
    jz_err e = JZ_OK;
    jz_json *encoded = groove__ivm__runtime__terminal__TerminalOperation__Serialize__to_json_vec(operations, n, &e);
    if (JZ_IS_ERR(e)) return e;
    if (n == 0) { *out = encoded; return JZ_OK; }
    if (encoded->kind != JZ_JSON_ARR) jz_panic("terminal operations serialize as an array");
    for (u32 i = 0; i < encoded->v.arr.len; i++) {
        jz_json *wire = encoded->v.arr.ptr[i];
        if (wire->kind != JZ_JSON_OBJ) jz_panic("terminal operation serializes as an object");
        jz_str key = jz_str_from("root_descriptor");
        jz_btree_remove(&wire->v.obj, &key, NULL, NULL);
        jz_str_drop(&key);
    }
    *out = encoded;
    return JZ_OK;
}
