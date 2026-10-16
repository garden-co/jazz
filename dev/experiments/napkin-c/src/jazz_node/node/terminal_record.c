/* Port of crates/jazz/layers/node/src/node/terminal_record.rs */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

typedef struct jazz_node_TerminalRecordState {
    groove_RecordDescriptor *descriptor;
    groove_ValueVec values;
    jz_btree collections; /* usize -> TerminalCollectionState */
} jazz_node_TerminalRecordState;
typedef struct jazz_node_TerminalCollectionState {
    groove_RecordDescriptor *descriptor;
    JZ_VEC(jz_bytes) order;
    jz_btree rows; /* Vec<u8> -> TerminalRecordState */
} jazz_node_TerminalCollectionState;

static void value_drop_v(void *p) { groove__records__values__Value__drop(p); }
static void bytes_drop_v(void *p) { jz_bytes_drop(p); }
void jazz_node__node__terminal_record__TerminalRecordState__drop(void *p);
static void collection_drop_v(void *p) {
    jazz_node_TerminalCollectionState *c = p;
    groove__records__RecordDescriptor__drop_box(c->descriptor);
    jz_vec_drop(&c->order, sizeof(jz_bytes), bytes_drop_v);
    jz_btree_drop(&c->rows);
}
static int cmp_usize(const void *a, const void *b, void *c) { (void)c; u32 x = *(const u32 *)a, y = *(const u32 *)b; return x < y ? -1 : x > y; }
static const jz_btree_desc desc_collections = { sizeof(u32), sizeof(jazz_node_TerminalCollectionState), cmp_usize, NULL, collection_drop_v };
static const jz_btree_desc desc_rows = { sizeof(jz_bytes), sizeof(jazz_node_TerminalRecordState), jz_cmp_bytes, bytes_drop_v, jazz_node__node__terminal_record__TerminalRecordState__drop };
static const jz_btree_desc desc_occurrences = { sizeof(jz_bytes), sizeof(u64), jz_cmp_bytes, bytes_drop_v, NULL };

void jazz_node__node__terminal_record__TerminalRecordState__drop(void *p) {
    jazz_node_TerminalRecordState *s = p;
    groove__records__RecordDescriptor__drop_box(s->descriptor);
    jz_vec_drop(&s->values, sizeof(groove_Value), value_drop_v);
    jz_btree_drop(&s->collections);
}

static jz_err invalid(const char *message) { return jazz_node__node__api_error__Error__into_err(JZN_API_PROTOCOL, jz_str_from(message)); }
static jz_err invalid_with(const char *prefix, jz_err *source) {
    jz_str d = {0};
    groove__records__Error__Display__fmt(source, &d);
    jz_str m = jz_format("%s: %.*s", prefix, (int)d.len, d.ptr);
    jz_str_drop(&d);
    jz_err_drop(source);
    return jazz_node__node__api_error__Error__into_err(JZN_API_PROTOCOL, m);
}

jz_err jazz_node__node__terminal_record__TerminalRecordState__new(groove_OwnedRecord *record, jazz_node_TerminalRecordState *out) {
    groove_ValueVec values = {0};
    jz_err e = groove__records__BorrowedRecord__to_values(record, &values);
    if (JZ_IS_ERR(e)) { groove__records__OwnedRecord__drop(record); return invalid_with("invalid retained terminal record", &e); }
    out->descriptor = groove__records__RecordDescriptor__clone_box(groove__records__OwnedRecord__descriptor(record));
    out->values = values;
    jz_btree_init(&out->collections, &desc_collections);
    groove__records__OwnedRecord__drop(record);
    return JZ_OK;
}

jz_err jazz_node__node__terminal_record__terminal_child_key(const groove_Value *value, jz_bytes *out) {
    if (value->tag != GROOVE_V_RECORD)
        return jazz_node__node__api_error__Error__into_err(JZN_API_PROTOCOL, jz_str_from("terminal descendant collection contains a non-record child"));
    groove_Value first;
    jz_err e = groove__records__BorrowedRecord__get_idx(value->v.record, 0, &first);
    if (JZ_IS_ERR(e)) return invalid_with("cannot decode terminal child key", &e);
    if (first.tag != GROOVE_V_UUID) {
        groove__records__values__Value__drop(&first);
        return jazz_node__node__api_error__Error__into_err(JZN_API_PROTOCOL, jz_str_from("terminal descendant child key must be its physical row UUID"));
    }
    *out = (jz_bytes){0};
    jz_vec_reserve(out, 17, 1);
    jz_bytes_push_u8(out, 10);
    jz_bytes_push(out, first.v.uuid.b, 16);
    return JZ_OK;
}

static bool is_record_array(const groove_ValueType *t) {
    return t->tag == GROOVE_VT_ARRAY && t->v.inner->tag == GROOVE_VT_RECORD;
}

jz_err jazz_node__node__terminal_record__TerminalRecordState__update_record(jazz_node_TerminalRecordState *self, groove_OwnedRecord *record) {
    if (!groove__records__RecordDescriptor__eq(groove__records__OwnedRecord__descriptor(record), self->descriptor)) {
        groove__records__OwnedRecord__drop(record);
        return invalid("terminal update changed its retained descriptor");
    }
    jazz_node_TerminalRecordState replacement;
    JZ_TRY(jazz_node__node__terminal_record__TerminalRecordState__new(record, &replacement));
    const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(self->descriptor);
    for (u32 index = 0; index < fields->len; index++) {
        if (is_record_array(&fields->ptr[index].value_type)) {
            groove_Value tmp = replacement.values.ptr[index];
            replacement.values.ptr[index] = self->values.ptr[index];
            self->values.ptr[index] = tmp;
        }
    }
    jz_vec_drop(&self->values, sizeof(groove_Value), value_drop_v);
    self->values = replacement.values;
    replacement.values = (groove_ValueVec){0};
    jazz_node__node__terminal_record__TerminalRecordState__drop(&replacement);
    return JZ_OK;
}

static bool bytes_eq_slice(const jz_bytes *a, const jz_bytes *b) { return jz_bytes_eq(a, b); }

static jz_err jazz_node__node__terminal_record__TerminalCollectionState__apply(jazz_node_TerminalCollectionState *self, const groove_TerminalEdit *edit) {
    const jz_bytes *key = &edit->key;
    switch (edit->tag) {
    case GROOVE_TE_INSERT:
    case GROOVE_TE_UPDATE: {
        groove_OwnedRecord *record = groove__records__OwnedRecord__new(jz_bytes_clone(&edit->value), groove__records__RecordDescriptor__clone_box(self->descriptor));
        groove_Value rv = { GROOVE_V_RECORD }; rv.v.record = record;
        jz_bytes row_key;
        jz_err e = jazz_node__node__terminal_record__terminal_child_key(&rv, &row_key);
        if (JZ_IS_ERR(e)) { groove__records__OwnedRecord__drop(record); return e; }
        bool valid_key = jz_bytes_eq(key, &row_key);
        if (!valid_key && key->len == row_key.len + 9 && memcmp(key->ptr, row_key.ptr, row_key.len) == 0 && key->ptr[row_key.len] == 0xff) {
            for (u32 i = row_key.len + 1; i < key->len; i++) if (key->ptr[i] != 0) { valid_key = true; break; }
        }
        jz_bytes_drop(&row_key);
        if (!valid_key) { groove__records__OwnedRecord__drop(record); return invalid("terminal child payload key disagrees with edit key"); }
        if (edit->tag == GROOVE_TE_INSERT) {
            jazz_node_TerminalRecordState state;
            JZ_TRY(jazz_node__node__terminal_record__TerminalRecordState__new(record, &state));
            if (jz_btree_contains(&self->rows, key)) {
                u32 w = 0;
                for (u32 i = 0; i < self->order.len; i++) {
                    if (bytes_eq_slice(&self->order.ptr[i], key)) jz_bytes_drop(&self->order.ptr[i]);
                    else self->order.ptr[w++] = self->order.ptr[i];
                }
                self->order.len = w;
            }
            u32 at = edit->index < self->order.len ? (u32)edit->index : self->order.len;
            jz_bytes kc = jz_bytes_clone(key);
            jz_vec_insert(&self->order, at, &kc, sizeof(jz_bytes));
            jz_bytes kc2 = jz_bytes_clone(key);
            jz_btree_insert(&self->rows, &kc2, &state, NULL);
        } else {
            jazz_node_TerminalRecordState *row = jz_btree_get(&self->rows, key);
            if (!row) { groove__records__OwnedRecord__drop(record); return invalid("terminal child update addressed a missing key"); }
            JZ_TRY(jazz_node__node__terminal_record__TerminalRecordState__update_record(row, record));
        }
        break;
    }
    case GROOVE_TE_REMOVE: {
        jz_bytes ok; jazz_node_TerminalRecordState ov;
        if (!jz_btree_remove(&self->rows, key, &ok, &ov)) return invalid("terminal child removal addressed a missing key");
        jz_bytes_drop(&ok); jazz_node__node__terminal_record__TerminalRecordState__drop(&ov);
        u32 w = 0;
        for (u32 i = 0; i < self->order.len; i++) {
            if (bytes_eq_slice(&self->order.ptr[i], key)) jz_bytes_drop(&self->order.ptr[i]);
            else self->order.ptr[w++] = self->order.ptr[i];
        }
        self->order.len = w;
        break;
    }
    case GROOVE_TE_MOVE: {
        u32 previous = 0; bool found = false;
        for (u32 i = 0; i < self->order.len; i++) if (bytes_eq_slice(&self->order.ptr[i], key)) { previous = i; found = true; break; }
        if (!found) return invalid("terminal child move addressed a missing key");
        jz_bytes k;
        jz_vec_remove(&self->order, previous, &k, sizeof(jz_bytes));
        u32 at = edit->index < self->order.len ? (u32)edit->index : self->order.len;
        jz_vec_insert(&self->order, at, &k, sizeof(jz_bytes));
        break;
    }
    default: JZ_UNREACHABLE();
    }
    return JZ_OK;
}

static jz_err jazz_node__node__terminal_record__TerminalRecordState__apply_path(jazz_node_TerminalRecordState *self, const groove_TerminalPathSegment *path,
                                                                               u32 npath, const groove_TerminalEdit *edit) {
    if (npath == 0 || path[0].tag != GROOVE_TPS_COLLECTION) return invalid("terminal descendant path must begin with a collection");
    char *name = jz_alloc(path[0].name.len + 1);
    memcpy(name, path[0].name.ptr, path[0].name.len); name[path[0].name.len] = 0;
    u32 index;
    bool found = groove__records__RecordDescriptor__field_index(self->descriptor, name, &index);
    jz_free(name);
    if (!found) return invalid("terminal descendant path names an unknown collection");
    if (!jz_btree_contains(&self->collections, &index)) {
        const groove_ValueType *vt = &groove__records__RecordDescriptor__fields(self->descriptor)->ptr[index].value_type;
        if (vt->tag != GROOVE_VT_ARRAY) return invalid("terminal descendant path names a non-array field");
        if (vt->v.inner->tag != GROOVE_VT_RECORD) return invalid("terminal descendant collection does not contain records");
        if (index >= self->values.len || self->values.ptr[index].tag != GROOVE_V_ARRAY)
            return invalid("terminal descendant collection payload is not an array");
        groove_ValueVec *children = &self->values.ptr[index].v.vals;
        jazz_node_TerminalCollectionState collection;
        collection.descriptor = groove__records__RecordDescriptor__clone_box(vt->v.inner->v.record);
        collection.order = (typeof(collection.order)){0};
        jz_vec_reserve(&collection.order, children->len, sizeof(jz_bytes));
        jz_btree_init(&collection.rows, &desc_rows);
        jz_btree occurrences;
        jz_btree_init(&occurrences, &desc_occurrences);
        jz_err e = JZ_OK;
        for (u32 i = 0; i < children->len; i++) {
            const groove_Value *child = &children->ptr[i];
            if (child->tag != GROOVE_V_RECORD) { e = invalid("terminal descendant collection contains a non-record child"); break; }
            jz_bytes row_key;
            if (JZ_IS_ERR(e = jazz_node__node__terminal_record__terminal_child_key(child, &row_key))) break;
            jz_bytes rk2 = jz_bytes_clone(&row_key);
            bool inserted;
            u64 *occurrence = jz_btree_entry(&occurrences, &rk2, &inserted);
            if (inserted) *occurrence = 0; else jz_bytes_drop(&rk2);
            jz_bytes key;
            groove__ivm__terminal_occurrence_key(row_key, *occurrence, &key);
            *occurrence += 1;
            jazz_node_TerminalRecordState state;
            if (JZ_IS_ERR(e = jazz_node__node__terminal_record__TerminalRecordState__new(groove__records__OwnedRecord__clone(child->v.record), &state))) { jz_bytes_drop(&key); break; }
            jz_bytes kc = jz_bytes_clone(&key);
            if (jz_btree_insert(&collection.rows, &kc, &state, NULL)) {
                jz_bytes_drop(&key);
                e = invalid("terminal descendant collection has duplicate child keys");
                break;
            }
            JZ_VEC_PUSH(collection.order, key);
        }
        jz_btree_drop(&occurrences);
        if (JZ_IS_ERR(e)) { collection_drop_v(&collection); return e; }
        jz_vec_truncate(children, 0, sizeof(groove_Value), value_drop_v);
        jz_btree_insert(&self->collections, &index, &collection, NULL);
    }
    jazz_node_TerminalCollectionState *collection = jz_btree_get(&self->collections, &index);
    if (!collection) jz_panic("initialized above");
    const groove_TerminalPathSegment *rest = path + 1;
    u32 nrest = npath - 1;
    if (nrest == 0) return jazz_node__node__terminal_record__TerminalCollectionState__apply(collection, edit);
    if (nrest == 1 && rest[0].tag == GROOVE_TPS_KEY) {
        if (edit->tag == GROOVE_TE_UPDATE && jz_bytes_eq(&edit->key, &rest[0].key))
            return jazz_node__node__terminal_record__TerminalCollectionState__apply(collection, edit);
        return invalid("terminal keyed path must match its scalar update key");
    }
    if (rest[0].tag == GROOVE_TPS_KEY && nrest > 1) {
        jazz_node_TerminalRecordState *child = jz_btree_get(&collection->rows, &rest[0].key);
        if (!child) return invalid("terminal child path addressed a missing key");
        return jazz_node__node__terminal_record__TerminalRecordState__apply_path(child, rest + 1, nrest - 1, edit);
    }
    return invalid("terminal descendant path does not alternate collection and key");
}

jz_err jazz_node__node__terminal_record__TerminalRecordState__apply(jazz_node_TerminalRecordState *self, const groove_TerminalOperation *operation) {
    if (!groove__records__RecordDescriptor__eq(self->descriptor, operation->root_descriptor))
        return invalid("terminal descendant descriptor disagrees with its retained root");
    return jazz_node__node__terminal_record__TerminalRecordState__apply_path(self, operation->path.ptr, operation->path.len, &operation->edit);
}

jz_err jazz_node__node__terminal_record__TerminalRecordState__record(const jazz_node_TerminalRecordState *self, groove_OwnedRecord **out) {
    groove_ValueVec values = {0};
    jz_vec_clone(&values, &self->values, sizeof(groove_Value), groove__records__values__Value__clone);
    u32 *index; jazz_node_TerminalCollectionState *collection;
    JZ_BTREE_FOR(&self->collections, index, collection) {
        groove_Value arr = { GROOVE_V_ARRAY };
        arr.v.vals = (groove_ValueVec){0};
        for (u32 i = 0; i < collection->order.len; i++) {
            const jazz_node_TerminalRecordState *row = jz_btree_get(&collection->rows, &collection->order.ptr[i]);
            if (!row) jz_panic("ordered child exists");
            groove_OwnedRecord *rec;
            jz_err e = jazz_node__node__terminal_record__TerminalRecordState__record(row, &rec);
            if (JZ_IS_ERR(e)) {
                groove__records__values__Value__drop(&arr);
                jz_vec_drop(&values, sizeof(groove_Value), value_drop_v);
                return e;
            }
            groove_Value rv = { GROOVE_V_RECORD }; rv.v.record = rec;
            JZ_VEC_PUSH(arr.v.vals, rv);
        }
        groove__records__values__Value__drop(&values.ptr[*index]);
        values.ptr[*index] = arr;
    }
    jz_bytes raw;
    jz_err e = groove__records__RecordDescriptor__create(self->descriptor, values.ptr, values.len, &raw);
    jz_vec_drop(&values, sizeof(groove_Value), value_drop_v);
    if (JZ_IS_ERR(e)) return invalid_with("cannot encode retained terminal record", &e);
    *out = groove__records__OwnedRecord__new(raw, groove__records__RecordDescriptor__clone_box(self->descriptor));
    return JZ_OK;
}

usize jazz_node__node__terminal_record__TerminalRecordState__retained_bytes(const jazz_node_TerminalRecordState *self) {
    usize total = jazz_postcard_serialized_size_values(&self->values) + self->values.cap * sizeof(groove_Value);
    u32 *index; jazz_node_TerminalCollectionState *collection;
    JZ_BTREE_FOR(&self->collections, index, collection) {
        usize c = collection->order.cap * sizeof(jz_bytes);
        for (u32 i = 0; i < collection->order.len; i++) c += collection->order.ptr[i].cap;
        jz_bytes *key; jazz_node_TerminalRecordState *row;
        JZ_BTREE_FOR(&collection->rows, key, row) c += key->cap + jazz_node__node__terminal_record__TerminalRecordState__retained_bytes(row);
        total += c;
    }
    return total;
}

static void collection_clone_v(void *dst, const void *src);
void jazz_node__node__terminal_record__TerminalRecordState__clone(void *dst, const void *src) {
    jazz_node_TerminalRecordState *d = dst; const jazz_node_TerminalRecordState *s = src;
    d->descriptor = groove__records__RecordDescriptor__clone_box(s->descriptor);
    d->values = (groove_ValueVec){0};
    jz_vec_clone(&d->values, &s->values, sizeof(groove_Value), groove__records__values__Value__clone);
    jz_btree_clone(&d->collections, &s->collections, NULL, collection_clone_v);
}
static void bytes_clone_v(void *d, const void *s) { *(jz_bytes *)d = jz_bytes_clone(s); }
static void collection_clone_v(void *dst, const void *src) {
    jazz_node_TerminalCollectionState *d = dst; const jazz_node_TerminalCollectionState *s = src;
    d->descriptor = groove__records__RecordDescriptor__clone_box(s->descriptor);
    d->order = (typeof(d->order)){0};
    jz_vec_clone(&d->order, &s->order, sizeof(jz_bytes), bytes_clone_v);
    jz_btree_clone(&d->rows, &s->rows, bytes_clone_v, jazz_node__node__terminal_record__TerminalRecordState__clone);
}
