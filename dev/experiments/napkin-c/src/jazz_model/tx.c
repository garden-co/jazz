/* tx.rs: transaction, fate, durability, snapshot and read-set vocabulary.
 * HistoryEntry accessors, RejectedTransaction accessors and
 * ContributionSubstitutionIndex are absent from the shipped WASM (only their
 * drop glue ships) and are not ported. */
#include "jazz_model/jazz_model.h"

#define TX(x) jazz_model__tx__##x
#define BR(x) jazz_model__model__branch__##x
#define GV(x) groove__records__values__Value__##x

/* define_record! field indices */
#define REJECTED_VERSION_FIELD_ROW_UUID_IDX 2
#define REJECTED_VERSION_FIELD__DELETION_IDX 3

static jz_err str_err(const char *m) { return jz_err_new(JAZZ_MODEL_ERR_STRING, m); }

/* ---------------------------------------------------------------- TxId */
void TX(TxId__Serialize__serialize)(jz_pc_w *w, const jazz_model_TxId *id) {
    jazz_types__time__TxTime__Serialize__serialize(w, id->time);
    jz_pc_uuid(w, &id->node);
}

jz_err TX(TxId__Deserialize__deserialize)(jz_pc_r *r, jazz_model_TxId *out) {
    JZ_TRY(jazz_types__time__TxTime__Deserialize__deserialize(r, &out->time));
    return jz_pc_r_uuid(r, &out->node);
}

int TX(TxId__Ord__cmp)(const void *pa, const void *pb, void *ctx) {
    (void)ctx;
    const jazz_model_TxId *a = pa, *b = pb;
    if (a->time != b->time) return a->time < b->time ? -1 : 1;
    return jz_uuid_cmp(&a->node, &b->node);
}

bool TX(TxId__PartialEq__eq)(const jazz_model_TxId *a, const jazz_model_TxId *b) {
    return a->time == b->time && jz_uuid_eq(&a->node, &b->node);
}
bool TX(TxId__eq_v)(const void *a, const void *b) { return TX(TxId__PartialEq__eq)(a, b); }
u64 TX(TxId__Hash__hash)(const void *p) {
    const jazz_model_TxId *id = p;
    return jz_hash_combine(jz_hash_u64(id->time), jz_uuid_hash(&id->node));
}

void TX(TxId__Debug__fmt)(jz_str *out, const jazz_model_TxId *id) {
    char node[37];
    jz_uuid_to_string(&id->node, node);
    jz_str_push_cstr(out, "TxId { time: ");
    jazz_types__time__TxTime__Debug__fmt(out, id->time);
    jz_str_appendf(out, ", node: NodeUuid(%s) }", node);
}

const jz_btree_desc jazz_model__tx__TXID_SET_DESC = { sizeof(jazz_model_TxId), 0, jazz_model__tx__TxId__Ord__cmp, NULL, NULL };

static void txid_vec_serialize(jz_pc_w *w, const jazz_model_TxIdVec *v) {
    jz_pc_seq(w, v->len);
    for (u32 i = 0; i < v->len; i++) TX(TxId__Serialize__serialize)(w, &v->ptr[i]);
}

/* ---------------------------------------------------------------- small enums */
void TX(TxKind__Serialize__serialize)(jz_pc_w *w, u8 kind) { jz_pc_variant(w, kind); }
jz_err TX(TxKind__Deserialize__deserialize)(jz_pc_r *r, u8 *out) {
    u32 idx;
    JZ_TRY(jz_pc_r_variant(r, &idx, 2));
    *out = (u8)idx;
    return JZ_OK;
}

static const char *const DURABILITY_NAMES[] = {"None", "Local", "Global"};
void TX(DurabilityTier__Debug__fmt)(jz_str *out, u8 tier) { jz_str_push_cstr(out, DURABILITY_NAMES[tier]); }
void TX(DurabilityTier__Serialize__serialize)(jz_pc_w *w, u8 tier) { jz_pc_variant(w, tier); }
jz_err TX(DurabilityTier__Deserialize__deserialize)(jz_pc_r *r, u8 *out) {
    u32 idx;
    JZ_TRY(jz_pc_r_variant(r, &idx, 3));
    *out = (u8)idx;
    return JZ_OK;
}

/* ---------------------------------------------------------------- RejectionReason / Fate */
void TX(RejectionReason__drop)(jazz_model_RejectionReason *r) {
    if (r->tag == JAZZ_REJECT_MALFORMED_COMMIT) jz_str_drop(&r->message);
}
void TX(RejectionReason__Clone__clone)(jazz_model_RejectionReason *dst, const jazz_model_RejectionReason *src) {
    *dst = *src;
    if (src->tag == JAZZ_REJECT_MALFORMED_COMMIT) dst->message = jz_str_clone(&src->message);
}
bool TX(RejectionReason__PartialEq__eq)(const jazz_model_RejectionReason *a, const jazz_model_RejectionReason *b) {
    if (a->tag != b->tag) return false;
    if (a->tag == JAZZ_REJECT_CASCADE) return TX(TxId__PartialEq__eq)(&a->root, &b->root);
    if (a->tag == JAZZ_REJECT_MALFORMED_COMMIT) return jz_str_eq(&a->message, &b->message);
    return true;
}

void TX(RejectionReason__Debug__fmt)(jz_str *out, const jazz_model_RejectionReason *r) {
    switch (r->tag) {
    case JAZZ_REJECT_CLIENT_CLOCK_TOO_FAR_AHEAD: jz_str_push_cstr(out, "ClientClockTooFarAhead"); break;
    case JAZZ_REJECT_AUTHORIZATION_DENIED: jz_str_push_cstr(out, "AuthorizationDenied"); break;
    case JAZZ_REJECT_EXCLUSIVE_CONFLICT: jz_str_push_cstr(out, "ExclusiveConflict"); break;
    case JAZZ_REJECT_CAUSALITY_VIOLATION: jz_str_push_cstr(out, "CausalityViolation"); break;
    case JAZZ_REJECT_CASCADE:
        jz_str_push_cstr(out, "Cascade { root: ");
        TX(TxId__Debug__fmt)(out, &r->root);
        jz_str_push_cstr(out, " }");
        break;
    case JAZZ_REJECT_MALFORMED_COMMIT:
        jz_str_push_cstr(out, "MalformedCommit(");
        jazz_model__debug_str(out, r->message.ptr, r->message.len);
        jz_str_push_char(out, ')');
        break;
    }
}

static void rejection_reason_serialize(jz_pc_w *w, const jazz_model_RejectionReason *r) {
    jz_pc_variant(w, r->tag);
    if (r->tag == JAZZ_REJECT_CASCADE) TX(TxId__Serialize__serialize)(w, &r->root);
    else if (r->tag == JAZZ_REJECT_MALFORMED_COMMIT) jz_pc_str(w, &r->message);
}

static jz_err rejection_reason_deserialize(jz_pc_r *r, jazz_model_RejectionReason *out) {
    u32 idx;
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_pc_r_variant(r, &idx, 6));
    out->tag = (u8)idx;
    if (idx == JAZZ_REJECT_CASCADE) return TX(TxId__Deserialize__deserialize)(r, &out->root);
    if (idx == JAZZ_REJECT_MALFORMED_COMMIT) return jz_pc_r_str(r, &out->message);
    return JZ_OK;
}

groove_ValueType TX(RejectionReason__storage_value_type)(void) {
    static const char *const unit_cases[4] = {"client_clock_too_far_ahead", "authorization_denied", "exclusive_conflict", "causality_violation"};
    groove_EnumCaseVec cases = {0};
    for (u32 i = 0; i < 4; i++)
        JZ_VEC_PUSH(cases, groove__records__values__EnumCase__new(jz_str_from(unit_cases[i]), groove__records__RecordDescriptor__Default__default()));
    jz_str root = jz_str_from("root");
    groove_ValueType tuple;
    memset(&tuple, 0, sizeof tuple);
    tuple.tag = GROOVE_VT_TUPLE;
    JZ_VEC_PUSH(tuple.u.tuple, groove__records__values__ValueType__scalar(GROOVE_VT_U64));
    JZ_VEC_PUSH(tuple.u.tuple, groove__records__values__ValueType__scalar(GROOVE_VT_UUID));
    JZ_VEC_PUSH(cases, groove__records__values__EnumCase__new(jz_str_from("cascade"), groove__records__RecordDescriptor__new(&root, &tuple, 1)));
    jz_str message = jz_str_from("message");
    groove_ValueType str = groove__records__values__ValueType__scalar(GROOVE_VT_STRING);
    JZ_VEC_PUSH(cases, groove__records__values__EnumCase__new(jz_str_from("malformed_commit"), groove__records__RecordDescriptor__new(&message, &str, 1)));
    groove_ValueType t;
    memset(&t, 0, sizeof t);
    t.tag = GROOVE_VT_ENUM;
    t.u.enum_ = JZ_NEW(groove_EnumSchema);
    if (groove__records__values__EnumSchema__new(jz_str_from("jazz_rejection_reason"), cases, t.u.enum_).code)
        jz_panic("fixed rejection reason schema");
    return t;
}

jz_err TX(RejectionReason__to_storage_value)(const jazz_model_RejectionReason *r, groove_Value *out) {
    u32 tag = r->tag;
    groove_Value field;
    u32 nfields = 0;
    memset(&field, 0, sizeof field);
    if (r->tag == JAZZ_REJECT_CASCADE) {
        field.tag = GROOVE_V_TUPLE;
        groove_Value t, n;
        memset(&t, 0, sizeof t);
        memset(&n, 0, sizeof n);
        t.tag = GROOVE_V_U64;
        t.u.u64_ = r->root.time;
        n.tag = GROOVE_V_UUID;
        n.u.uuid = r->root.node;
        JZ_VEC_PUSH(field.u.items, t);
        JZ_VEC_PUSH(field.u.items, n);
        nfields = 1;
    } else if (r->tag == JAZZ_REJECT_MALFORMED_COMMIT) {
        field.tag = GROOVE_V_STRING;
        field.u.s = jz_str_clone(&r->message);
        nfields = 1;
    }
    groove_ValueType t = TX(RejectionReason__storage_value_type)();
    if (t.tag != GROOVE_VT_ENUM) JZ_UNREACHABLE();
    const groove_EnumCase *c;
    jz_err e = groove__records__values__EnumSchema__case(t.u.enum_, tag, &c);
    if (!e.code) {
        memset(out, 0, sizeof *out);
        out->tag = GROOVE_V_ENUM;
        e = groove__records__values__EnumValue__create(tag, c->payload, nfields ? &field : NULL, nfields, &out->u.enum_);
    }
    if (nfields) GV(drop)(&field);
    groove__records__values__ValueType__drop(&t);
    return e;
}

static jz_err jazz_model__tx__tx_id_from_value(groove_Value value, jazz_model_TxId *out) {
    if (value.tag != GROOVE_V_TUPLE || value.u.items.len != 2) { GV(drop)(&value); return str_err("tx id tuple"); }
    groove_Value *time = &value.u.items.ptr[0], *node = &value.u.items.ptr[1];
    jz_err e = JZ_OK;
    if (time->tag != GROOVE_V_U64) e = str_err("tx id time");
    else if (node->tag != GROOVE_V_UUID) e = str_err("tx id node");
    else { out->time = time->u.u64_; out->node = node->u.uuid; }
    GV(drop)(&value);
    return e;
}

static jz_err jazz_model__tx__deletion_from_value(groove_Value value, bool *some, u8 *out) {
    *some = false;
    switch (value.tag) {
    case GROOVE_V_NULLABLE:
        if (!value.u.nullable) return JZ_OK;
        {
            groove_Value inner = *value.u.nullable;
            jz_free(value.u.nullable);
            return jazz_model__tx__deletion_from_value(inner, some, out);
        }
    case GROOVE_V_ENUM_TAG:
        if (value.u.enum_tag <= 1) { *some = true; *out = value.u.enum_tag; return JZ_OK; }
        break;
    case GROOVE_V_U8:
        if (value.u.u8_ <= 1) { *some = true; *out = value.u.u8_; return JZ_OK; }
        break;
    default: break;
    }
    GV(drop)(&value);
    return str_err("deletion");
}

jz_err TX(RejectionReason__from_storage_value)(groove_Value value, jazz_model_RejectionReason *out) {
    memset(out, 0, sizeof *out);
    if (value.tag != GROOVE_V_ENUM) { GV(drop)(&value); return groove__records__Error__simple(GROOVE_REC_E_NonCanonicalRecord); }
    groove_ValueType t = TX(RejectionReason__storage_value_type)();
    if (t.tag != GROOVE_VT_ENUM) JZ_UNREACHABLE();
    jz_err e;
    const groove_EnumCase *c;
    groove_ValueVec fields = {0};
    u32 tag = value.u.enum_.tag;
    if ((e = groove__records__values__EnumSchema__case(t.u.enum_, tag, &c)).code) goto done;
    groove_BorrowedRecord bound = { jz_bytes_slice(&value.u.enum_.record.raw), c->payload };
    if ((e = groove__records__BorrowedRecord__to_values(bound, &fields)).code) goto done;
    jz_bytes again = {0};
    if ((e = groove__records__RecordDescriptor__create(c->payload, fields.ptr, fields.len, &again)).code) goto done;
    bool same = jz_bytes_eq(&again, &value.u.enum_.record.raw);
    jz_bytes_drop(&again);
    if (!same) { e = groove__records__Error__simple(GROOVE_REC_E_NonCanonicalRecord); goto done; }
    e = groove__records__Error__simple(GROOVE_REC_E_NonCanonicalRecord);
    if (tag <= 3 && fields.len == 0) { out->tag = (u8)tag; e = JZ_OK; }
    else if (tag == 4 && fields.len == 1) {
        groove_Value root;
        GV(Clone__clone)(&root, &fields.ptr[0]);
        jz_err te = jazz_model__tx__tx_id_from_value(root, &out->root);
        if (!te.code) { out->tag = JAZZ_REJECT_CASCADE; e = JZ_OK; }
        else jz_err_drop(&te);
    } else if (tag == 5 && fields.len == 1 && fields.ptr[0].tag == GROOVE_V_STRING) {
        out->tag = JAZZ_REJECT_MALFORMED_COMMIT;
        out->message = jz_str_clone(&fields.ptr[0].u.s);
        e = JZ_OK;
    }
done:
    groove__records__values__ValueVec__drop(&fields);
    groove__records__values__ValueType__drop(&t);
    GV(drop)(&value);
    return e;
}

void TX(Fate__drop)(jazz_model_Fate *f) { if (f->tag == JAZZ_FATE_REJECTED) TX(RejectionReason__drop)(&f->reason); }
void TX(Fate__Clone__clone)(jazz_model_Fate *dst, const jazz_model_Fate *src) {
    *dst = *src;
    if (src->tag == JAZZ_FATE_REJECTED) TX(RejectionReason__Clone__clone)(&dst->reason, &src->reason);
}
bool TX(Fate__PartialEq__eq)(const jazz_model_Fate *a, const jazz_model_Fate *b) {
    return a->tag == b->tag && (a->tag != JAZZ_FATE_REJECTED || TX(RejectionReason__PartialEq__eq)(&a->reason, &b->reason));
}
void TX(Fate__Serialize__serialize)(jz_pc_w *w, const jazz_model_Fate *f) {
    jz_pc_variant(w, f->tag);
    if (f->tag == JAZZ_FATE_REJECTED) rejection_reason_serialize(w, &f->reason);
}
jz_err TX(Fate__Deserialize__deserialize)(jz_pc_r *r, jazz_model_Fate *out) {
    u32 idx;
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_pc_r_variant(r, &idx, 3));
    out->tag = (u8)idx;
    if (idx == JAZZ_FATE_REJECTED) return rejection_reason_deserialize(r, &out->reason);
    return JZ_OK;
}

/* ---------------------------------------------------------------- contribution coordinates */
static void component_drop(jazz_model_ContributionComponent *c) {
    if (c->tag == JAZZ_COMPONENT_COLUMN) jz_str_drop(&c->column);
    else if (c->tag == JAZZ_COMPONENT_OPERATION) { jz_str_drop(&c->column); jz_bytes_drop(&c->identity); }
}
static void component_clone(jazz_model_ContributionComponent *dst, const jazz_model_ContributionComponent *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    if (src->tag != JAZZ_COMPONENT_REGISTER) dst->column = jz_str_clone(&src->column);
    if (src->tag == JAZZ_COMPONENT_OPERATION) dst->identity = jz_bytes_clone(&src->identity);
}
static int component_cmp(const jazz_model_ContributionComponent *a, const jazz_model_ContributionComponent *b) {
    if (a->tag != b->tag) return a->tag < b->tag ? -1 : 1;
    if (a->tag == JAZZ_COMPONENT_REGISTER) return 0;
    int c = jz_str_cmp(&a->column, &b->column);
    if (c || a->tag == JAZZ_COMPONENT_COLUMN) return c;
    return jz_bytes_cmp(&a->identity, &b->identity);
}

static void coordinate_drop(jazz_model_ContributionCoordinate *c) {
    BR(BranchKey__drop)(&c->branch_key);
    jz_str_drop(&c->table);
    component_drop(&c->component);
}
static void coordinate_clone(jazz_model_ContributionCoordinate *dst, const jazz_model_ContributionCoordinate *src) {
    BR(BranchKey__Clone__clone)(&dst->branch_key, &src->branch_key);
    dst->table = jz_str_clone(&src->table);
    dst->row_uuid = src->row_uuid;
    dst->layer = src->layer;
    component_clone(&dst->component, &src->component);
}

int TX(ContributionCoordinate__Ord__cmp)(const jazz_model_ContributionCoordinate *a, const jazz_model_ContributionCoordinate *b) {
    int c = BR(BranchKey__Ord__cmp)(&a->branch_key, &b->branch_key, NULL);
    if (c) return c;
    if ((c = jz_str_cmp(&a->table, &b->table))) return c;
    if ((c = jz_uuid_cmp(&a->row_uuid, &b->row_uuid))) return c;
    if (a->layer != b->layer) return a->layer < b->layer ? -1 : 1;
    return component_cmp(&a->component, &b->component);
}
bool TX(ContributionCoordinate__PartialEq__eq)(const jazz_model_ContributionCoordinate *a, const jazz_model_ContributionCoordinate *b) {
    return TX(ContributionCoordinate__Ord__cmp)(a, b) == 0;
}

static void component_serialize(jz_pc_w *w, const jazz_model_ContributionComponent *c) {
    jz_pc_variant(w, c->tag);
    if (c->tag != JAZZ_COMPONENT_REGISTER) jz_pc_str(w, &c->column);
    if (c->tag == JAZZ_COMPONENT_OPERATION) {
        jz_pc_seq(w, c->identity.len);
        jz_pc_raw(w, c->identity.ptr, c->identity.len);
    }
}
static jz_err component_deserialize(jz_pc_r *r, jazz_model_ContributionComponent *out) {
    u32 idx, n;
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_pc_r_variant(r, &idx, 3));
    out->tag = (u8)idx;
    if (idx == JAZZ_COMPONENT_REGISTER) return JZ_OK;
    JZ_TRY(jz_pc_r_str(r, &out->column));
    if (idx == JAZZ_COMPONENT_OPERATION) {
        JZ_TRY(jz_pc_r_seq(r, &n));
        for (u32 i = 0; i < n; i++) {
            u8 b;
            JZ_TRY(jz_pc_r_u8(r, &b));
            jz_bytes_push_u8(&out->identity, b);
        }
    }
    return JZ_OK;
}

static void coordinate_serialize(jz_pc_w *w, const jazz_model_ContributionCoordinate *c) {
    BR(BranchKey__Serialize__serialize)(w, &c->branch_key);
    jz_pc_str(w, &c->table);
    jz_pc_uuid(w, &c->row_uuid);
    jz_pc_variant(w, c->layer);
    component_serialize(w, &c->component);
}
static jz_err coordinate_deserialize(jz_pc_r *r, jazz_model_ContributionCoordinate *out) {
    u32 layer;
    memset(out, 0, sizeof *out);
    JZ_TRY(BR(BranchKey__Deserialize__deserialize)(r, &out->branch_key));
    JZ_TRY(jz_pc_r_str(r, &out->table));
    JZ_TRY(jz_pc_r_uuid(r, &out->row_uuid));
    JZ_TRY(jz_pc_r_variant(r, &layer, 2));
    out->layer = (u8)layer;
    return component_deserialize(r, &out->component);
}

static void dot_drop_v(void *d) { coordinate_drop(&((jazz_model_ContributionDot *)d)->coordinate); }
static void dot_clone_v(void *dst, const void *src) {
    jazz_model_ContributionDot *d = dst;
    const jazz_model_ContributionDot *s = src;
    d->tx_id = s->tx_id;
    coordinate_clone(&d->coordinate, &s->coordinate);
}
static int dot_cmp(const void *pa, const void *pb, void *ctx) {
    (void)ctx;
    const jazz_model_ContributionDot *a = pa, *b = pb;
    int c = TX(TxId__Ord__cmp)(&a->tx_id, &b->tx_id, NULL);
    return c ? c : TX(ContributionCoordinate__Ord__cmp)(&a->coordinate, &b->coordinate);
}
static int dot_cmp2(const void *a, const void *b) { return dot_cmp(a, b, NULL); }

static void substitution_drop_v(void *s) {
    jazz_model_ContributionSubstitution *x = s;
    coordinate_drop(&x->target);
    jz_vec_drop(&x->sources, sizeof(jazz_model_ContributionDot), dot_drop_v);
}
static void substitution_clone_v(void *dst, const void *src) {
    jazz_model_ContributionSubstitution *d = dst;
    const jazz_model_ContributionSubstitution *s = src;
    coordinate_clone(&d->target, &s->target);
    jz_vec_clone(&d->sources, &s->sources, sizeof(jazz_model_ContributionDot), dot_clone_v);
}
static int substitution_target_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    return TX(ContributionCoordinate__Ord__cmp)(&((const jazz_model_ContributionSubstitution *)a)->target,
                                                &((const jazz_model_ContributionSubstitution *)b)->target);
}
static bool substitution_vec_eq(const jazz_model_ContributionSubstitutionVec *a, const jazz_model_ContributionSubstitutionVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) {
        if (!TX(ContributionCoordinate__PartialEq__eq)(&a->ptr[i].target, &b->ptr[i].target)) return false;
        if (a->ptr[i].sources.len != b->ptr[i].sources.len) return false;
        for (u32 j = 0; j < a->ptr[i].sources.len; j++)
            if (dot_cmp(&a->ptr[i].sources.ptr[j], &b->ptr[i].sources.ptr[j], NULL)) return false;
    }
    return true;
}

static void substitution_serialize(jz_pc_w *w, const jazz_model_ContributionSubstitution *s) {
    coordinate_serialize(w, &s->target);
    jz_pc_seq(w, s->sources.len);
    for (u32 i = 0; i < s->sources.len; i++) {
        TX(TxId__Serialize__serialize)(w, &s->sources.ptr[i].tx_id);
        coordinate_serialize(w, &s->sources.ptr[i].coordinate);
    }
}
static jz_err substitution_deserialize(jz_pc_r *r, jazz_model_ContributionSubstitution *out) {
    u32 n;
    memset(out, 0, sizeof *out);
    JZ_TRY(coordinate_deserialize(r, &out->target));
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_model_ContributionDot d;
        JZ_TRY(TX(TxId__Deserialize__deserialize)(r, &d.tx_id));
        JZ_TRY(coordinate_deserialize(r, &d.coordinate));
        JZ_VEC_PUSH(out->sources, d);
    }
    return JZ_OK;
}

/* ---------------------------------------------------------------- BranchViewCopyEvidence */
static void copy_base_drop(jazz_model_BranchViewCopyBase *b) {
    BR(BranchKey__drop)(&b->branch);
    if (b->tag == JAZZ_COPY_BASE_SNAPSHOT) BR(SnapshotRef__drop)(&b->snapshot);
}
static void copy_base_clone(jazz_model_BranchViewCopyBase *dst, const jazz_model_BranchViewCopyBase *src) {
    memset(dst, 0, sizeof *dst);
    dst->tag = src->tag;
    BR(BranchKey__Clone__clone)(&dst->branch, &src->branch);
    if (src->tag == JAZZ_COPY_BASE_SNAPSHOT) BR(SnapshotRef__Clone__clone)(&dst->snapshot, &src->snapshot);
}
static bool copy_base_eq(const jazz_model_BranchViewCopyBase *a, const jazz_model_BranchViewCopyBase *b) {
    return a->tag == b->tag && BR(BranchKey__PartialEq__eq)(&a->branch, &b->branch) &&
           (a->tag == JAZZ_COPY_BASE_CURRENT || BR(SnapshotRef__PartialEq__eq)(&a->snapshot, &b->snapshot));
}

void TX(BranchViewCopyEvidence__drop)(jazz_model_BranchViewCopyEvidence *e) {
    BR(BranchKey__drop)(&e->head);
    copy_base_drop(&e->base);
    jz_str_drop(&e->table);
}
static void evidence_drop_v(void *e) { TX(BranchViewCopyEvidence__drop)((jazz_model_BranchViewCopyEvidence *)e); }
void TX(BranchViewCopyEvidence__Clone__clone)(jazz_model_BranchViewCopyEvidence *dst, const jazz_model_BranchViewCopyEvidence *src) {
    dst->version = src->version;
    BR(BranchKey__Clone__clone)(&dst->head, &src->head);
    copy_base_clone(&dst->base, &src->base);
    dst->table = jz_str_clone(&src->table);
    dst->row_uuid = src->row_uuid;
    dst->source_version = src->source_version;
}
static void evidence_clone_v(void *dst, const void *src) { TX(BranchViewCopyEvidence__Clone__clone)(dst, src); }
bool TX(BranchViewCopyEvidence__PartialEq__eq)(const jazz_model_BranchViewCopyEvidence *a, const jazz_model_BranchViewCopyEvidence *b) {
    return a->version == b->version && BR(BranchKey__PartialEq__eq)(&a->head, &b->head) && copy_base_eq(&a->base, &b->base) &&
           jz_str_eq(&a->table, &b->table) && jz_uuid_eq(&a->row_uuid, &b->row_uuid) &&
           TX(TxId__PartialEq__eq)(&a->source_version, &b->source_version);
}
static bool evidence_vec_eq(const jazz_model_BranchViewCopyEvidenceVec *a, const jazz_model_BranchViewCopyEvidenceVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!TX(BranchViewCopyEvidence__PartialEq__eq)(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}

void TX(BranchViewCopyEvidence__Serialize__serialize)(jz_pc_w *w, const jazz_model_BranchViewCopyEvidence *e) {
    jz_pc_u8(w, e->version);
    BR(BranchKey__Serialize__serialize)(w, &e->head);
    jz_pc_variant(w, e->base.tag);
    BR(BranchKey__Serialize__serialize)(w, &e->base.branch);
    if (e->base.tag == JAZZ_COPY_BASE_SNAPSHOT) BR(SnapshotRef__Serialize__serialize)(w, &e->base.snapshot);
    jz_pc_str(w, &e->table);
    jz_pc_uuid(w, &e->row_uuid);
    TX(TxId__Serialize__serialize)(w, &e->source_version);
}

jz_err TX(BranchViewCopyEvidence__Deserialize__deserialize)(jz_pc_r *r, jazz_model_BranchViewCopyEvidence *out) {
    u32 base;
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_pc_r_u8(r, &out->version));
    JZ_TRY(BR(BranchKey__Deserialize__deserialize)(r, &out->head));
    JZ_TRY(jz_pc_r_variant(r, &base, 2));
    out->base.tag = (u8)base;
    JZ_TRY(BR(BranchKey__Deserialize__deserialize)(r, &out->base.branch));
    if (base == JAZZ_COPY_BASE_SNAPSHOT) JZ_TRY(BR(SnapshotRef__Deserialize__deserialize)(r, &out->base.snapshot));
    JZ_TRY(jz_pc_r_str(r, &out->table));
    JZ_TRY(jz_pc_r_uuid(r, &out->row_uuid));
    return TX(TxId__Deserialize__deserialize)(r, &out->source_version);
}

/* ---------------------------------------------------------------- BranchWriteIntent */
static void intent_drop_v(void *p) {
    jazz_model_BranchWriteIntent *i = p;
    BR(BranchKey__drop)(&i->head);
    if (i->operation.tag == JAZZ_BWO_VIEW_UPDATE_COPY) TX(BranchViewCopyEvidence__drop)(&i->operation.evidence);
}
static void intent_clone_v(void *dst, const void *src) {
    jazz_model_BranchWriteIntent *d = dst;
    const jazz_model_BranchWriteIntent *s = src;
    *d = *s;
    BR(BranchKey__Clone__clone)(&d->head, &s->head);
    if (s->operation.tag == JAZZ_BWO_VIEW_UPDATE_COPY) TX(BranchViewCopyEvidence__Clone__clone)(&d->operation.evidence, &s->operation.evidence);
}
static bool intent_eq(const jazz_model_BranchWriteIntent *a, const jazz_model_BranchWriteIntent *b) {
    if (a->version != b->version || a->physical_table_id != b->physical_table_id) return false;
    if (!jz_uuid_eq(&a->authored_schema, &b->authored_schema) || !jz_uuid_eq(&a->row_uuid, &b->row_uuid)) return false;
    if (!BR(BranchKey__PartialEq__eq)(&a->head, &b->head) || a->operation.tag != b->operation.tag) return false;
    return a->operation.tag != JAZZ_BWO_VIEW_UPDATE_COPY ||
           TX(BranchViewCopyEvidence__PartialEq__eq)(&a->operation.evidence, &b->operation.evidence);
}
static bool intent_vec_eq(const jazz_model_BranchWriteIntentVec *a, const jazz_model_BranchWriteIntentVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!intent_eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
/* (physical_table_id, authored_schema, row_uuid, &head) */
static int intent_key_cmp(const void *pa, const void *pb, void *ctx) {
    (void)ctx;
    const jazz_model_BranchWriteIntent *a = pa, *b = pb;
    if (a->physical_table_id != b->physical_table_id) return a->physical_table_id < b->physical_table_id ? -1 : 1;
    int c = jz_uuid_cmp(&a->authored_schema, &b->authored_schema);
    if (c) return c;
    if ((c = jz_uuid_cmp(&a->row_uuid, &b->row_uuid))) return c;
    return BR(BranchKey__Ord__cmp)(&a->head, &b->head, NULL);
}

static void intent_serialize(jz_pc_w *w, const jazz_model_BranchWriteIntent *i) {
    jz_pc_u8(w, i->version);
    jz_pc_varint(w, i->physical_table_id);
    jz_pc_uuid(w, &i->authored_schema);
    jz_pc_uuid(w, &i->row_uuid);
    BR(BranchKey__Serialize__serialize)(w, &i->head);
    jz_pc_variant(w, i->operation.tag);
    if (i->operation.tag == JAZZ_BWO_VIEW_UPDATE_COPY) TX(BranchViewCopyEvidence__Serialize__serialize)(w, &i->operation.evidence);
}
static jz_err intent_deserialize(jz_pc_r *r, jazz_model_BranchWriteIntent *out) {
    u32 op;
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_pc_r_u8(r, &out->version));
    JZ_TRY(jz_pc_r_varint(r, &out->physical_table_id));
    JZ_TRY(jz_pc_r_uuid(r, &out->authored_schema));
    JZ_TRY(jz_pc_r_uuid(r, &out->row_uuid));
    JZ_TRY(BR(BranchKey__Deserialize__deserialize)(r, &out->head));
    JZ_TRY(jz_pc_r_variant(r, &op, 3));
    out->operation.tag = (u8)op;
    if (op == JAZZ_BWO_VIEW_UPDATE_COPY) return TX(BranchViewCopyEvidence__Deserialize__deserialize)(r, &out->operation.evidence);
    return JZ_OK;
}

/* ---------------------------------------------------------------- ContributionMergeProvenance */
void TX(ContributionMergeProvenance__drop)(jazz_model_ContributionMergeProvenance *p) {
    BR(BranchKey__drop)(&p->source);
    BR(BranchKey__drop)(&p->target);
    jz_vec_drop(&p->substitutions, sizeof(jazz_model_ContributionSubstitution), substitution_drop_v);
    jz_vec_drop(&p->branch_view_copies, sizeof(jazz_model_BranchViewCopyEvidence), evidence_drop_v);
    jz_vec_drop(&p->branch_write_intents, sizeof(jazz_model_BranchWriteIntent), intent_drop_v);
}

void TX(ContributionMergeProvenance__Clone__clone)(jazz_model_ContributionMergeProvenance *dst, const jazz_model_ContributionMergeProvenance *src) {
    BR(BranchKey__Clone__clone)(&dst->source, &src->source);
    BR(BranchKey__Clone__clone)(&dst->target, &src->target);
    jz_vec_clone(&dst->substitutions, &src->substitutions, sizeof(jazz_model_ContributionSubstitution), substitution_clone_v);
    jz_vec_clone(&dst->branch_view_copies, &src->branch_view_copies, sizeof(jazz_model_BranchViewCopyEvidence), evidence_clone_v);
    jz_vec_clone(&dst->branch_write_intents, &src->branch_write_intents, sizeof(jazz_model_BranchWriteIntent), intent_clone_v);
}

bool TX(ContributionMergeProvenance__PartialEq__eq)(const jazz_model_ContributionMergeProvenance *a, const jazz_model_ContributionMergeProvenance *b) {
    return BR(BranchKey__PartialEq__eq)(&a->source, &b->source) && BR(BranchKey__PartialEq__eq)(&a->target, &b->target) &&
           substitution_vec_eq(&a->substitutions, &b->substitutions) &&
           evidence_vec_eq(&a->branch_view_copies, &b->branch_view_copies) &&
           intent_vec_eq(&a->branch_write_intents, &b->branch_write_intents);
}

jz_err TX(ContributionMergeProvenance__canonical)(jazz_model_BranchKey source, jazz_model_BranchKey target,
                                                  jazz_model_ContributionSubstitutionVec substitutions,
                                                  jazz_model_ContributionMergeProvenance *out) {
    jz_err e = JZ_OK;
    for (u32 i = 0; i < substitutions.len; i++) {
        jazz_model_ContributionDotVec *sources = &substitutions.ptr[i].sources;
        jz_sort(sources->ptr, sources->len, sizeof(jazz_model_ContributionDot), dot_cmp, NULL);
        jz_vec_dedup(sources, sizeof(jazz_model_ContributionDot), dot_cmp2, dot_drop_v);
        if (sources->len == 0) { e = str_err("contribution substitution requires a source dot"); break; }
    }
    if (!e.code) {
        jz_sort(substitutions.ptr, substitutions.len, sizeof(jazz_model_ContributionSubstitution), substitution_target_cmp, NULL);
        for (u32 i = 1; i < substitutions.len; i++)
            if (TX(ContributionCoordinate__PartialEq__eq)(&substitutions.ptr[i - 1].target, &substitutions.ptr[i].target)) {
                e = str_err("contribution substitution targets must be unique");
                break;
            }
    }
    if (e.code) {
        BR(BranchKey__drop)(&source);
        BR(BranchKey__drop)(&target);
        jz_vec_drop(&substitutions, sizeof(jazz_model_ContributionSubstitution), substitution_drop_v);
        return e;
    }
    memset(out, 0, sizeof *out);
    out->source = source;
    out->target = target;
    out->substitutions = substitutions;
    return JZ_OK;
}

void TX(ContributionMergeProvenance__branch_view_copy)(jazz_model_BranchViewCopyEvidence evidence, jazz_model_ContributionMergeProvenance *out) {
    memset(out, 0, sizeof *out);
    JZ_VEC_PUSH(out->branch_view_copies, evidence);
}

static bool key_invalid(const jazz_model_BranchKey *k) {
    jz_bytes b;
    jz_err e = BR(BranchKey__try_canonical_bytes)(k, &b);
    if (e.code) { jz_err_drop(&e); return true; }
    jz_bytes_drop(&b);
    return false;
}

/* BTreeSet<(String, RowUuid, BranchKey)> key */
typedef struct seen_key { const jz_str *table; jz_uuid row_uuid; const jazz_model_BranchKey *head; } seen_key;
static int seen_cmp(const void *pa, const void *pb, void *ctx) {
    (void)ctx;
    const seen_key *a = pa, *b = pb;
    int c = jz_str_cmp(a->table, b->table);
    if (c) return c;
    if ((c = jz_uuid_cmp(&a->row_uuid, &b->row_uuid))) return c;
    return BR(BranchKey__Ord__cmp)(a->head, b->head, NULL);
}
static const jz_btree_desc SEEN_DESC = { sizeof(seen_key), 0, seen_cmp, NULL, NULL };

jz_err TX(ContributionMergeProvenance__validate)(const jazz_model_ContributionMergeProvenance *p) {
    bool invalid = key_invalid(&p->source) || key_invalid(&p->target);
    for (u32 i = 0; !invalid && i < p->substitutions.len; i++) {
        const jazz_model_ContributionSubstitution *s = &p->substitutions.ptr[i];
        if (key_invalid(&s->target.branch_key)) invalid = true;
        for (u32 j = 0; !invalid && j < s->sources.len; j++)
            if (key_invalid(&s->sources.ptr[j].coordinate.branch_key)) invalid = true;
    }
    if (invalid) return str_err("contribution provenance branch key is invalid");

    jazz_model_BranchKey source, target;
    jazz_model_ContributionSubstitutionVec subs = {0};
    BR(BranchKey__Clone__clone)(&source, &p->source);
    BR(BranchKey__Clone__clone)(&target, &p->target);
    jz_vec_clone(&subs, &p->substitutions, sizeof(jazz_model_ContributionSubstitution), substitution_clone_v);
    jazz_model_ContributionMergeProvenance canonical;
    JZ_TRY(TX(ContributionMergeProvenance__canonical)(source, target, subs, &canonical));
    bool same = BR(BranchKey__PartialEq__eq)(&canonical.source, &p->source) && BR(BranchKey__PartialEq__eq)(&canonical.target, &p->target) &&
                substitution_vec_eq(&canonical.substitutions, &p->substitutions);
    TX(ContributionMergeProvenance__drop)(&canonical);
    if (!same) return str_err("contribution merge provenance must be canonical");

    jazz_model_BranchKey empty = {{0}};
    if (p->branch_view_copies.len &&
        (p->substitutions.len || !BR(BranchKey__PartialEq__eq)(&p->source, &empty) || !BR(BranchKey__PartialEq__eq)(&p->target, &empty)))
        return str_err("branch-view copy evidence must not carry contribution substitutions");

    jz_btree seen;
    jz_btree_init(&seen, &SEEN_DESC);
    for (u32 i = 0; i < p->branch_view_copies.len; i++) {
        const jazz_model_BranchViewCopyEvidence *ev = &p->branch_view_copies.ptr[i];
        seen_key k = { &ev->table, ev->row_uuid, &ev->head };
        if (ev->version != 1 || jz_btree_insert(&seen, &k, NULL, NULL)) {
            jz_btree_drop(&seen);
            return str_err("branch-view copy evidence must be canonical v1 provenance");
        }
    }
    jz_btree_drop(&seen);

    jazz_model_BranchWriteIntentVec intents = {0};
    jz_vec_clone(&intents, &p->branch_write_intents, sizeof(jazz_model_BranchWriteIntent), intent_clone_v);
    jz_sort(intents.ptr, intents.len, sizeof(jazz_model_BranchWriteIntent), intent_key_cmp, NULL);
    bool bad = !intent_vec_eq(&intents, &p->branch_write_intents);
    for (u32 i = 1; !bad && i < intents.len; i++)
        if (intent_key_cmp(&intents.ptr[i - 1], &intents.ptr[i], NULL) == 0) bad = true;
    for (u32 i = 0; !bad && i < intents.len; i++)
        if (intents.ptr[i].version != 1 || intents.ptr[i].head.values.len == 0) bad = true;
    jz_vec_drop(&intents, sizeof(jazz_model_BranchWriteIntent), intent_drop_v);
    if (bad) return str_err("branch write intents must be canonical v1 coordinates");

    jazz_model_BranchViewCopyEvidenceVec expected = {0};
    for (u32 i = 0; i < p->branch_write_intents.len; i++) {
        const jazz_model_BranchWriteIntent *intent = &p->branch_write_intents.ptr[i];
        if (intent->operation.tag != JAZZ_BWO_VIEW_UPDATE_COPY) continue;
        const jazz_model_BranchViewCopyEvidence *ev = &intent->operation.evidence;
        if (!jz_uuid_eq(&ev->row_uuid, &intent->row_uuid) || !BR(BranchKey__PartialEq__eq)(&ev->head, &intent->head)) {
            jz_vec_drop(&expected, sizeof(jazz_model_BranchViewCopyEvidence), evidence_drop_v);
            return str_err("branch write copy evidence is not bound to its intent");
        }
        jazz_model_BranchViewCopyEvidence copy;
        TX(BranchViewCopyEvidence__Clone__clone)(&copy, ev);
        JZ_VEC_PUSH(expected, copy);
    }
    bool match = evidence_vec_eq(&p->branch_view_copies, &expected);
    jz_vec_drop(&expected, sizeof(jazz_model_BranchViewCopyEvidence), evidence_drop_v);
    if (!match) return str_err("branch write copy evidence must be the canonical intent projection");
    return JZ_OK;
}

static void provenance_serialize(jz_pc_w *w, const jazz_model_ContributionMergeProvenance *p) {
    BR(BranchKey__Serialize__serialize)(w, &p->source);
    BR(BranchKey__Serialize__serialize)(w, &p->target);
    jz_pc_seq(w, p->substitutions.len);
    for (u32 i = 0; i < p->substitutions.len; i++) substitution_serialize(w, &p->substitutions.ptr[i]);
    jz_pc_seq(w, p->branch_view_copies.len);
    for (u32 i = 0; i < p->branch_view_copies.len; i++) TX(BranchViewCopyEvidence__Serialize__serialize)(w, &p->branch_view_copies.ptr[i]);
    jz_pc_seq(w, p->branch_write_intents.len);
    for (u32 i = 0; i < p->branch_write_intents.len; i++) intent_serialize(w, &p->branch_write_intents.ptr[i]);
}

static jz_err provenance_deserialize(jz_pc_r *r, jazz_model_ContributionMergeProvenance *out) {
    u32 n;
    memset(out, 0, sizeof *out);
    JZ_TRY(BR(BranchKey__Deserialize__deserialize)(r, &out->source));
    JZ_TRY(BR(BranchKey__Deserialize__deserialize)(r, &out->target));
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_model_ContributionSubstitution s;
        JZ_TRY(substitution_deserialize(r, &s));
        JZ_VEC_PUSH(out->substitutions, s);
    }
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_model_BranchViewCopyEvidence ev;
        JZ_TRY(TX(BranchViewCopyEvidence__Deserialize__deserialize)(r, &ev));
        JZ_VEC_PUSH(out->branch_view_copies, ev);
    }
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jazz_model_BranchWriteIntent in;
        JZ_TRY(intent_deserialize(r, &in));
        JZ_VEC_PUSH(out->branch_write_intents, in);
    }
    return JZ_OK;
}

/* ---------------------------------------------------------------- read sets */
static void row_read_drop_v(void *p) { jz_str_drop(&((jazz_model_RowRead *)p)->table); }
static void row_read_clone_v(void *dst, const void *src) {
    *(jazz_model_RowRead *)dst = *(const jazz_model_RowRead *)src;
    ((jazz_model_RowRead *)dst)->table = jz_str_clone(&((const jazz_model_RowRead *)src)->table);
}
static void absent_read_drop_v(void *p) { jz_str_drop(&((jazz_model_AbsentRead *)p)->table); }
static void absent_read_clone_v(void *dst, const void *src) {
    *(jazz_model_AbsentRead *)dst = *(const jazz_model_AbsentRead *)src;
    ((jazz_model_AbsentRead *)dst)->table = jz_str_clone(&((const jazz_model_AbsentRead *)src)->table);
}
static void predicate_read_drop_v(void *p) {
    jazz_model_PredicateRead *x = p;
    jz_str_drop(&x->table);
    if (x->shape) { jazz_model__query__Query__drop(x->shape); jz_free(x->shape); }
    jz_btree_drop(&x->binding_values);
}
static void predicate_read_clone_v(void *dst, const void *src) {
    jazz_model_PredicateRead *d = dst;
    const jazz_model_PredicateRead *s = src;
    d->table = jz_str_clone(&s->table);
    d->shape_id = s->shape_id;
    d->shape = jazz_model__query__Query__box_new();
    jazz_model__query__Query__Clone__clone(d->shape, s->shape);
    d->binding_id = s->binding_id;
    jz_btree_clone(&d->binding_values, &s->binding_values, jazz_model__str_clone_v, groove__records__values__Value__Clone__clone_v);
}
static bool gvalue_eq_v(const void *a, const void *b) { return GV(PartialEq__eq)(a, b); }

static jz_err predicate_read_serialize(jz_pc_w *w, const jazz_model_PredicateRead *p) {
    jz_pc_str(w, &p->table);
    jz_pc_uuid(w, &p->shape_id.id);
    JZ_TRY(jazz_model__query__Query__Serialize__serialize(w, p->shape));
    jz_pc_uuid(w, &p->binding_id.id);
    jz_str *k;
    groove_Value *v;
    jz_pc_seq(w, p->binding_values.len);
    JZ_BTREE_FOR(&p->binding_values, k, v) {
        jz_pc_str(w, k);
        GV(Serialize__serialize)(w, v);
    }
    return JZ_OK;
}

static jz_err predicate_read_deserialize(jz_pc_r *r, jazz_model_PredicateRead *out) {
    u32 n;
    memset(out, 0, sizeof *out);
    jz_btree_init(&out->binding_values, &jazz_model__STR_GVALUE_DESC);
    JZ_TRY(jz_pc_r_str(r, &out->table));
    JZ_TRY(jz_pc_r_uuid(r, &out->shape_id.id));
    out->shape = jazz_model__query__Query__box_new();
    JZ_TRY(jazz_model__query__Query__Deserialize__deserialize(r, out->shape));
    JZ_TRY(jz_pc_r_uuid(r, &out->binding_id.id));
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jz_str k;
        groove_Value v;
        JZ_TRY(jz_pc_r_str(r, &k));
        jz_err e = GV(Deserialize__deserialize)(r, &v);
        if (e.code) { jz_str_drop(&k); return e; }
        jz_btree_insert(&out->binding_values, &k, &v, NULL);
    }
    return JZ_OK;
}

/* ---------------------------------------------------------------- Transaction */
void TX(Transaction__drop)(jazz_model_Transaction *t) {
    if (t->has_base_snapshot) BR(SnapshotRef__drop)(&t->base_snapshot);
    if (t->has_row_read_set) jz_vec_drop(&t->row_read_set, sizeof(jazz_model_RowRead), row_read_drop_v);
    if (t->has_absent_read_set) jz_vec_drop(&t->absent_read_set, sizeof(jazz_model_AbsentRead), absent_read_drop_v);
    if (t->has_predicate_read_set) jz_vec_drop(&t->predicate_read_set, sizeof(jazz_model_PredicateRead), predicate_read_drop_v);
    jazz_model__OptStr__drop(&t->user_metadata_json);
    if (t->contribution_merge) {
        TX(ContributionMergeProvenance__drop)(t->contribution_merge);
        jz_free(t->contribution_merge);
    }
    memset(t, 0, sizeof *t);
}

void TX(Transaction__Clone__clone)(jazz_model_Transaction *dst, const jazz_model_Transaction *src) {
    *dst = *src;
    if (src->has_base_snapshot) BR(SnapshotRef__Clone__clone)(&dst->base_snapshot, &src->base_snapshot);
    if (src->has_row_read_set) jz_vec_clone(&dst->row_read_set, &src->row_read_set, sizeof(jazz_model_RowRead), row_read_clone_v);
    if (src->has_absent_read_set) jz_vec_clone(&dst->absent_read_set, &src->absent_read_set, sizeof(jazz_model_AbsentRead), absent_read_clone_v);
    if (src->has_predicate_read_set)
        jz_vec_clone(&dst->predicate_read_set, &src->predicate_read_set, sizeof(jazz_model_PredicateRead), predicate_read_clone_v);
    dst->user_metadata_json = jazz_model__OptStr__clone(&src->user_metadata_json);
    if (src->contribution_merge) {
        dst->contribution_merge = JZ_NEW(jazz_model_ContributionMergeProvenance);
        TX(ContributionMergeProvenance__Clone__clone)(dst->contribution_merge, src->contribution_merge);
    }
}

bool TX(Transaction__PartialEq__eq)(const jazz_model_Transaction *a, const jazz_model_Transaction *b) {
    if (!TX(TxId__PartialEq__eq)(&a->tx_id, &b->tx_id) || a->kind != b->kind || a->n_total_writes != b->n_total_writes) return false;
    if (a->made_by.interned != b->made_by.interned || a->made_by.tag != b->made_by.tag) return false;
    if (a->has_permission_subject != b->has_permission_subject) return false;
    if (a->has_permission_subject && a->permission_subject.interned != b->permission_subject.interned) return false;
    if (a->has_base_snapshot != b->has_base_snapshot) return false;
    if (a->has_base_snapshot && !BR(SnapshotRef__PartialEq__eq)(&a->base_snapshot, &b->base_snapshot)) return false;
    if (a->has_row_read_set != b->has_row_read_set || a->row_read_set.len != b->row_read_set.len) return false;
    for (u32 i = 0; i < a->row_read_set.len; i++) {
        const jazz_model_RowRead *x = &a->row_read_set.ptr[i], *y = &b->row_read_set.ptr[i];
        if (!jz_str_eq(&x->table, &y->table) || !jz_uuid_eq(&x->row_uuid, &y->row_uuid) || !TX(TxId__PartialEq__eq)(&x->version, &y->version))
            return false;
    }
    if (a->has_absent_read_set != b->has_absent_read_set || a->absent_read_set.len != b->absent_read_set.len) return false;
    for (u32 i = 0; i < a->absent_read_set.len; i++) {
        const jazz_model_AbsentRead *x = &a->absent_read_set.ptr[i], *y = &b->absent_read_set.ptr[i];
        if (!jz_str_eq(&x->table, &y->table) || !jz_uuid_eq(&x->row_uuid, &y->row_uuid)) return false;
    }
    if (a->has_predicate_read_set != b->has_predicate_read_set || a->predicate_read_set.len != b->predicate_read_set.len) return false;
    for (u32 i = 0; i < a->predicate_read_set.len; i++) {
        const jazz_model_PredicateRead *x = &a->predicate_read_set.ptr[i], *y = &b->predicate_read_set.ptr[i];
        if (!jz_str_eq(&x->table, &y->table) || !jz_uuid_eq(&x->shape_id.id, &y->shape_id.id) ||
            !jazz_model__query__Query__PartialEq__eq(x->shape, y->shape) || !jz_uuid_eq(&x->binding_id.id, &y->binding_id.id) ||
            !jz_btree_eq(&x->binding_values, &y->binding_values, gvalue_eq_v))
            return false;
    }
    if (!jazz_model__OptStr__eq(&a->user_metadata_json, &b->user_metadata_json)) return false;
    if ((a->contribution_merge == NULL) != (b->contribution_merge == NULL)) return false;
    return !a->contribution_merge || TX(ContributionMergeProvenance__PartialEq__eq)(a->contribution_merge, b->contribution_merge);
}

jz_err TX(Transaction__Serialize__serialize)(jz_pc_w *w, const jazz_model_Transaction *t) {
    TX(TxId__Serialize__serialize)(w, &t->tx_id);
    TX(TxKind__Serialize__serialize)(w, t->kind);
    jz_pc_varint(w, t->n_total_writes);
    jazz_types__ids__AuthorSubject__Serialize__serialize(w, &t->made_by);
    jz_pc_option(w, t->has_permission_subject);
    if (t->has_permission_subject) jazz_types__ids__AuthorSubject__Serialize__serialize(w, &t->permission_subject);
    jz_pc_option(w, t->has_base_snapshot);
    if (t->has_base_snapshot) BR(SnapshotRef__Serialize__serialize)(w, &t->base_snapshot);
    jz_pc_option(w, t->has_row_read_set);
    if (t->has_row_read_set) {
        jz_pc_seq(w, t->row_read_set.len);
        for (u32 i = 0; i < t->row_read_set.len; i++) {
            jz_pc_str(w, &t->row_read_set.ptr[i].table);
            jz_pc_uuid(w, &t->row_read_set.ptr[i].row_uuid);
            TX(TxId__Serialize__serialize)(w, &t->row_read_set.ptr[i].version);
        }
    }
    jz_pc_option(w, t->has_absent_read_set);
    if (t->has_absent_read_set) {
        jz_pc_seq(w, t->absent_read_set.len);
        for (u32 i = 0; i < t->absent_read_set.len; i++) {
            jz_pc_str(w, &t->absent_read_set.ptr[i].table);
            jz_pc_uuid(w, &t->absent_read_set.ptr[i].row_uuid);
        }
    }
    jz_pc_option(w, t->has_predicate_read_set);
    if (t->has_predicate_read_set) {
        jz_pc_seq(w, t->predicate_read_set.len);
        for (u32 i = 0; i < t->predicate_read_set.len; i++) JZ_TRY(predicate_read_serialize(w, &t->predicate_read_set.ptr[i]));
    }
    jz_pc_option(w, t->user_metadata_json.some);
    if (t->user_metadata_json.some) jz_pc_str(w, &t->user_metadata_json.s);
    jz_pc_option(w, t->contribution_merge != NULL);
    if (t->contribution_merge) provenance_serialize(w, t->contribution_merge);
    return JZ_OK;
}

static jz_err transaction_fields(jz_pc_r *r, jazz_model_Transaction *out) {
    bool some;
    u32 n;
    JZ_TRY(TX(TxId__Deserialize__deserialize)(r, &out->tx_id));
    JZ_TRY(TX(TxKind__Deserialize__deserialize)(r, &out->kind));
    JZ_TRY(jz_pc_r_u32(r, &out->n_total_writes));
    JZ_TRY(jazz_types__ids__AuthorSubject__Deserialize__deserialize(r, &out->made_by));
    JZ_TRY(jz_pc_r_option(r, &out->has_permission_subject));
    if (out->has_permission_subject) JZ_TRY(jazz_types__ids__AuthorSubject__Deserialize__deserialize(r, &out->permission_subject));
    JZ_TRY(jz_pc_r_option(r, &some));
    if (some) {
        JZ_TRY(BR(SnapshotRef__Deserialize__deserialize)(r, &out->base_snapshot));
        out->has_base_snapshot = true;
    }
    JZ_TRY(jz_pc_r_option(r, &out->has_row_read_set));
    if (out->has_row_read_set) {
        JZ_TRY(jz_pc_r_seq(r, &n));
        for (u32 i = 0; i < n; i++) {
            jazz_model_RowRead rr;
            JZ_TRY(jz_pc_r_str(r, &rr.table));
            JZ_VEC_PUSH(out->row_read_set, rr);
            jazz_model_RowRead *slot = &out->row_read_set.ptr[out->row_read_set.len - 1];
            JZ_TRY(jz_pc_r_uuid(r, &slot->row_uuid));
            JZ_TRY(TX(TxId__Deserialize__deserialize)(r, &slot->version));
        }
    }
    JZ_TRY(jz_pc_r_option(r, &out->has_absent_read_set));
    if (out->has_absent_read_set) {
        JZ_TRY(jz_pc_r_seq(r, &n));
        for (u32 i = 0; i < n; i++) {
            jazz_model_AbsentRead ar;
            JZ_TRY(jz_pc_r_str(r, &ar.table));
            JZ_VEC_PUSH(out->absent_read_set, ar);
            JZ_TRY(jz_pc_r_uuid(r, &out->absent_read_set.ptr[out->absent_read_set.len - 1].row_uuid));
        }
    }
    JZ_TRY(jz_pc_r_option(r, &out->has_predicate_read_set));
    if (out->has_predicate_read_set) {
        JZ_TRY(jz_pc_r_seq(r, &n));
        for (u32 i = 0; i < n; i++) {
            jazz_model_PredicateRead pr;
            jz_err e = predicate_read_deserialize(r, &pr);
            if (e.code) { predicate_read_drop_v(&pr); return e; }
            JZ_VEC_PUSH(out->predicate_read_set, pr);
        }
    }
    JZ_TRY(jz_pc_r_option(r, &out->user_metadata_json.some));
    if (out->user_metadata_json.some) JZ_TRY(jz_pc_r_str(r, &out->user_metadata_json.s));
    /* #[serde(default)] contribution_merge: a missing trailing field is None */
    if (r->p == r->end) return JZ_OK;
    JZ_TRY(jz_pc_r_option(r, &some));
    if (some) {
        out->contribution_merge = JZ_NEW(jazz_model_ContributionMergeProvenance);
        jz_err e = provenance_deserialize(r, out->contribution_merge);
        if (e.code) return e;
    }
    return JZ_OK;
}

jz_err TX(Transaction__Deserialize__deserialize)(jz_pc_r *r, jazz_model_Transaction *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_pc_r_enter(r));
    jz_err e = transaction_fields(r, out);
    jz_pc_r_leave(r);
    if (e.code) TX(Transaction__drop)(out);
    return e;
}

/* ---------------------------------------------------------------- records */
void TX(TransactionRecord__drop)(jazz_model_TransactionRecord *t) {
    TX(Fate__drop)(&t->fate);
    jazz_model__OptStr__drop(&t->user_metadata_json);
    if (t->contribution_merge) {
        TX(ContributionMergeProvenance__drop)(t->contribution_merge);
        jz_free(t->contribution_merge);
        t->contribution_merge = NULL;
    }
}

void TX(HistoryEntry__drop)(jazz_model_HistoryEntry *h) {
    jz_str_drop(&h->table);
    groove__records__OwnedRecord__drop(&h->version);
    TX(TransactionRecord__drop)(&h->transaction);
}

void TX(RejectedVersion__drop)(jazz_model_RejectedVersion *v) {
    jz_str_drop(&v->table);
    groove__records__OwnedRecord__drop(&v->record);
}
static void rejected_version_drop_v(void *v) { TX(RejectedVersion__drop)((jazz_model_RejectedVersion *)v); }

void TX(RejectedTransaction__drop)(jazz_model_RejectedTransaction *t) {
    groove__records__OwnedRecord__drop(&t->record);
    jz_vec_drop(&t->versions, sizeof(jazz_model_RejectedVersion), rejected_version_drop_v);
}

jz_str TX(RejectedVersion__table)(const jazz_model_RejectedVersion *v) { return jz_str_clone(&v->table); }

jazz_types_RowUuid TX(RejectedVersion__row_uuid)(const jazz_model_RejectedVersion *v) {
    groove_BorrowedRecord b = { jz_bytes_slice(&v->record.raw), v->record.descriptor };
    jz_uuid u;
    if (groove__records__BorrowedRecord__get_uuid(b, REJECTED_VERSION_FIELD_ROW_UUID_IDX, &u).code) jz_panic("valid rejected row uuid");
    return u;
}

bool TX(RejectedVersion__deletion)(const jazz_model_RejectedVersion *v, u8 *out) {
    groove_BorrowedRecord b = { jz_bytes_slice(&v->record.raw), v->record.descriptor };
    groove_Value value;
    if (groove__records__BorrowedRecord__get_idx(b, REJECTED_VERSION_FIELD__DELETION_IDX, &value).code) jz_panic("valid rejected deletion");
    bool some;
    if (jazz_model__tx__deletion_from_value(value, &some, out).code) jz_panic("valid rejected deletion");
    return some;
}
