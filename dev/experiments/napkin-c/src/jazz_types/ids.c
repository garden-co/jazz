/* crates/jazz/layers/types/src/ids.rs */
#include "jazz_types.h"

const char jazz_types__ids__AuthorSubject__SYSTEM_ISSUER[] = "urn:jazz:system";
const char jazz_types__ids__AuthorSubject__SYSTEM_SUBJECT[] = "system";
const char jazz_types__ids__AuthorSubject__SYSTEM_CANONICAL[] = "[\"urn:jazz:system\",\"system\"]";
const char jazz_types__ids__AuthorSubject__LOCAL_FIRST_ISSUER[] = "urn:jazz:local-first";
const char jazz_types__ids__AuthorSubject__STATIC_BEARER_ISSUER[] = "urn:jazz:static-bearer";
const char jazz_types__ids__AuthorSubject__ANONYMOUS_ISSUER[] = "urn:jazz:anonymous";
const jazz_types_AuthorSubject jazz_types__ids__AuthorSubject__SYSTEM = { JAZZ_AUTHOR_SYSTEM, NULL };

#define SYSTEM_ISSUER jazz_types__ids__AuthorSubject__SYSTEM_ISSUER
#define SYSTEM_SUBJECT jazz_types__ids__AuthorSubject__SYSTEM_SUBJECT
#define SYSTEM_CANONICAL jazz_types__ids__AuthorSubject__SYSTEM_CANONICAL

static bool slice_eq_cstr(jz_slice s, const char *c) {
    u32 n = (u32)strlen(c);
    return s.len == n && memcmp(s.ptr, c, n) == 0;
}
static bool str_eq_cstr(const jz_str *s, const char *c) { return jz_str_eq_cstr(s, c); }

/* ---------------- uuid_id! / id_newtype! helpers ---------------- */
jz_bytes jazz_types__ids__SchemaFamilyId__to_bytes(jazz_types_SchemaFamilyId id) { return jz_bytes_from(id.b, 16); }

void jazz_types__ids__uuid_id__Debug__fmt(jz_str *out, const char *name, const jz_uuid *u) {
    char buf[37];
    jz_uuid_to_string(u, buf);
    jz_str_appendf(out, "%s(%s)", name, buf);
}
void jazz_types__ids__u64_id__Debug__fmt(jz_str *out, const char *name, u64 v) {
    jz_str_appendf(out, "%s(%llu)", name, (unsigned long long)v);
}

/* ---------------- AuthorSubjectError ---------------- */
void jazz_types__ids__AuthorSubjectError__Display__fmt(jz_str *out, u32 code, const jz_str *detail) {
    switch (code) {
    case JAZZ_TYPES_ERR_MISSING_ISSUER: jz_str_push_cstr(out, "author issuer must be non-empty"); break;
    case JAZZ_TYPES_ERR_MISSING_SUBJECT: jz_str_push_cstr(out, "author subject must be non-empty"); break;
    case JAZZ_TYPES_ERR_RESERVED_ISSUER: jz_str_appendf(out, "author issuer is reserved: %.*s", (int)detail->len, detail->ptr); break;
    case JAZZ_TYPES_ERR_INVALID_CANONICAL: jz_str_appendf(out, "invalid canonical author subject: %.*s", (int)detail->len, detail->ptr); break;
    case JAZZ_TYPES_ERR_NON_CANONICAL: jz_str_push_cstr(out, "author subject is not canonically JSON encoded"); break;
    case JAZZ_TYPES_ERR_MISSING_ACCOUNT: jz_str_push_cstr(out, "persisted row author requires an admitted account"); break;
    case JAZZ_TYPES_ERR_RESERVED_ACCOUNT: jz_str_push_cstr(out, "author account is reserved"); break;
    case JAZZ_TYPES_ERR_INVALID_SYSTEM_ORIGIN: jz_str_push_cstr(out, "system row author must retain a node UUID subject"); break;
    default: JZ_UNREACHABLE();
    }
}

void jazz_types__ids__AuthorSubjectError__Debug__fmt(jz_str *out, u32 code, const jz_str *detail) {
    static const char *const names[] = { "MissingIssuer", "MissingSubject", "ReservedIssuer", "InvalidCanonical",
                                         "NonCanonical", "MissingAccount", "ReservedAccount", "InvalidSystemOrigin" };
    jz_str_push_cstr(out, names[code - JAZZ_TYPES_ERR_MISSING_ISSUER]);
    if (code == JAZZ_TYPES_ERR_RESERVED_ISSUER || code == JAZZ_TYPES_ERR_INVALID_CANONICAL)
        jz_str_appendf(out, "(\"%.*s\")", (int)detail->len, detail->ptr);
}

jz_err jazz_types__ids__author_subject_error(u32 code, const char *detail, u32 detail_len) {
    jz_str d = jz_str_from_slice(jz_slice_of(detail, detail_len));
    jz_str msg = {0};
    jazz_types__ids__AuthorSubjectError__Display__fmt(&msg, code, &d);
    jz_str_drop(&d);
    jz_err e = jz_err_fmt(code, "%.*s", (int)msg.len, msg.ptr);
    jz_str_drop(&msg);
    return e;
}
#define ASE(code) jazz_types__ids__author_subject_error((code), "", 0)
#define ASE_S(code, s) jazz_types__ids__author_subject_error((code), (const char *)(s).ptr, (s).len)

/* ---------------- InternedAuthor + interning ---------------- */
void jazz_types__ids__InternedAuthor__Debug__fmt(jz_str *out, const jazz_types_InternedAuthor *a) {
    jz_str_push_cstr(out, "InternedAuthor { account: ");
    if (a->has_account) {
        jazz_types__ids__uuid_id__Debug__fmt(out, "Some(AccountId", &a->account);
        jz_str_push_char(out, ')');
    } else {
        jz_str_push_cstr(out, "None");
    }
    jz_str_appendf(out, ", issuer: \"%.*s\", subject: \"%.*s\", canonical: \"%.*s\" }", (int)a->issuer.len, a->issuer.ptr,
                   (int)a->subject.len, a->subject.ptr, (int)a->canonical.len, a->canonical.ptr);
}

/* AuthorIdentity::identity() based hash / eq; caches do not participate. */
static u64 jazz_types__ids__InternedAuthor__Hash__hash(const void *key) {
    const jazz_types_InternedAuthor *a = *(const jazz_types_InternedAuthor *const *)key;
    u64 h = jz_hash_u64(a->has_account);
    if (a->has_account) h = jz_hash_combine(h, jz_hash_bytes(a->account.b, 16));
    h = jz_hash_combine(h, jz_hash_bytes(a->issuer.ptr, a->issuer.len));
    h = jz_hash_combine(h, jz_hash_bytes(a->subject.ptr, a->subject.len));
    return h;
}
static bool jazz_types__ids__InternedAuthor__PartialEq__eq(const void *x, const void *y) {
    const jazz_types_InternedAuthor *a = *(const jazz_types_InternedAuthor *const *)x;
    const jazz_types_InternedAuthor *b = *(const jazz_types_InternedAuthor *const *)y;
    if (a->has_account != b->has_account) return false;
    if (a->has_account && !jz_uuid_eq(&a->account, &b->account)) return false;
    return jz_str_eq(&a->issuer, &b->issuer) && jz_str_eq(&a->subject, &b->subject);
}
static const jz_hmap_desc AUTHOR_POOL_DESC = {
    sizeof(jazz_types_InternedAuthor *), sizeof(jazz_types_InternedAuthor *), /* key and value: the pooled pointer */
    jazz_types__ids__InternedAuthor__Hash__hash, jazz_types__ids__InternedAuthor__PartialEq__eq, NULL, NULL };
static jz_hmap AUTHOR_POOL;
static bool AUTHOR_POOL_INIT;

/* From<&dyn AuthorIdentity> for InternedAuthor: derive the canonical spelling once. */
static jazz_types_InternedAuthor *jazz_types__ids__InternedAuthor__from_identity(bool has_account, jazz_types_AccountId account,
                                                                                 jz_slice issuer, jz_slice subject) {
    jz_str canonical = {0};
    jz_json_w w;
    memset(&w, 0, sizeof w);
    w.out = &canonical;
    jz_jw_begin_arr(&w);
    if (has_account) {
        char buf[37];
        jz_uuid_to_string(&account, buf);
        jz_jw_str(&w, buf, 36);
    }
    jz_jw_str(&w, (const char *)issuer.ptr, issuer.len);
    jz_jw_str(&w, (const char *)subject.ptr, subject.len);
    jz_jw_end_arr(&w);
    jazz_types_InternedAuthor *a = JZ_NEW(jazz_types_InternedAuthor);
    a->has_account = has_account;
    a->account = account;
    a->issuer = jz_str_from_slice(issuer);
    a->subject = jz_str_from_slice(subject);
    a->canonical = canonical;
    return a;
}

/* intern_author: Intern::from_ref(&BorrowedAuthor(..) as &dyn AuthorIdentity) */
static const jazz_types_InternedAuthor *jazz_types__ids__intern_author(bool has_account, jazz_types_AccountId account,
                                                                     jz_slice issuer, jz_slice subject) {
    if (!AUTHOR_POOL_INIT) { jz_hmap_init(&AUTHOR_POOL, &AUTHOR_POOL_DESC); AUTHOR_POOL_INIT = true; }
    /* BorrowedAuthor lookup key: strings borrowed, nothing allocated. */
    jazz_types_InternedAuthor borrowed;
    memset(&borrowed, 0, sizeof borrowed);
    borrowed.has_account = has_account;
    borrowed.account = account;
    borrowed.issuer.ptr = (char *)issuer.ptr; borrowed.issuer.len = issuer.len;
    borrowed.subject.ptr = (char *)subject.ptr; borrowed.subject.len = subject.len;
    jazz_types_InternedAuthor *key = &borrowed;
    jazz_types_InternedAuthor **found = jz_hmap_get(&AUTHOR_POOL, &key);
    if (found) return *found;
    jazz_types_InternedAuthor *owned = jazz_types__ids__InternedAuthor__from_identity(has_account, account, issuer, subject);
    jz_hmap_insert(&AUTHOR_POOL, &owned, &owned, NULL);
    return owned;
}

/* ---------------- descriptors ---------------- */
static groove_ValueType vt_scalar(u8 tag) { groove_ValueType t; memset(&t, 0, sizeof t); t.tag = tag; return t; }
static groove_ValueType vt_record(groove_RecordDescriptor d) {
    groove_ValueType t; memset(&t, 0, sizeof t); t.tag = GROOVE_VT_RECORD;
    t.u.record = jz_alloc(sizeof *t.u.record); *t.u.record = d; return t;
}
static groove_ValueType vt_nullable(groove_ValueType inner) {
    groove_ValueType t; memset(&t, 0, sizeof t); t.tag = GROOVE_VT_NULLABLE;
    t.u.inner = jz_alloc(sizeof *t.u.inner); *t.u.inner = inner; return t;
}
static groove_RecordDescriptor descriptor2(const char *n0, groove_ValueType t0, const char *n1, groove_ValueType t1) {
    jz_str names[2] = { jz_str_from(n0), jz_str_from(n1) };
    groove_ValueType types[2] = { t0, t1 };
    groove_RecordDescriptor d = groove__records__RecordDescriptor__new(names, types, 2);
    jz_str_drop(&names[0]); jz_str_drop(&names[1]);
    return d;
}

/* The (issuer, subject) principal record shared by row authors and the account registry. */
groove_RecordDescriptor jazz_types__ids__author_identity_descriptor(void) {
    static bool init; static groove_RecordDescriptor DESCRIPTOR;
    if (!init) {
        DESCRIPTOR = descriptor2("issuer", vt_scalar(GROOVE_VT_STRING), "subject", vt_scalar(GROOVE_VT_STRING));
        init = true;
    }
    return DESCRIPTOR;
}

static groove_RecordDescriptor jazz_types__ids__RowAuthor__record_descriptor(void) {
    static bool init; static groove_RecordDescriptor DESCRIPTOR;
    if (!init) {
        DESCRIPTOR = descriptor2("account", vt_scalar(GROOVE_VT_UUID), "identity",
                                 vt_record(jazz_types__ids__author_identity_descriptor()));
        init = true;
    }
    return DESCRIPTOR;
}

static groove_RecordDescriptor jazz_types__ids__AuthorSubject__record_descriptor(void) {
    static bool init; static groove_RecordDescriptor DESCRIPTOR;
    if (!init) {
        DESCRIPTOR = descriptor2("account", vt_nullable(vt_scalar(GROOVE_VT_UUID)), "identity",
                                 vt_record(jazz_types__ids__author_identity_descriptor()));
        init = true;
    }
    return DESCRIPTOR;
}

static groove_Value v_string(jz_str s) { groove_Value v; memset(&v, 0, sizeof v); v.tag = GROOVE_V_STRING; v.u.s = s; return v; }
static groove_Value v_uuid(jz_uuid u) { groove_Value v; memset(&v, 0, sizeof v); v.tag = GROOVE_V_UUID; v.u.uuid = u; return v; }
static groove_Value v_record(groove_OwnedRecord r) { groove_Value v; memset(&v, 0, sizeof v); v.tag = GROOVE_V_RECORD; v.u.record = r; return v; }

/* Shared body of AuthorSubject::build_record / RowAuthor::build_record. */
static groove_OwnedRecord build_author_record(jz_str issuer, jz_str subject, groove_RecordDescriptor descriptor,
                                              groove_Value account, const char *identity_msg, const char *record_msg) {
    groove_RecordDescriptor identity = jazz_types__ids__author_identity_descriptor();
    groove_Value parts[2] = { v_string(issuer), v_string(subject) };
    jz_bytes raw;
    jz_err e = groove__records__RecordDescriptor__create(identity, parts, 2, &raw);
    groove__records__values__Value__drop(&parts[0]);
    groove__records__values__Value__drop(&parts[1]);
    if (e.code) jz_panic(identity_msg);
    groove_Value values[2] = { account, v_record(groove__records__OwnedRecord__new(raw, identity)) };
    e = groove__records__RecordDescriptor__create(descriptor, values, 2, &raw);
    groove__records__values__Value__drop(&values[0]);
    groove__records__values__Value__drop(&values[1]);
    if (e.code) jz_panic(record_msg);
    return groove__records__OwnedRecord__new(raw, descriptor);
}

/* Decode the (issuer, subject) identity sub-record at field 1. */
static jz_err decode_identity(groove_BorrowedRecord record, jz_slice *issuer, jz_slice *subject, bool *bad) {
    u32 start, end;
    *bad = true;
    jz_err e = groove__records__RecordDescriptor__field_span(record.descriptor, record.raw, 1, &start, &end);
    if (e.code) { jz_err_drop(&e); return JZ_OK; }
    groove_RecordDescriptor identity_descriptor = jazz_types__ids__author_identity_descriptor();
    groove_BorrowedRecord identity = groove__records__RecordDescriptor__bind(identity_descriptor, jz_slice_of(record.raw.ptr + start, end - start));
    e = groove__records__BorrowedRecord__get_str(&identity, 0, issuer);
    if (e.code) { jz_err_drop(&e); return JZ_OK; }
    e = groove__records__BorrowedRecord__get_str(&identity, 1, subject);
    if (e.code) { jz_err_drop(&e); return JZ_OK; }
    *bad = false;
    return JZ_OK;
}

/* ---------------- parse helpers ---------------- */
static jz_err jazz_types__ids__parse_system_origin(jz_slice subject, jazz_types_NodeUuid *out) {
    jz_uuid node;
    jz_err e = jz_uuid_parse(subject, &node);
    if (e.code) { jz_err_drop(&e); return ASE(JAZZ_TYPES_ERR_INVALID_SYSTEM_ORIGIN); }
    char buf[37];
    jz_uuid_to_string(&node, buf); /* hyphenated().encode_lower() */
    if (subject.len != 36 || memcmp(buf, subject.ptr, 36) != 0) return ASE(JAZZ_TYPES_ERR_INVALID_SYSTEM_ORIGIN);
    *out = node;
    return JZ_OK;
}

typedef struct { bool has_account; jazz_types_AccountId account; jz_str issuer; jz_str subject; } CanonicalParts;

static jz_err jazz_types__ids__parse_canonical_parts(jz_slice canonical, CanonicalParts *out) {
    jz_json *json;
    jz_err e = jz_json_parse(canonical, &json);
    if (e.code) {
        jz_str detail = {0};
        if (e.msg) detail = jz_str_clone(e.msg);
        jz_err_drop(&e);
        jz_err r = ASE_S(JAZZ_TYPES_ERR_INVALID_CANONICAL, detail);
        jz_str_drop(&detail);
        return r;
    }
    /* Vec<String> */
    if (json->kind != JZ_JSON_ARR) {
        jz_json_free(json);
        return jazz_types__ids__author_subject_error(JAZZ_TYPES_ERR_INVALID_CANONICAL, "invalid type: expected a sequence", 33);
    }
    for (u32 i = 0; i < json->v.arr.len; i++) {
        if (json->v.arr.ptr[i]->kind != JZ_JSON_STR) {
            jz_json_free(json);
            return jazz_types__ids__author_subject_error(JAZZ_TYPES_ERR_INVALID_CANONICAL, "invalid type: expected a string", 31);
        }
    }
    memset(out, 0, sizeof *out);
    if (json->v.arr.len == 2) {
        out->issuer = jz_str_clone(&json->v.arr.ptr[0]->v.s);
        out->subject = jz_str_clone(&json->v.arr.ptr[1]->v.s);
    } else if (json->v.arr.len == 3) {
        jz_uuid uuid;
        e = jz_uuid_parse(jz_str_slice(&json->v.arr.ptr[0]->v.s), &uuid);
        if (e.code) {
            jz_str detail = {0};
            if (e.msg) detail = jz_str_clone(e.msg);
            jz_err_drop(&e);
            jz_json_free(json);
            jz_err r = ASE_S(JAZZ_TYPES_ERR_INVALID_CANONICAL, detail);
            jz_str_drop(&detail);
            return r;
        }
        out->has_account = true;
        out->account = uuid;
        out->issuer = jz_str_clone(&json->v.arr.ptr[1]->v.s);
        out->subject = jz_str_clone(&json->v.arr.ptr[2]->v.s);
    } else {
        jz_json_free(json);
        return jazz_types__ids__author_subject_error(JAZZ_TYPES_ERR_INVALID_CANONICAL, "expected principal or account author", 36);
    }
    jz_json_free(json);
    return JZ_OK;
}

static void canonical_parts_drop(CanonicalParts *p) { jz_str_drop(&p->issuer); jz_str_drop(&p->subject); }

/* ---------------- AuthorSubject ---------------- */
jz_slice jazz_types__ids__AuthorSubject__canonical(const jazz_types_AuthorSubject *s) {
    switch (s->tag) {
    case JAZZ_AUTHOR_SYSTEM: return JZ_LIT(SYSTEM_CANONICAL);
    case JAZZ_AUTHOR_SYSTEM_AT:
    case JAZZ_AUTHOR_AUTHENTICATED: return jz_str_slice(&s->interned->canonical);
    }
    JZ_UNREACHABLE();
}

bool jazz_types_AuthorSubject_eq(const jazz_types_AuthorSubject *a, const jazz_types_AuthorSubject *b) {
    return a->tag == b->tag && a->interned == b->interned;
}

int jazz_types__ids__AuthorSubject__Ord__cmp(const jazz_types_AuthorSubject *a, const jazz_types_AuthorSubject *b) {
    return jz_slice_cmp(jazz_types__ids__AuthorSubject__canonical(a), jazz_types__ids__AuthorSubject__canonical(b));
}
int jazz_types__ids__AuthorSubject__cmp_v(const void *a, const void *b, void *ctx) {
    (void)ctx;
    return jazz_types__ids__AuthorSubject__Ord__cmp(a, b);
}
/* PartialOrd::partial_cmp -> Some(cmp) */
int jazz_types__ids__AuthorSubject__PartialOrd__partial_cmp(const jazz_types_AuthorSubject *a, const jazz_types_AuthorSubject *b) {
    return jazz_types__ids__AuthorSubject__Ord__cmp(a, b);
}

void jazz_types__ids__AuthorSubject__Debug__fmt(jz_str *out, const jazz_types_AuthorSubject *s) {
    switch (s->tag) {
    case JAZZ_AUTHOR_SYSTEM: jz_str_push_cstr(out, "System"); return;
    case JAZZ_AUTHOR_SYSTEM_AT: jz_str_push_cstr(out, "SystemAt("); break;
    case JAZZ_AUTHOR_AUTHENTICATED: jz_str_push_cstr(out, "Authenticated("); break;
    }
    jazz_types__ids__InternedAuthor__Debug__fmt(out, s->interned);
    jz_str_push_char(out, ')');
}

static bool jazz_types__ids__AuthorSubject__is_reserved_issuer(jz_slice issuer) {
    return slice_eq_cstr(issuer, SYSTEM_ISSUER) || slice_eq_cstr(issuer, jazz_types__ids__AuthorSubject__LOCAL_FIRST_ISSUER) ||
           slice_eq_cstr(issuer, jazz_types__ids__AuthorSubject__STATIC_BEARER_ISSUER) ||
           slice_eq_cstr(issuer, jazz_types__ids__AuthorSubject__ANONYMOUS_ISSUER);
}

static jazz_types_AuthorSubject jazz_types__ids__AuthorSubject__intern(jz_slice issuer, jz_slice subject) {
    jazz_types_AuthorSubject s = { JAZZ_AUTHOR_AUTHENTICATED, NULL };
    jazz_types_AccountId none = {{0}};
    s.interned = jazz_types__ids__intern_author(false, none, issuer, subject);
    return s;
}

jz_err jazz_types__ids__AuthorSubject__authenticated(jz_slice issuer, jz_slice subject, jazz_types_AuthorSubject *out) {
    if (!jazz_types__identity__principal_is_nonempty(issuer)) return ASE(JAZZ_TYPES_ERR_MISSING_ISSUER);
    if (!jazz_types__identity__principal_is_nonempty(subject)) return ASE(JAZZ_TYPES_ERR_MISSING_SUBJECT);
    if (jazz_types__ids__AuthorSubject__is_reserved_issuer(issuer)) return ASE_S(JAZZ_TYPES_ERR_RESERVED_ISSUER, issuer);
    *out = jazz_types__ids__AuthorSubject__intern(issuer, subject);
    return JZ_OK;
}

jz_err jazz_types__ids__AuthorSubject__reserved(jz_slice issuer, jz_slice subject, jazz_types_AuthorSubject *out) {
    if (!jazz_types__identity__principal_is_nonempty(subject)) return ASE(JAZZ_TYPES_ERR_MISSING_SUBJECT);
    if (!(slice_eq_cstr(issuer, jazz_types__ids__AuthorSubject__LOCAL_FIRST_ISSUER) ||
          slice_eq_cstr(issuer, jazz_types__ids__AuthorSubject__STATIC_BEARER_ISSUER) ||
          slice_eq_cstr(issuer, jazz_types__ids__AuthorSubject__ANONYMOUS_ISSUER)))
        return ASE_S(JAZZ_TYPES_ERR_RESERVED_ISSUER, issuer);
    *out = jazz_types__ids__AuthorSubject__intern(issuer, subject);
    return JZ_OK;
}

jazz_types_AuthorSubject jazz_types__ids__AuthorSubject__system_at(jazz_types_NodeUuid node) {
    char buf[37];
    jz_uuid_to_string(&node, buf);
    jazz_types_AuthorSubject s = { JAZZ_AUTHOR_SYSTEM_AT, NULL };
    s.interned = jazz_types__ids__intern_author(true, jazz_types__account_registry__SYSTEM_ACCOUNT_ID, JZ_LIT(SYSTEM_ISSUER),
                                                jz_slice_of(buf, 36));
    return s;
}

jazz_types_AuthorSubject jazz_types__ids__AuthorSubject__with_account(jazz_types_AuthorSubject s, jazz_types_AccountId account) {
    JZ_ASSERT(s.tag == JAZZ_AUTHOR_AUTHENTICATED, "system is not an account principal");
    JZ_ASSERT(!jazz_types__account_registry__AccountId__is_system(account), "system account is not user-owned");
    jazz_types_AuthorSubject r = { JAZZ_AUTHOR_AUTHENTICATED, NULL };
    r.interned = jazz_types__ids__intern_author(true, account, jz_str_slice(&s.interned->issuer), jz_str_slice(&s.interned->subject));
    return r;
}

void jazz_types__ids__AuthorSubject__principal_parts(const jazz_types_AuthorSubject *s, jz_str *issuer, jz_str *subject) {
    switch (s->tag) {
    case JAZZ_AUTHOR_SYSTEM:
        *issuer = jz_str_from(SYSTEM_ISSUER);
        *subject = jz_str_from(SYSTEM_SUBJECT);
        return;
    case JAZZ_AUTHOR_SYSTEM_AT:
    case JAZZ_AUTHOR_AUTHENTICATED:
        *issuer = jz_str_clone(&s->interned->issuer);
        *subject = jz_str_clone(&s->interned->subject);
        return;
    }
}

bool jazz_types__ids__AuthorSubject__account_id(const jazz_types_AuthorSubject *s, jazz_types_AccountId *out) {
    switch (s->tag) {
    case JAZZ_AUTHOR_SYSTEM: return false;
    default:
        if (!s->interned->has_account) return false;
        *out = s->interned->account;
        return true;
    }
}

groove_ValueType jazz_types__ids__AuthorSubject__value_type(void) {
    return vt_record(jazz_types__ids__AuthorSubject__record_descriptor());
}

/* Valid nested author metadata paths. */
bool jazz_types__ids__AuthorSubject__metadata_path(jz_slice column, jz_slice *root, const char *const **path, u32 *path_len) {
    static const char *const P_ACCOUNT[] = { "account" };
    static const char *const P_IDENTITY[] = { "identity" };
    static const char *const P_ISSUER[] = { "identity", "issuer" };
    static const char *const P_SUBJECT[] = { "identity", "subject" };
    u32 dot = 0;
    while (dot < column.len && column.ptr[dot] != '.') dot++;
    if (dot == column.len) return false; /* split_once('.')? */
    jz_slice r = jz_slice_of(column.ptr, dot);
    jz_slice p = jz_slice_of(column.ptr + dot + 1, column.len - dot - 1);
    if (!(slice_eq_cstr(r, "$createdBy") || slice_eq_cstr(r, "$updatedBy"))) return false;
    if (slice_eq_cstr(p, "account")) { *path = P_ACCOUNT; *path_len = 1; }
    else if (slice_eq_cstr(p, "identity")) { *path = P_IDENTITY; *path_len = 1; }
    else if (slice_eq_cstr(p, "identity.issuer")) { *path = P_ISSUER; *path_len = 2; }
    else if (slice_eq_cstr(p, "identity.subject")) { *path = P_SUBJECT; *path_len = 2; }
    else return false;
    *root = r;
    return true;
}

const groove_ValueType *jazz_types__ids__AuthorSubject__metadata_type(jz_slice column) {
    static bool init; static groove_ValueType AUTHOR;
    if (!init) { AUTHOR = jazz_types__ids__RowAuthor__value_type(); init = true; }
    const groove_ValueType *ty = &AUTHOR;
    if (slice_eq_cstr(column, "$createdBy") || slice_eq_cstr(column, "$updatedBy")) return ty;
    jz_slice root; const char *const *path; u32 n;
    if (!jazz_types__ids__AuthorSubject__metadata_path(column, &root, &path, &n)) return NULL;
    for (u32 i = 0; i < n; i++) {
        if (ty->tag != GROOVE_VT_RECORD) return NULL;
        groove_RecordDescriptor record = *ty->u.record;
        u32 idx, nfields;
        if (!groove__records__RecordDescriptor__field_index(record, path[i], (u32)strlen(path[i]), &idx)) return NULL;
        const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(record, &nfields);
        if (idx >= nfields) return NULL;
        ty = &fields[idx].value_type;
    }
    return ty;
}

static groove_OwnedRecord jazz_types__ids__AuthorSubject__build_record(jazz_types_AuthorSubject s) {
    jz_str issuer, subject;
    jazz_types__ids__AuthorSubject__principal_parts(&s, &issuer, &subject);
    groove_RecordDescriptor descriptor = jazz_types__ids__AuthorSubject__record_descriptor();
    groove_Value account; memset(&account, 0, sizeof account);
    account.tag = GROOVE_V_NULLABLE;
    jazz_types_AccountId id;
    if (jazz_types__ids__AuthorSubject__account_id(&s, &id)) {
        account.u.nullable = jz_alloc(sizeof(groove_Value));
        *account.u.nullable = v_uuid(id);
    }
    return build_author_record(issuer, subject, descriptor, account, "author identity record", "author record");
}

const groove_OwnedRecord *jazz_types__ids__AuthorSubject__encoded_record(jazz_types_AuthorSubject s) {
    switch (s.tag) {
    case JAZZ_AUTHOR_SYSTEM: {
        static bool init; static groove_OwnedRecord RECORD;
        if (!init) { RECORD = jazz_types__ids__AuthorSubject__build_record(s); init = true; }
        return &RECORD;
    }
    default: {
        jazz_types_InternedAuthor *a = (jazz_types_InternedAuthor *)s.interned;
        if (!a->session_record_init) { a->session_record = jazz_types__ids__AuthorSubject__build_record(s); a->session_record_init = true; }
        return &a->session_record;
    }
    }
}

groove_Value jazz_types__ids__AuthorSubject__to_value(jazz_types_AuthorSubject s) {
    groove_OwnedRecord r;
    groove__records__OwnedRecord__Clone__clone(&r, jazz_types__ids__AuthorSubject__encoded_record(s));
    return v_record(r);
}

jz_err jazz_types__ids__AuthorSubject__from_value(groove_Value value, jazz_types_AuthorSubject *out) {
    static const char BAD[] = "invalid author record";
    jz_err result = JZ_OK;
    if (value.tag != GROOVE_V_RECORD) {
        groove__records__values__Value__drop(&value);
        return jazz_types__ids__author_subject_error(JAZZ_TYPES_ERR_INVALID_CANONICAL, BAD, sizeof BAD - 1);
    }
    groove_OwnedRecord *record = &value.u.record;
    groove_RecordDescriptor session_descriptor = jazz_types__ids__AuthorSubject__record_descriptor();
    groove_RecordDescriptor row_descriptor = jazz_types__ids__RowAuthor__record_descriptor();
    bool row_author = record->descriptor == row_descriptor;
    groove_BorrowedRecord borrowed = groove_OwnedRecord_borrowed(record);
    bool has_account = false; jz_uuid account = {{0}};
    jz_slice issuer, subject;
    bool bad;
    if (!row_author && record->descriptor != session_descriptor) goto bad;
    if (row_author) {
        jz_err e = groove__records__BorrowedRecord__get_uuid(&borrowed, 0, &account);
        if (e.code) { jz_err_drop(&e); goto bad; }
        has_account = true;
    } else {
        jz_err e = groove__records__BorrowedRecord__get_nullable_uuid(&borrowed, 0, &has_account, &account);
        if (e.code) { jz_err_drop(&e); goto bad; }
    }
    decode_identity(borrowed, &issuer, &subject, &bad);
    if (bad) goto bad;
    if (slice_eq_cstr(issuer, SYSTEM_ISSUER)) {
        if (!row_author && !has_account && slice_eq_cstr(subject, SYSTEM_SUBJECT)) {
            *out = jazz_types__ids__AuthorSubject__SYSTEM;
            goto done;
        }
        if (!has_account) { result = ASE(JAZZ_TYPES_ERR_INVALID_SYSTEM_ORIGIN); goto done; }
        if (!jz_uuid_eq(&account, &jazz_types__account_registry__SYSTEM_ACCOUNT_ID)) { result = ASE(JAZZ_TYPES_ERR_RESERVED_ACCOUNT); goto done; }
        jazz_types_NodeUuid node;
        result = jazz_types__ids__parse_system_origin(subject, &node);
        if (result.code) goto done;
        out->tag = JAZZ_AUTHOR_SYSTEM_AT;
        out->interned = jazz_types__ids__intern_author(true, account, issuer, subject);
        goto done;
    }
    if (has_account && jz_uuid_eq(&account, &jazz_types__account_registry__SYSTEM_ACCOUNT_ID)) { result = ASE(JAZZ_TYPES_ERR_RESERVED_ACCOUNT); goto done; }
    if (!jazz_types__identity__principal_is_nonempty(issuer)) { result = ASE(JAZZ_TYPES_ERR_MISSING_ISSUER); goto done; }
    if (!jazz_types__identity__principal_is_nonempty(subject)) { result = ASE(JAZZ_TYPES_ERR_MISSING_SUBJECT); goto done; }
    out->tag = JAZZ_AUTHOR_AUTHENTICATED;
    out->interned = jazz_types__ids__intern_author(has_account, account, issuer, subject);
    goto done;
bad:
    result = jazz_types__ids__author_subject_error(JAZZ_TYPES_ERR_INVALID_CANONICAL, BAD, sizeof BAD - 1);
done:
    groove__records__values__Value__drop(&value);
    return result;
}

jz_str jazz_types__ids__AuthorSubject__ownership_id(const jazz_types_AuthorSubject *s) {
    jazz_types_AccountId account;
    if (jazz_types__ids__AuthorSubject__account_id(s, &account)) {
        char buf[37];
        jz_uuid_to_string(&account, buf);
        return jz_str_from_slice(jz_slice_of(buf, 36));
    }
    return jz_str_from_slice(jazz_types__ids__AuthorSubject__canonical(s));
}

static bool canonical_matches(const jazz_types_AuthorSubject *a, jz_slice canonical) {
    return jz_slice_cmp(jazz_types__ids__AuthorSubject__canonical(a), canonical) == 0;
}

jz_err jazz_types__ids__AuthorSubject__from_canonical(jz_slice canonical, jazz_types_AuthorSubject *out) {
    if (slice_eq_cstr(canonical, SYSTEM_CANONICAL)) { *out = jazz_types__ids__AuthorSubject__SYSTEM; return JZ_OK; }
    CanonicalParts parts;
    JZ_TRY(jazz_types__ids__parse_canonical_parts(canonical, &parts));
    jz_err result = JZ_OK;
    jz_slice issuer = jz_str_slice(&parts.issuer), subject = jz_str_slice(&parts.subject);
    if (str_eq_cstr(&parts.issuer, SYSTEM_ISSUER)) {
        if (!parts.has_account) { result = ASE_S(JAZZ_TYPES_ERR_RESERVED_ISSUER, issuer); goto done; }
        if (!jz_uuid_eq(&parts.account, &jazz_types__account_registry__SYSTEM_ACCOUNT_ID)) { result = ASE(JAZZ_TYPES_ERR_RESERVED_ACCOUNT); goto done; }
        jazz_types_NodeUuid node;
        result = jazz_types__ids__parse_system_origin(subject, &node);
        if (result.code) goto done;
        jazz_types_AuthorSubject author = jazz_types__ids__AuthorSubject__system_at(node);
        if (!canonical_matches(&author, canonical)) { result = ASE(JAZZ_TYPES_ERR_NON_CANONICAL); goto done; }
        *out = author;
        goto done;
    }
    if (parts.has_account && jazz_types__account_registry__AccountId__is_system(parts.account)) { result = ASE(JAZZ_TYPES_ERR_RESERVED_ACCOUNT); goto done; }
    if (!jazz_types__identity__principal_is_nonempty(issuer)) { result = ASE(JAZZ_TYPES_ERR_MISSING_ISSUER); goto done; }
    if (!jazz_types__identity__principal_is_nonempty(subject)) { result = ASE(JAZZ_TYPES_ERR_MISSING_SUBJECT); goto done; }
    {
        jazz_types_AuthorSubject principal = jazz_types__ids__AuthorSubject__intern(issuer, subject);
        jazz_types_AuthorSubject author = parts.has_account ? jazz_types__ids__AuthorSubject__with_account(principal, parts.account) : principal;
        if (!canonical_matches(&author, canonical)) { result = ASE(JAZZ_TYPES_ERR_NON_CANONICAL); goto done; }
        *out = author;
    }
done:
    canonical_parts_drop(&parts);
    return result;
}

jz_err jazz_types__ids__AuthorSubject__from_untrusted_canonical(jz_slice canonical, jazz_types_AuthorSubject *out) {
    CanonicalParts parts;
    JZ_TRY(jazz_types__ids__parse_canonical_parts(canonical, &parts));
    jazz_types_AuthorSubject principal;
    jz_err result = jazz_types__ids__AuthorSubject__authenticated(jz_str_slice(&parts.issuer), jz_str_slice(&parts.subject), &principal);
    if (result.code) goto done;
    if (parts.has_account && jazz_types__account_registry__AccountId__is_system(parts.account)) { result = ASE(JAZZ_TYPES_ERR_RESERVED_ACCOUNT); goto done; }
    {
        jazz_types_AuthorSubject author = parts.has_account ? jazz_types__ids__AuthorSubject__with_account(principal, parts.account) : principal;
        if (!canonical_matches(&author, canonical)) { result = ASE(JAZZ_TYPES_ERR_NON_CANONICAL); goto done; }
        *out = author;
    }
done:
    canonical_parts_drop(&parts);
    return result;
}

static jz_err serde_custom_from(jz_err e) {
    /* serde::de::Error::custom(error) keeps the Display text */
    jz_err r = jz_err_fmt(JAZZ_TYPES_ERR_SERDE_CUSTOM, "%.*s", e.msg ? (int)e.msg->len : 0, e.msg ? e.msg->ptr : "");
    jz_err_drop(&e);
    return r;
}

jz_err jazz_types__ids__AuthorSubject__deserialize_untrusted(jz_pc_r *r, jazz_types_AuthorSubject *out) {
    jz_str canonical;
    JZ_TRY(jz_pc_r_str(r, &canonical));
    jz_err e = jazz_types__ids__AuthorSubject__from_untrusted_canonical(jz_str_slice(&canonical), out);
    jz_str_drop(&canonical);
    return e.code ? serde_custom_from(e) : JZ_OK;
}

bool jazz_types__ids__AuthorSubject__is_anonymous(const jazz_types_AuthorSubject *s) {
    return s->tag == JAZZ_AUTHOR_AUTHENTICATED && str_eq_cstr(&s->interned->issuer, jazz_types__ids__AuthorSubject__ANONYMOUS_ISSUER);
}

void jazz_types__ids__AuthorSubject__Serialize__serialize(jz_pc_w *w, const jazz_types_AuthorSubject *s) {
    jz_slice c = jazz_types__ids__AuthorSubject__canonical(s);
    jz_pc_bytes(w, c.ptr, c.len); /* serialize_str: varint length + utf8 */
}

jz_err jazz_types__ids__AuthorSubject__Deserialize__deserialize(jz_pc_r *r, jazz_types_AuthorSubject *out) {
    jz_str canonical;
    JZ_TRY(jz_pc_r_str(r, &canonical));
    jz_err e = jazz_types__ids__AuthorSubject__from_canonical(jz_str_slice(&canonical), out);
    jz_str_drop(&canonical);
    return e.code ? serde_custom_from(e) : JZ_OK;
}

/* ---------------- RowAuthor ---------------- */
jazz_types_RowAuthor jazz_types__ids__RowAuthor__system_at(jazz_types_NodeUuid node) {
    jazz_types_AuthorSubject s = jazz_types__ids__AuthorSubject__system_at(node);
    if (s.tag != JAZZ_AUTHOR_SYSTEM_AT) jz_panic("system attribution has a dedicated author variant");
    jazz_types_RowAuthor r = { JAZZ_ROW_AUTHOR_SYSTEM_AT, s.interned };
    return r;
}

static jz_err jazz_types__ids__RowAuthor__from_authenticated(const jazz_types_InternedAuthor *author, jazz_types_RowAuthor *out) {
    if (str_eq_cstr(&author->issuer, SYSTEM_ISSUER)) return ASE_S(JAZZ_TYPES_ERR_RESERVED_ISSUER, author->issuer);
    if (author->has_account) {
        if (!jazz_types__account_registry__AccountId__is_system(author->account)) {
            out->tag = JAZZ_ROW_AUTHOR_ACCOUNT;
            out->interned = author;
            return JZ_OK;
        }
        return ASE(JAZZ_TYPES_ERR_RESERVED_ACCOUNT);
    }
    return ASE(JAZZ_TYPES_ERR_MISSING_ACCOUNT);
}

static jz_err jazz_types__ids__RowAuthor__from_system_at(const jazz_types_InternedAuthor *author, jazz_types_RowAuthor *out) {
    if (!author->has_account || !jz_uuid_eq(&author->account, &jazz_types__account_registry__SYSTEM_ACCOUNT_ID))
        return ASE(JAZZ_TYPES_ERR_RESERVED_ACCOUNT);
    if (!str_eq_cstr(&author->issuer, SYSTEM_ISSUER)) return ASE(JAZZ_TYPES_ERR_INVALID_SYSTEM_ORIGIN);
    out->tag = JAZZ_ROW_AUTHOR_SYSTEM_AT;
    out->interned = author;
    return JZ_OK;
}

jz_err jazz_types__ids__RowAuthor__from_session(jazz_types_AuthorSubject s, jazz_types_NodeUuid node, jazz_types_RowAuthor *out) {
    switch (s.tag) {
    case JAZZ_AUTHOR_SYSTEM: *out = jazz_types__ids__RowAuthor__system_at(node); return JZ_OK;
    case JAZZ_AUTHOR_SYSTEM_AT: return ASE(JAZZ_TYPES_ERR_INVALID_SYSTEM_ORIGIN);
    default: return jazz_types__ids__RowAuthor__from_authenticated(s.interned, out);
    }
}

jz_err jazz_types__ids__RowAuthor__from_persisted_subject(jazz_types_AuthorSubject s, jazz_types_RowAuthor *out) {
    switch (s.tag) {
    case JAZZ_AUTHOR_SYSTEM: return ASE(JAZZ_TYPES_ERR_INVALID_SYSTEM_ORIGIN);
    case JAZZ_AUTHOR_SYSTEM_AT: return jazz_types__ids__RowAuthor__from_system_at(s.interned, out);
    default: return jazz_types__ids__RowAuthor__from_authenticated(s.interned, out);
    }
}

jazz_types_AccountId jazz_types__ids__RowAuthor__account_id(const jazz_types_RowAuthor *a) {
    if (!a->interned->has_account) jz_panic("row author construction requires an account");
    return a->interned->account;
}

void jazz_types__ids__RowAuthor__principal_parts(const jazz_types_RowAuthor *a, jz_str *issuer, jz_str *subject) {
    *issuer = jz_str_clone(&a->interned->issuer);
    *subject = jz_str_clone(&a->interned->subject);
}

bool jazz_types__ids__RowAuthor__system_origin(const jazz_types_RowAuthor *a, jazz_types_NodeUuid *out) {
    if (a->tag != JAZZ_ROW_AUTHOR_SYSTEM_AT) return false;
    jz_err e = jz_uuid_parse(jz_str_slice(&a->interned->subject), out);
    if (e.code) { jz_err_drop(&e); return false; }
    return true;
}

jazz_types_AuthorSubject jazz_types__ids__RowAuthor__as_author_subject(jazz_types_RowAuthor a) {
    jazz_types_AuthorSubject s;
    s.tag = a.tag == JAZZ_ROW_AUTHOR_SYSTEM_AT ? JAZZ_AUTHOR_SYSTEM_AT : JAZZ_AUTHOR_AUTHENTICATED;
    s.interned = a.interned;
    return s;
}

groove_ValueType jazz_types__ids__RowAuthor__value_type(void) {
    return vt_record(jazz_types__ids__RowAuthor__record_descriptor());
}

static groove_OwnedRecord jazz_types__ids__RowAuthor__build_record(jazz_types_RowAuthor a) {
    jz_str issuer, subject;
    jazz_types__ids__RowAuthor__principal_parts(&a, &issuer, &subject);
    return build_author_record(issuer, subject, jazz_types__ids__RowAuthor__record_descriptor(),
                               v_uuid(jazz_types__ids__RowAuthor__account_id(&a)), "row author identity record", "row author record");
}

const groove_OwnedRecord *jazz_types__ids__RowAuthor__encoded_record(jazz_types_RowAuthor a) {
    jazz_types_InternedAuthor *author = (jazz_types_InternedAuthor *)a.interned;
    if (!author->row_record_init) { author->row_record = jazz_types__ids__RowAuthor__build_record(a); author->row_record_init = true; }
    return &author->row_record;
}

groove_Value jazz_types__ids__RowAuthor__to_value(jazz_types_RowAuthor a) {
    groove_OwnedRecord r;
    groove__records__OwnedRecord__Clone__clone(&r, jazz_types__ids__RowAuthor__encoded_record(a));
    return v_record(r);
}

jz_err jazz_types__ids__RowAuthor__from_record(groove_BorrowedRecord record, jazz_types_RowAuthor *out) {
    static const char BAD[] = "invalid row author record";
    groove_RecordDescriptor expected = jazz_types__ids__RowAuthor__record_descriptor();
    if (record.descriptor != expected) goto bad;
    jz_uuid account;
    jz_err e = groove__records__BorrowedRecord__get_uuid(&record, 0, &account);
    if (e.code) { jz_err_drop(&e); goto bad; }
    jz_slice issuer, subject;
    bool is_bad;
    decode_identity(record, &issuer, &subject, &is_bad);
    if (is_bad) goto bad;
    if (!jazz_types__identity__principal_is_nonempty(issuer)) return ASE(JAZZ_TYPES_ERR_MISSING_ISSUER);
    if (!jazz_types__identity__principal_is_nonempty(subject)) return ASE(JAZZ_TYPES_ERR_MISSING_SUBJECT);
    if (slice_eq_cstr(issuer, SYSTEM_ISSUER)) {
        if (!jz_uuid_eq(&account, &jazz_types__account_registry__SYSTEM_ACCOUNT_ID)) return ASE(JAZZ_TYPES_ERR_RESERVED_ACCOUNT);
        jazz_types_NodeUuid node;
        JZ_TRY(jazz_types__ids__parse_system_origin(subject, &node));
        out->tag = JAZZ_ROW_AUTHOR_SYSTEM_AT;
        out->interned = jazz_types__ids__intern_author(true, account, issuer, subject);
        return JZ_OK;
    }
    if (jazz_types__account_registry__AccountId__is_system(account)) return ASE(JAZZ_TYPES_ERR_RESERVED_ACCOUNT);
    out->tag = JAZZ_ROW_AUTHOR_ACCOUNT;
    out->interned = jazz_types__ids__intern_author(true, account, issuer, subject);
    return JZ_OK;
bad:
    return jazz_types__ids__author_subject_error(JAZZ_TYPES_ERR_INVALID_CANONICAL, BAD, sizeof BAD - 1);
}

jz_err jazz_types__ids__RowAuthor__from_value(groove_Value value, jazz_types_RowAuthor *out) {
    static const char BAD[] = "invalid row author record";
    if (value.tag != GROOVE_V_RECORD) {
        groove__records__values__Value__drop(&value);
        return jazz_types__ids__author_subject_error(JAZZ_TYPES_ERR_INVALID_CANONICAL, BAD, sizeof BAD - 1);
    }
    jz_err e = jazz_types__ids__RowAuthor__from_record(groove_OwnedRecord_borrowed(&value.u.record), out);
    groove__records__values__Value__drop(&value);
    return e;
}

jz_slice jazz_types__ids__RowAuthor__canonical(const jazz_types_RowAuthor *a) { return jz_str_slice(&a->interned->canonical); }

void jazz_types__ids__RowAuthor__Debug__fmt(jz_str *out, const jazz_types_RowAuthor *a) {
    jz_str_push_cstr(out, a->tag == JAZZ_ROW_AUTHOR_SYSTEM_AT ? "RowAuthor(SystemAt(" : "RowAuthor(Account(");
    jazz_types__ids__InternedAuthor__Debug__fmt(out, a->interned);
    jz_str_push_cstr(out, "))");
}
