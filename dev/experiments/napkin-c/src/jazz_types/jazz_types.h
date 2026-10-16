/* jazz_types.h: public types of crate jazz_types (crates/jazz/layers/types/src). */
#ifndef JAZZ_TYPES_H
#define JAZZ_TYPES_H
#include "jz_rt.h"
#include "jazz_protocol/_ext_protocol-types.h"

/* ---------------- error codes (crate-local enum space) ---------------- */
enum {
    /* ids::AuthorSubjectError */
    JAZZ_TYPES_ERR_MISSING_ISSUER = 0x0100,
    JAZZ_TYPES_ERR_MISSING_SUBJECT,
    JAZZ_TYPES_ERR_RESERVED_ISSUER,
    JAZZ_TYPES_ERR_INVALID_CANONICAL,
    JAZZ_TYPES_ERR_NON_CANONICAL,
    JAZZ_TYPES_ERR_MISSING_ACCOUNT,
    JAZZ_TYPES_ERR_RESERVED_ACCOUNT,
    JAZZ_TYPES_ERR_INVALID_SYSTEM_ORIGIN,
    /* time */
    JAZZ_TYPES_ERR_HLC_OVERFLOW,
    JAZZ_TYPES_ERR_HLC_PHYSICAL_OUT_OF_RANGE,
    /* identity (Result<_, String>) */
    JAZZ_TYPES_ERR_IDENTITY,
    /* postcard_exact: postcard::Error::DeserializeBadEncoding */
    JAZZ_TYPES_ERR_PC_BAD_ENCODING,
    /* app_id: uuid::Error */
    JAZZ_TYPES_ERR_UUID,
    /* serde custom error */
    JAZZ_TYPES_ERR_SERDE_CUSTOM,
};

/* ---------------- ids ---------------- */
typedef jz_uuid jazz_types_NodeUuid;
typedef u64 jazz_types_NodeAlias;
typedef jz_uuid jazz_types_SchemaVersionId;
typedef u64 jazz_types_SchemaVersionAlias;
typedef u64 jazz_types_PhysicalTableId;
typedef u64 jazz_types_PhysicalColumnId;
typedef jz_uuid jazz_types_GlobalPhysicalTableId;
typedef jz_uuid jazz_types_GlobalPhysicalColumnId;
typedef jz_uuid jazz_types_GlobalPhysicalEnumVariantId;
typedef jz_uuid jazz_types_MigrationLensId;
typedef jz_uuid jazz_types_SchemaLineagePublicationId;
typedef jz_uuid jazz_types_SchemaFamilyId;
typedef jz_uuid jazz_types_RowUuid;
typedef jz_uuid jazz_types_AccountId;

typedef struct jazz_types_InternedAuthor {
    bool has_account; jazz_types_AccountId account;
    jz_str issuer;
    jz_str subject;
    jz_str canonical;
    /* OnceLock<OwnedRecord> caches */
    bool row_record_init; groove_OwnedRecord row_record;
    bool session_record_init; groove_OwnedRecord session_record;
} jazz_types_InternedAuthor;

enum { JAZZ_AUTHOR_SYSTEM = 0, JAZZ_AUTHOR_SYSTEM_AT = 1, JAZZ_AUTHOR_AUTHENTICATED = 2 };
typedef struct jazz_types_AuthorSubject { u32 tag; const jazz_types_InternedAuthor *interned; } jazz_types_AuthorSubject;
enum { JAZZ_ROW_AUTHOR_SYSTEM_AT = 0, JAZZ_ROW_AUTHOR_ACCOUNT = 1 };
typedef struct jazz_types_RowAuthor { u32 tag; const jazz_types_InternedAuthor *interned; } jazz_types_RowAuthor;

extern const char jazz_types__ids__AuthorSubject__SYSTEM_ISSUER[];
extern const char jazz_types__ids__AuthorSubject__SYSTEM_SUBJECT[];
extern const char jazz_types__ids__AuthorSubject__SYSTEM_CANONICAL[];
extern const char jazz_types__ids__AuthorSubject__LOCAL_FIRST_ISSUER[];
extern const char jazz_types__ids__AuthorSubject__STATIC_BEARER_ISSUER[];
extern const char jazz_types__ids__AuthorSubject__ANONYMOUS_ISSUER[];
extern const jazz_types_AuthorSubject jazz_types__ids__AuthorSubject__SYSTEM;

void jazz_types__ids__AuthorSubjectError__Display__fmt(jz_str *out, u32 code, const jz_str *detail);
jz_err jazz_types__ids__author_subject_error(u32 code, const char *detail, u32 detail_len);

jz_slice jazz_types__ids__AuthorSubject__canonical(const jazz_types_AuthorSubject *s);
bool jazz_types_AuthorSubject_eq(const jazz_types_AuthorSubject *a, const jazz_types_AuthorSubject *b);
int jazz_types__ids__AuthorSubject__Ord__cmp(const jazz_types_AuthorSubject *a, const jazz_types_AuthorSubject *b);
int jazz_types__ids__AuthorSubject__cmp_v(const void *a, const void *b, void *ctx);
void jazz_types__ids__AuthorSubject__Debug__fmt(jz_str *out, const jazz_types_AuthorSubject *s);
void jazz_types__ids__AuthorSubject__Serialize__serialize(jz_pc_w *w, const jazz_types_AuthorSubject *s);
jz_err jazz_types__ids__AuthorSubject__Deserialize__deserialize(jz_pc_r *r, jazz_types_AuthorSubject *out);
jz_err jazz_types__ids__AuthorSubject__deserialize_untrusted(jz_pc_r *r, jazz_types_AuthorSubject *out);
jz_err jazz_types__ids__AuthorSubject__authenticated(jz_slice issuer, jz_slice subject, jazz_types_AuthorSubject *out);
jz_err jazz_types__ids__AuthorSubject__reserved(jz_slice issuer, jz_slice subject, jazz_types_AuthorSubject *out);
jazz_types_AuthorSubject jazz_types__ids__AuthorSubject__system_at(jazz_types_NodeUuid node);
jazz_types_AuthorSubject jazz_types__ids__AuthorSubject__with_account(jazz_types_AuthorSubject s, jazz_types_AccountId account);
void jazz_types__ids__AuthorSubject__principal_parts(const jazz_types_AuthorSubject *s, jz_str *issuer, jz_str *subject);
bool jazz_types__ids__AuthorSubject__account_id(const jazz_types_AuthorSubject *s, jazz_types_AccountId *out);
groove_ValueType jazz_types__ids__AuthorSubject__value_type(void);
bool jazz_types__ids__AuthorSubject__metadata_path(jz_slice column, jz_slice *root, const char *const **path, u32 *path_len);
const groove_ValueType *jazz_types__ids__AuthorSubject__metadata_type(jz_slice column);
groove_Value jazz_types__ids__AuthorSubject__to_value(jazz_types_AuthorSubject s);
const groove_OwnedRecord *jazz_types__ids__AuthorSubject__encoded_record(jazz_types_AuthorSubject s);
jz_err jazz_types__ids__AuthorSubject__from_value(groove_Value value, jazz_types_AuthorSubject *out);
jz_str jazz_types__ids__AuthorSubject__ownership_id(const jazz_types_AuthorSubject *s);
jz_err jazz_types__ids__AuthorSubject__from_canonical(jz_slice canonical, jazz_types_AuthorSubject *out);
jz_err jazz_types__ids__AuthorSubject__from_untrusted_canonical(jz_slice canonical, jazz_types_AuthorSubject *out);
bool jazz_types__ids__AuthorSubject__is_anonymous(const jazz_types_AuthorSubject *s);

jazz_types_RowAuthor jazz_types__ids__RowAuthor__system_at(jazz_types_NodeUuid node);
jz_err jazz_types__ids__RowAuthor__from_session(jazz_types_AuthorSubject s, jazz_types_NodeUuid node, jazz_types_RowAuthor *out);
jz_err jazz_types__ids__RowAuthor__from_persisted_subject(jazz_types_AuthorSubject s, jazz_types_RowAuthor *out);
jazz_types_AccountId jazz_types__ids__RowAuthor__account_id(const jazz_types_RowAuthor *a);
void jazz_types__ids__RowAuthor__principal_parts(const jazz_types_RowAuthor *a, jz_str *issuer, jz_str *subject);
bool jazz_types__ids__RowAuthor__system_origin(const jazz_types_RowAuthor *a, jazz_types_NodeUuid *out);
jazz_types_AuthorSubject jazz_types__ids__RowAuthor__as_author_subject(jazz_types_RowAuthor a);
groove_ValueType jazz_types__ids__RowAuthor__value_type(void);
groove_Value jazz_types__ids__RowAuthor__to_value(jazz_types_RowAuthor a);
const groove_OwnedRecord *jazz_types__ids__RowAuthor__encoded_record(jazz_types_RowAuthor a);
jz_err jazz_types__ids__RowAuthor__from_value(groove_Value value, jazz_types_RowAuthor *out);
jz_err jazz_types__ids__RowAuthor__from_record(groove_BorrowedRecord record, jazz_types_RowAuthor *out);
jz_slice jazz_types__ids__RowAuthor__canonical(const jazz_types_RowAuthor *a);
jz_bytes jazz_types__ids__SchemaFamilyId__to_bytes(jazz_types_SchemaFamilyId id);
void jazz_types__ids__uuid_id__Debug__fmt(jz_str *out, const char *name, const jz_uuid *u);
void jazz_types__ids__u64_id__Debug__fmt(jz_str *out, const char *name, u64 v);

/* ---------------- account_registry (client-relevant part) ---------------- */
extern const jazz_types_AccountId jazz_types__account_registry__SYSTEM_ACCOUNT_ID;
bool jazz_types__account_registry__AccountId__is_system(jazz_types_AccountId a);
jazz_types_AccountId jazz_types__account_registry__local_first_account_id(jz_uuid app, jz_slice subject);

/* ---------------- time ---------------- */
typedef u64 jazz_types_TxTime;
typedef u64 jazz_types_GlobalTime;
#define JAZZ_HLC_PHYSICAL_BITS 46u
#define JAZZ_HLC_COUNTER_BITS (64u - JAZZ_HLC_PHYSICAL_BITS)
#define JAZZ_HLC_MAX_PHYSICAL_MS ((((u64)1) << JAZZ_HLC_PHYSICAL_BITS) - 1)
#define JAZZ_HLC_MAX_LOGICAL_COUNTER ((u32)((1u << JAZZ_HLC_COUNTER_BITS) - 1))
bool jazz_types__time__GlobalTime__new(u64 physical_ms, u32 counter, jazz_types_GlobalTime *out);
u64 jazz_types__time__GlobalTime__physical_ms(jazz_types_GlobalTime t);
u32 jazz_types__time__GlobalTime__counter(jazz_types_GlobalTime t);
jz_err jazz_types__time__GlobalTime__tick(jazz_types_GlobalTime reg, u64 now_ms, jazz_types_GlobalTime *out);
u64 jazz_types__time__GlobalTime__authority_now_ms(u64 now_ms, u64 fallback_ms);
jazz_types_TxTime jazz_types__time__TxTime__new(u64 physical_ms, u32 counter);
jz_err jazz_types__time__TxTime__from_physical_ms(u64 ms, jazz_types_TxTime *out);
u64 jazz_types__time__TxTime__physical_ms(jazz_types_TxTime t);
u32 jazz_types__time__TxTime__counter(jazz_types_TxTime t);
jz_err jazz_types__time__TxTime__tick_after(jazz_types_TxTime t, jazz_types_TxTime *out);
jz_err jazz_types__time__TxTime__tick(jazz_types_TxTime reg, u64 now_ms, jazz_types_TxTime *out);
jazz_types_TxTime jazz_types__time__TxTime__From_u64__from(u64 ms);
void jazz_types__time__TxTime__Serialize__serialize(jz_pc_w *w, jazz_types_TxTime t);
jz_err jazz_types__time__TxTime__Deserialize__deserialize(jz_pc_r *r, jazz_types_TxTime *out);
void jazz_types__time__GlobalTime__Serialize__serialize(jz_pc_w *w, jazz_types_GlobalTime t);
jz_err jazz_types__time__GlobalTime__Deserialize__deserialize(jz_pc_r *r, jazz_types_GlobalTime *out);
void jazz_types__time__TxTime__Debug__fmt(jz_str *out, jazz_types_TxTime t);
void jazz_types__time__GlobalTime__Debug__fmt(jz_str *out, jazz_types_GlobalTime t);

/* ---------------- object ---------------- */
typedef const jz_uuid *jazz_types_ObjectId; /* Intern<Uuid> */
jazz_types_ObjectId jazz_types__object__ObjectId__new(void);
jazz_types_ObjectId jazz_types__object__ObjectId__from_uuid(const jz_uuid *u);
int jazz_types__object__ObjectId__Ord__cmp(jazz_types_ObjectId a, jazz_types_ObjectId b);
void jazz_types__object__ObjectId__Debug__fmt(jz_str *out, jazz_types_ObjectId id);
void jazz_types__object__ObjectId__Display__fmt(jz_str *out, jazz_types_ObjectId id);
void jazz_types__object__ObjectId__Serialize__serialize(jz_pc_w *w, jazz_types_ObjectId id);
jz_err jazz_types__object__ObjectId__Deserialize__deserialize(jz_pc_r *r, jazz_types_ObjectId *out);
typedef struct jazz_types_UnionArm { u32 position; jz_str label; } jazz_types_UnionArm;
typedef struct jazz_types_OutputOccurrenceId {
    jazz_types_ObjectId root;
    JZ_VEC(jazz_types_ObjectId) joined;       /* SmallVec<[ObjectId; 2]> */
    JZ_VEC(jazz_types_UnionArm) union_arms;   /* SmallVec<[(usize, String); 1]>, serde(skip) */
} jazz_types_OutputOccurrenceId;
void jazz_types__object__OutputOccurrenceId__single_source(jazz_types_OutputOccurrenceId *out, jazz_types_ObjectId root);
void jazz_types__object__OutputOccurrenceId__new(jazz_types_OutputOccurrenceId *out, jazz_types_ObjectId root, const jazz_types_ObjectId *joined, u32 n);
bool jazz_types__object__OutputOccurrenceId__with_union_arms(jazz_types_OutputOccurrenceId *out, jazz_types_ObjectId root, const jazz_types_ObjectId *joined, u32 n, jazz_types_UnionArm *arms_moved, u32 narms);
bool jazz_types__object__OutputOccurrenceId__has_typed_discriminators(const jazz_types_OutputOccurrenceId *o);
const jazz_types_UnionArm *jazz_types__object__OutputOccurrenceId__union_arms(const jazz_types_OutputOccurrenceId *o, u32 *n);
const jazz_types_ObjectId *jazz_types__object__OutputOccurrenceId__joined_sources(const jazz_types_OutputOccurrenceId *o, u32 *n);
jz_bytes jazz_types__object__OutputOccurrenceId__canonical_bytes(const jazz_types_OutputOccurrenceId *o);
void jazz_types__object__OutputOccurrenceId__Clone__clone(jazz_types_OutputOccurrenceId *dst, const jazz_types_OutputOccurrenceId *src);
void jazz_types__object__OutputOccurrenceId__drop(jazz_types_OutputOccurrenceId *o);
bool jazz_types__object__OutputOccurrenceId__PartialEq__eq(const jazz_types_OutputOccurrenceId *a, const jazz_types_OutputOccurrenceId *b);
int jazz_types__object__OutputOccurrenceId__Ord__cmp(const jazz_types_OutputOccurrenceId *a, const jazz_types_OutputOccurrenceId *b);
void jazz_types__object__OutputOccurrenceId__Debug__fmt(jz_str *out, const jazz_types_OutputOccurrenceId *o);
void jazz_types__object__OutputOccurrenceId__Serialize__serialize(jz_pc_w *w, const jazz_types_OutputOccurrenceId *o);
jz_err jazz_types__object__OutputOccurrenceId__Deserialize__deserialize(jz_pc_r *r, jazz_types_OutputOccurrenceId *out);
typedef struct jazz_types_ResultKey { jazz_types_OutputOccurrenceId occ; } jazz_types_ResultKey;
void jazz_types__object__ResultKey__Serialize__serialize(jz_pc_w *w, const jazz_types_ResultKey *k);
jz_err jazz_types__object__ResultKey__Deserialize__deserialize(jz_pc_r *r, jazz_types_ResultKey *out);
bool jazz_types__object__ResultKey__row_id(const jazz_types_ResultKey *k, jazz_types_ObjectId *out);
typedef const jz_str *jazz_types_BranchName; /* Intern<String> */
jazz_types_BranchName jazz_types__object__BranchName__new(jz_str name);
void jazz_types__object__BranchName__Serialize__serialize(jz_pc_w *w, jazz_types_BranchName n);
jz_err jazz_types__object__BranchName__Deserialize__deserialize(jz_pc_r *r, jazz_types_BranchName *out);

/* ---------------- app_id ---------------- */
typedef jazz_types_ObjectId jazz_types_AppId;
jazz_types_AppId jazz_types__app_id__AppId__random(void);
jazz_types_AppId jazz_types__app_id__AppId__from_name(jz_slice name);
jz_err jazz_types__app_id__AppId__from_string(jz_slice s, jazz_types_AppId *out);
void jazz_types__app_id__AppId__Display__fmt(jz_str *out, jazz_types_AppId id);

/* ---------------- identity ---------------- */
extern const char jazz_types__identity__LOCAL_FIRST_ISSUER[];
extern const char jazz_types__identity__ANONYMOUS_ISSUER[];
typedef struct jazz_types_VerifiedSelfSigned { jz_str user_id; u8 public_key_bytes[32]; const char *issuer; } jazz_types_VerifiedSelfSigned;
bool jazz_types__identity__principal_is_nonempty(jz_slice principal);
jz_err jazz_types__identity__verify_jazz_self_signed_proof_with_max_ttl_at(jz_slice token, jz_slice expected_audience, u64 max_ttl_seconds, u64 now_seconds, jazz_types_VerifiedSelfSigned *out);
jz_err jazz_types__identity__verify_jazz_self_signed_proof_at(jz_slice token, jz_slice expected_audience, u64 now_seconds, jazz_types_VerifiedSelfSigned *out);
jz_err jazz_types__identity__verify_client_runtime_author_at(jz_slice token, jz_slice expected_audience, jz_slice claimed_author, u64 now_seconds, jazz_types_AuthorSubject *out);

/* ---------------- debug_env ---------------- */
bool jazz_types__debug_env__covered_input_trace(void);
bool jazz_types__debug_env__rehydrate_trace(void);
bool jazz_types__debug_env__force_singleton_version_carriers(void);

/* ---------------- postcard_exact ---------------- */
/* Serialize + Deserialize vtable for the one C copy of decode_postcard_exact<T>. */
typedef struct jazz_types_PcCodec {
    jz_err (*decode)(jz_pc_r *r, void *out);
    void (*encode)(jz_pc_w *w, const void *value);
    void (*drop)(void *value);
} jazz_types_PcCodec;
jz_err jazz_types__postcard_exact__decode_postcard_exact(const jazz_types_PcCodec *codec, jz_slice bytes, void *out);
bool jazz_types__postcard_exact__encodes_exactly(const jazz_types_PcCodec *codec, const void *value, jz_slice expected);

/* ---------------- local_executor ---------------- */
/* A ready-immediate future: poll returns true when *out is ready. */
typedef struct jazz_types_Future { bool (*poll)(void *self, const jz_waker *cx, void *out); void *self; } jazz_types_Future;
void jazz_types__local_executor__block_on(jazz_types_Future fut, void *out);

#endif
