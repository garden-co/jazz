/* mod.h: the engine's public vocabulary (crates/jazz/layers/engine/src/query_engine/
 * {mod,fields,input,output,policy,publication,read,resolver,schemas,binding_values}.rs).
 * One header for the whole public surface, as the Rust `pub use x::*` re-exports
 * make all of it one namespace. */
#ifndef JAZZ_ENGINE_QUERY_ENGINE_MOD_H
#define JAZZ_ENGINE_QUERY_ENGINE_MOD_H
#include "jz_rt.h"
#include "../_ext_engine.h"

#define JZ_S(str) jz_str_slice(str)
#define JZ_C(cstr) jz_slice_of((cstr), (u32)strlen(cstr))

/* ---------------------------------------------------------------- shared helpers (mod.c) */
void jazz_engine__str_drop_v(void *s);
void jazz_engine__str_clone_v(void *dst, const void *src);
void jazz_engine__strvec_clone(jazz_engine_StrVec *dst, const jazz_engine_StrVec *src);
void jazz_engine__strvec_drop(jazz_engine_StrVec *v);
void jazz_engine__strvec_drop_v(void *v);
bool jazz_engine__strvec_eq(const jazz_engine_StrVec *a, const jazz_engine_StrVec *b);
int jazz_engine__strvec_cmp(const jazz_engine_StrVec *a, const jazz_engine_StrVec *b);
bool jazz_engine__strvec_contains(const jazz_engine_StrVec *v, const jz_str *s);
bool jazz_engine__strvec_contains_cstr(const jazz_engine_StrVec *v, const char *s);
void jazz_engine__strvec_push_cstr(jazz_engine_StrVec *v, const char *s);
void jazz_engine__strvec_push_clone(jazz_engine_StrVec *v, const jz_str *s);
jazz_engine_StrVec jazz_engine__strvec_one(jz_str s);
jazz_engine_StrVec jazz_engine__strvec_from_set(const jz_btree *set);
bool jazz_engine__str_starts_with(const jz_str *s, const char *prefix);
bool jazz_engine__str_eq_slice(const jz_str *s, jz_slice b);
jz_str jazz_engine__str_concat(const char *a, const jz_str *b);
void jazz_engine__strset_insert_cstr(jz_btree *set, const char *s);
void jazz_engine__strset_insert_clone(jz_btree *set, const jz_str *s);
bool jazz_engine__strset_contains_cstr(const jz_btree *set, const char *s);
void jazz_engine__str_drop_unused(jz_str s);
extern const jz_btree_desc jazz_engine__STRSET_DESC;        /* BTreeSet<String> */
extern const jz_btree_desc jazz_engine__STR_STR_MAP_DESC;   /* BTreeMap<String,String> */
extern const jz_btree_desc jazz_engine__STR_VT_MAP_DESC;    /* BTreeMap<String,ColumnType> */
extern const jz_btree_desc jazz_engine__STR_VALUE_MAP_DESC; /* BTreeMap<String,Value> */
extern const jz_btree_desc jazz_engine__STR_U32_MAP_DESC;   /* BTreeMap<String,usize> */
extern const jz_hmap_desc jazz_engine__STRSET_HDESC;        /* HashSet<String> */
extern const jz_hmap_desc jazz_engine__PTRSET_HDESC;        /* HashSet<*const T> */
extern const jz_hmap_desc jazz_engine__PTR_U32_HDESC;       /* HashMap<*const T, usize> */
extern const jz_hmap_desc jazz_engine__STR_U32_HDESC;       /* HashMap<String, usize> */
void jazz_engine__strset_clone(jz_btree *dst, const jz_btree *src);
void jazz_engine__vt_drop_v(void *t);
void jazz_engine__vt_clone_v(void *dst, const void *src);
bool jazz_engine__vt_eq_v(const void *a, const void *b);
void jazz_engine__strvtmap_clone(jz_btree *dst, const jz_btree *src);

/* Debug (core::fmt) helpers shared by every derived Debug impl of the unit */
void jazz_engine__fmt__str(jz_str *out, const jz_str *s);
void jazz_engine__fmt__cstr(jz_str *out, const char *s);
void jazz_engine__fmt__struct_begin(jz_str *out, const char *name);
void jazz_engine__fmt__field(jz_str *out, const char *name, bool first);
void jazz_engine__fmt__struct_end(jz_str *out);
void jazz_engine__fmt__tuple_begin(jz_str *out, const char *name);
void jazz_engine__fmt__tuple_end(jz_str *out);
void jazz_engine__fmt__sep(jz_str *out, bool first);
void jazz_engine__fmt__uuid(jz_str *out, const jz_uuid *u);
void jazz_engine__fmt__bool(jz_str *out, bool b);
void jazz_engine__fmt__u64(jz_str *out, u64 v);
void jazz_engine__fmt__option_none(jz_str *out);
void jazz_engine__fmt__strvec(jz_str *out, const jazz_engine_StrVec *v);
void jazz_engine__fmt__strset(jz_str *out, const jz_btree *set);
typedef void (*jazz_engine_fmt_fn)(jz_str *out, const void *v);
void jazz_engine__fmt__list(jz_str *out, const void *items, u32 n, usize elem, jazz_engine_fmt_fn f);
void jazz_engine__fmt__set(jz_str *out, const jz_btree *set, jazz_engine_fmt_fn f);
void jazz_engine__fmt__map(jz_str *out, const jz_btree *map, jazz_engine_fmt_fn kf, jazz_engine_fmt_fn vf);
void jazz_engine__fmt__str_v(jz_str *out, const void *s);
void jazz_engine__fmt__vt_v(jz_str *out, const void *t);
void jazz_engine__fmt__value_v(jz_str *out, const void *v);
jz_str jazz_engine__err_display(const jz_err *e);
u64 jazz_engine__ptr_hash_v(const void *k);
bool jazz_engine__ptr_eq_v(const void *a, const void *b);
void jazz_engine__btree_put(jz_btree *t, void *key, const void *val);
bool jazz_engine__btree_set_add(jz_btree *t, void *key);
void *jazz_engine__btree_or_insert(jz_btree *t, void *key, void *val);
jz_str jazz_engine__str_view(jz_slice s);

/* ---------------------------------------------------------------- fields.rs */
typedef struct jazz_engine_ClaimPath { jazz_engine_StrVec segments; } jazz_engine_ClaimPath;
#define JAZZ_ENGINE_AGGREGATE_OUTPUT_PREFIX "__jazz_aggregate_"
#define JAZZ_ENGINE_LEFT_JOIN_PREFIX "left."
#define JAZZ_ENGINE_RIGHT_JOIN_PREFIX "right."
#define JAZZ_ENGINE_CLOSURE_REQUIRED_ELEMENT "__closure_required_element"
#define JAZZ_ENGINE_USER_COLUMN_PREFIX JAZZ_APP_COLUMN_PREFIX
jz_str jazz_engine__query_engine__fields__user_column_field(jz_slice column);
jazz_engine_StrVec jazz_engine__query_engine__fields__current_row_field_names(const jazz_model_TableSchema *table);
jz_str jazz_engine__query_engine__fields__aggregate_output_field(jz_slice output);
jz_str jazz_engine__query_engine__fields__aggregate_output_app_field(jz_slice output);
jz_str jazz_engine__query_engine__fields__aggregate_output_column(jz_slice output);
bool jazz_engine__query_engine__fields__aggregate_output_logical_name(jz_slice column, jz_slice *out);
bool jazz_engine__query_engine__fields__descriptor_public_name(const groove_DescriptorField *field, jz_slice *out);
jz_str jazz_engine__query_engine__fields__join_field(const char *prefix, jz_slice field);
jz_str jazz_engine__query_engine__fields__left_field(jz_slice field);
jz_str jazz_engine__query_engine__fields__right_field(jz_slice field);
jz_str jazz_engine__query_engine__fields__route_param_field(jz_slice param);
bool jazz_engine__query_engine__fields__route_param_from_field(jz_slice field, jz_slice *out);
jz_str jazz_engine__query_engine__fields__claim_param_field(const jazz_engine_ClaimPath *path);
bool jazz_engine__query_engine__fields__claim_path_from_param_field(jz_slice field, jazz_engine_ClaimPath *out);
jz_str jazz_engine__query_engine__fields__table_user_column_field(jz_slice table, jz_slice column);

/* ---------------------------------------------------------------- binding_values.rs */
groove_Value jazz_engine__query_engine__binding_values__coerce_prepared_binding_value(groove_Value value, const groove_ColumnType *column_type);

/* ---------------------------------------------------------------- publication.rs */
typedef enum { JAZZ_ENGINE_CRBR_PHYSICAL_COLUMN, JAZZ_ENGINE_CRBR_LOGICAL_FIELD } jazz_engine_CurrentRowBindingRole;
typedef enum { JAZZ_ENGINE_CRRV_APPLICATION_CELL, JAZZ_ENGINE_CRRV_PUBLIC_PROVENANCE, JAZZ_ENGINE_CRRV_HIDDEN_METADATA } jazz_engine_CurrentRowResultVisibility;
enum { JAZZ_ENGINE_CRPF_STORED_COLUMN, JAZZ_ENGINE_CRPF_RESULT_FIELD, JAZZ_ENGINE_CRPF_UNRESOLVED_SOURCE_CELL };
typedef struct jazz_engine_CurrentRowPublicationField {
    u8 tag;
    jazz_types_PhysicalColumnId id; /* StoredColumn */
    jz_str name;                    /* StoredColumn/UnresolvedSourceCell output_name; ResultField name */
    u8 visibility;                  /* ResultField */
} jazz_engine_CurrentRowPublicationField;
typedef JZ_VEC(jazz_engine_CurrentRowPublicationField) jazz_engine_CurrentRowPublicationFieldVec;
u8 jazz_engine__query_engine__publication__CurrentRowResultVisibility__current_row_metadata(jz_slice name);
const jz_str *jazz_engine__query_engine__publication__CurrentRowPublicationField__public_name(const jazz_engine_CurrentRowPublicationField *f);
const jz_str *jazz_engine__query_engine__publication__CurrentRowPublicationField__application_name(const jazz_engine_CurrentRowPublicationField *f);
u8 jazz_engine__query_engine__publication__CurrentRowPublicationField__role(const jazz_engine_CurrentRowPublicationField *f);
void jazz_engine__query_engine__publication__CurrentRowPublicationField__Clone__clone(jazz_engine_CurrentRowPublicationField *dst, const jazz_engine_CurrentRowPublicationField *src);
void jazz_engine__query_engine__publication__CurrentRowPublicationField__drop(jazz_engine_CurrentRowPublicationField *f);
bool jazz_engine__query_engine__publication__CurrentRowPublicationField__PartialEq__eq(const jazz_engine_CurrentRowPublicationField *a, const jazz_engine_CurrentRowPublicationField *b);
void jazz_engine__query_engine__publication__CurrentRowPublicationField__Debug__fmt(jz_str *out, const jazz_engine_CurrentRowPublicationField *f);
void jazz_engine__crpf_drop_v(void *f);
void jazz_engine__crpf_clone_v(void *dst, const void *src);

#include "input.h"
#include "output.h"
#include "read.h"
#include "policy.h"
#include "resolver.h"
#include "schemas.h"

/* input.rs QueryProgramRequest (declared here: it needs read/policy/output types) */
typedef struct jazz_engine_QueryProgramRequest {
    u8 authorization_mode; /* JAZZ_ENGINE_AUTH_* */
    jazz_engine_RequestedReadSet reads;
    jazz_engine_PolicyContext policy;
    jazz_engine_RowSetProgramInput input;
    jazz_engine_RowSetOutputRequest output;
} jazz_engine_QueryProgramRequest;
void JE_IN(QueryProgramRequest__Clone__clone)(jazz_engine_QueryProgramRequest *dst, const jazz_engine_QueryProgramRequest *src);
void JE_IN(QueryProgramRequest__drop)(jazz_engine_QueryProgramRequest *r);
bool JE_IN(QueryProgramRequest__PartialEq__eq)(const jazz_engine_QueryProgramRequest *a, const jazz_engine_QueryProgramRequest *b);

#endif
