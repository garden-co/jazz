/* model/public_schema_convert.rs: compile a developer-authored public schema into the runtime schema. */
#include "jazz_model/jazz_model.h"
#include <stdarg.h>
#include <stdio.h>

#define C(x) jazz_model__model__public_schema_convert__##x
#define Q(x) jazz_model__query__##x
#define PS(x) jazz_model__model__public_api__types__schema__##x
#define PV(x) jazz_model__model__public_api__types__value__##x
#define PE(x) jazz_model__model__public_api__policy__##x
#define RIR(x) jazz_model__model__public_api__relation_ir__##x
#define SC(x) jazz_model__schema__##x
#define VT(x) groove__records__values__ValueType__##x
#define GV(x) groove__records__values__Value__##x
#define VDROP(v, N) jz_vec_drop(&(v), sizeof(*(v).ptr), Q(N##__drop_v))
#define SL(s) jz_str_slice(&(s))
#define STRF(s) (int)(s).len, (s).ptr
#define TNF(t) (int)(t)->len, (t)->ptr /* %.*s of a TableName / ColumnName */

static const char DIRECT_USER_CLAIM[] = "user";
static const char *const PUBLIC_USER_SESSION_PATHS[] = {"user"};
static const char DIRECT_AUTH_MODE_CLAIM[] = "authMode";
static const char *const PUBLIC_AUTH_MODE_SESSION_PATHS[] = {"authMode", "auth_mode"};
static const char RESERVED_AGGREGATE_OUTPUT_PREFIX[] = "__jazz_aggregate_";

/* ---------------------------------------------------------------- SchemaConversionError */
/* SchemaConversionError { path, message }: Display "{path}: {message}" folded into the jz_err message */
static jz_err verr(jz_str path, const char *fmt, va_list ap) {
    char msg[1024];
    vsnprintf(msg, sizeof msg, fmt, ap);
    jz_err e = jz_err_fmt(JAZZ_MODEL_ERR_SCHEMA_CONVERSION, "%.*s: %s", STRF(path), msg);
    jz_str_drop(&path);
    return e;
}
static jz_err err(jz_str path, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    jz_err e = verr(path, fmt, ap);
    va_end(ap);
    return e;
}
static jz_err column_err(jazz_model_TableName table, jz_slice column, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    jz_err e = verr(jz_format("$.%.*s.%.*s", TNF(table), (int)column.len, column.ptr), fmt, ap);
    va_end(ap);
    return e;
}
/* The error path of `path` within `table`'s policies. */
static jz_str policy_path(jazz_model_TableName table, const char *path) { return jz_format("$.%.*s.%s", TNF(table), path); }
static const char *err_text(const jz_err *e) { return e->msg ? e->msg->ptr : ""; }
static int err_len(const jz_err *e) { return e->msg ? (int)e->msg->len : 0; }

static bool str_has_prefix(jz_slice s, const char *p) { u32 n = (u32)strlen(p); return s.len >= n && memcmp(s.ptr, p, n) == 0; }
static bool slice_is(jz_slice s, const char *c) { return s.len == strlen(c) && memcmp(s.ptr, c, s.len) == 0; }
static jz_slice tn(jazz_model_TableName t) { return jz_str_slice(t); }

static groove_ValueType vt_clone(const groove_ValueType *t) { groove_ValueType d; VT(Clone__clone)(&d, t); return d; }
static groove_Value gv(u8 tag) { groove_Value v; memset(&v, 0, sizeof(v)); v.tag = tag; return v; }
static groove_Value gv_some(groove_Value inner) {
    groove_Value v = gv(GROOVE_V_NULLABLE);
    v.u.nullable = JZ_NEW(groove_Value);
    *v.u.nullable = inner;
    return v;
}

/* ---------------------------------------------------------------- forward decls */
static jz_err convert_table(const jazz_model_public_Schema *schema, jazz_model_TableName name, const jazz_model_public_TableSchema *table,
                            jazz_model_TableSchema *out);
static void coerce_typed_literals(const jazz_model_public_Schema *schema, jazz_model_TableSchemaVec *tables);
static jz_err validate_converted_schema(const jazz_model_TableSchemaVec *tables);
static jz_err convert_column_type(jazz_model_TableName table, jz_slice column, const jazz_model_public_ColumnType *t, groove_ValueType *out);

jz_err C(convert_public_schema)(const jazz_model_public_Schema *schema, jazz_model_JazzSchema *out) {
    jazz_model_TableSchemaVec converted = {0};
    const jazz_model_TableName *name;
    const jazz_model_public_TableSchema *table;
    JZ_BTREE_FOR(&schema->tables, name, table) {
        jazz_model_TableSchema t;
        jz_err e = convert_table(schema, *name, table, &t);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&converted, sizeof(jazz_model_TableSchema), SC(TableSchema__drop_v)); return e; }
        JZ_VEC_PUSH(converted, t);
    }
    coerce_typed_literals(schema, &converted);
    jz_err e = validate_converted_schema(&converted);
    if (JZ_IS_ERR(e)) { jz_vec_drop(&converted, sizeof(jazz_model_TableSchema), SC(TableSchema__drop_v)); return e; }
    jazz_model_public_Schema public_schema;
    PS(Schema__Clone__clone)(&public_schema, schema);
    *out = SC(JazzSchema__from_runtime)(public_schema, (jazz_model_RuntimeSchema){converted});
    return JZ_OK;
}

/* Decode and compile the public JSON schema used by native bindings. */
jz_err C(decode_public_schema_json)(jz_slice bytes, jazz_model_JazzSchema *out) {
    jz_json *json = NULL;
    jz_err e = jz_json_parse(bytes, &json);
    jazz_model_public_Schema schema;
    if (!JZ_IS_ERR(e)) e = PS(Schema__Deserialize__deserialize)(json, &schema);
    if (json) jz_json_free(json);
    if (JZ_IS_ERR(e)) {
        jz_err s = jz_err_fmt(JAZZ_MODEL_ERR_STRING, "decode public schema: %.*s", err_len(&e), err_text(&e));
        jz_err_drop(&e);
        return s;
    }
    e = C(convert_public_schema)(&schema, out);
    PS(Schema__drop)(&schema);
    if (JZ_IS_ERR(e)) {
        jz_err s = jz_err_fmt(JAZZ_MODEL_ERR_STRING, "compile public schema: %.*s", err_len(&e), err_text(&e));
        jz_err_drop(&e);
        return s;
    }
    return JZ_OK;
}

/* ---------------------------------------------------------------- converted-schema validation */
static bool is_uuid_reference_type(const groove_ValueType *t) {
    switch (t->tag) {
    case GROOVE_VT_NULLABLE: return is_uuid_reference_type(t->u.inner);
    case GROOVE_VT_UUID: return true;
    case GROOVE_VT_ARRAY: return t->u.inner->tag == GROOVE_VT_UUID;
    default: return false;
    }
}

static const jazz_model_ColumnSchema *find_column(const jazz_model_TableSchema *t, const jz_str *name) {
    JZ_VEC_FOR(const jazz_model_ColumnSchema, c, t->columns) if (jz_str_eq(&c->name, name)) return c;
    return NULL;
}

static jz_err validate_converted_references(const jazz_model_TableSchemaVec *tables) {
    jz_btree table_names;
    jz_btree_init(&table_names, &jazz_model__STR_SET_DESC);
    JZ_VEC_FOR(const jazz_model_TableSchema, t, *tables) { jz_str k = jz_str_clone(&t->name); jz_btree_insert(&table_names, &k, NULL, NULL); }
    jz_err e = JZ_OK;
    JZ_VEC_FOR(const jazz_model_TableSchema, t, *tables) {
        const jz_str *column_name, *target_table;
        JZ_BTREE_FOR(&t->references, column_name, target_table) {
            jz_str path = jz_format("$.%.*s.%.*s", STRF(t->name), STRF(*column_name));
            if (!jz_btree_contains(&table_names, target_table)) {
                e = err(path, "reference target table '%.*s' does not exist", STRF(*target_table));
                goto done;
            }
            const jazz_model_ColumnSchema *c = find_column(t, column_name);
            if (!c) { e = err(path, "reference source column is not declared"); goto done; }
            if (!is_uuid_reference_type(&c->column_type)) { e = err(path, "reference source column must use UUID or UUID[] type"); goto done; }
            jz_str_drop(&path);
        }
    }
done:
    jz_btree_drop(&table_names);
    return e;
}

static void vt_drop_v(void *t) { VT(drop)(t); }
static const jz_btree_desc STR_VT_DESC = {sizeof(jz_str), sizeof(groove_ValueType), jz_cmp_str, jazz_model__str_drop_v, vt_drop_v};

static jz_err validate_converted_schema(const jazz_model_TableSchemaVec *tables) {
    jazz_model_RuntimeSchema schema;
    jz_vec_clone(&schema.tables, tables, sizeof(jazz_model_TableSchema), SC(TableSchema__Clone__clone_v));
    jz_err e = validate_converted_references(tables);
    if (JZ_IS_ERR(e)) { SC(RuntimeSchema__drop)(&schema); return e; }

    jz_btree branch_column_types;
    jz_btree_init(&branch_column_types, &STR_VT_DESC);
    JZ_VEC_FOR(const jazz_model_TableSchema, t, *tables) {
        jz_btree bound;
        jz_btree_init(&bound, &jazz_model__STR_SET_DESC);
        JZ_VEC_FOR(const jz_str, bc, t->branch_by) {
            jz_str path = jz_format("$.%.*s.branchBy", STRF(t->name));
            jz_str dbg = {0};
            jazz_model__debug_str(&dbg, bc->ptr, bc->len);
            const jazz_model_ColumnSchema *c = find_column(t, bc);
            jz_str k = jz_str_clone(bc);
            u8 tag = c ? c->column_type.tag : 0;
            if (!c) e = err(path, "unknown branch column %.*s", STRF(dbg));
            else if (jz_btree_insert(&bound, &k, NULL, NULL)) e = err(path, "branch column %.*s is listed more than once", STRF(dbg));
            else if (tag == GROOVE_VT_NULLABLE) e = err(path, "branch column %.*s must be non-null", STRF(dbg));
            else if (tag != GROOVE_VT_STRING && tag != GROOVE_VT_UUID && tag != GROOVE_VT_U8 && tag != GROOVE_VT_U16 && tag != GROOVE_VT_U32 &&
                     tag != GROOVE_VT_U64 && tag != GROOVE_VT_I32 && tag != GROOVE_VT_I64 && tag != GROOVE_VT_ENUM_TAG)
                e = err(path, "branch columns require string, UUID, stable enum, or fixed-width integer values");
            else {
                jz_str bk = jz_str_clone(bc);
                groove_ValueType ty = vt_clone(&c->column_type), existing;
                if (jz_btree_insert(&branch_column_types, &bk, &ty, &existing)) {
                    bool same = VT(PartialEq__eq)(&existing, &c->column_type);
                    VT(drop)(&existing);
                    if (!same) e = err(path, "branch column %.*s must have the same type in every table", STRF(dbg));
                    else jz_str_drop(&path);
                } else {
                    jz_str_drop(&path);
                }
            }
            if (!c) jz_str_drop(&k);
            jz_str_drop(&dbg);
            if (JZ_IS_ERR(e)) break;
        }
        jz_btree_drop(&bound);
        if (JZ_IS_ERR(e)) goto done;
    }
    JZ_VEC_FOR(const jazz_model_TableSchema, t, *tables) {
        jazz_model_ValidatedQuery v;
        if (t->read_policy) {
            e = Q(Query__validate_runtime)(t->read_policy, &schema, &v);
            if (JZ_IS_ERR(e)) {
                jz_err c = err(jz_format("$.%.*s.policies.select.using", STRF(t->name)), "converted read policy is invalid: %.*s",
                               err_len(&e), err_text(&e));
                jz_err_drop(&e);
                e = c;
                goto done;
            }
            Q(ValidatedQuery__drop)(&v);
        }
        const char *labels[4];
        const jazz_model_Query *policies[4];
        u32 n = SC(WritePolicies__iter)(&t->write_policies, labels, policies);
        for (u32 i = 0; i < n; i++) {
            e = Q(Query__validate_runtime)(policies[i], &schema, &v);
            if (JZ_IS_ERR(e)) {
                jz_err c = err(jz_format("$.%.*s.policies.%s", STRF(t->name), labels[i]), "converted write policy is invalid: %.*s",
                               err_len(&e), err_text(&e));
                jz_err_drop(&e);
                e = c;
                goto done;
            }
            Q(ValidatedQuery__drop)(&v);
        }
    }
done:
    jz_btree_drop(&branch_column_types);
    SC(RuntimeSchema__drop)(&schema);
    return e;
}

/* ---------------------------------------------------------------- typed-literal coercion */
enum { TARGET_CORE = 0, TARGET_PUBLIC_ENUM = 1 };
typedef struct TypedLiteralTarget { u8 tag; groove_ValueType core; } TypedLiteralTarget;
static void target_drop_v(void *p) { TypedLiteralTarget *t = p; if (t->tag == TARGET_CORE) VT(drop)(&t->core); }
static const jz_btree_desc STR_TARGET_DESC = {sizeof(jz_str), sizeof(TypedLiteralTarget), jz_cmp_str, jazz_model__str_drop_v, target_drop_v};
static void btree_drop_v(void *p) { jz_btree_drop(p); }
/* Literal-coercion target of every column, by table then column name. */
static const jz_btree_desc COLUMN_TYPES_DESC = {sizeof(jz_str), sizeof(jz_btree), jz_cmp_str, jazz_model__str_drop_v, btree_drop_v};
typedef jz_btree ColumnTypes;

static const TypedLiteralTarget *column_target(const ColumnTypes *types, jz_slice table, jz_slice column) {
    jz_str tk = jz_str_from_slice(table), ck = jz_str_from_slice(column);
    const jz_btree *columns = jz_btree_get(types, &tk);
    const TypedLiteralTarget *t = columns ? jz_btree_get(columns, &ck) : NULL;
    jz_str_drop(&tk); jz_str_drop(&ck);
    return t;
}

static bool column_type_for_operand(jz_slice table, jz_slice column, const ColumnTypes *types, TypedLiteralTarget *out) {
    const groove_ValueType *ty = jazz_types__ids__AuthorSubject__metadata_type(column);
    if (ty) { out->tag = TARGET_CORE; out->core = vt_clone(ty); return true; }
    if (slice_is(column, "id")) { out->tag = TARGET_CORE; out->core = VT(scalar)(GROOVE_VT_UUID); return true; }
    if (slice_is(column, "$createdAt") || slice_is(column, "$updatedAt")) { out->tag = TARGET_CORE; out->core = VT(scalar)(GROOVE_VT_U64); return true; }
    const TypedLiteralTarget *t = column_target(types, table, column);
    if (!t) return false;
    out->tag = t->tag;
    if (t->tag == TARGET_CORE) out->core = vt_clone(&t->core);
    return true;
}

static groove_Value uuid_string(const jz_uuid *u) {
    char buf[37];
    jz_uuid_to_string(u, buf);
    groove_Value v = gv(GROOVE_V_STRING);
    v.u.s = jz_str_from(buf);
    return v;
}
static void string_to_uuid_if_parses(groove_Value *v) {
    jz_uuid u;
    jz_err e = jz_uuid_parse(SL(v->u.s), &u);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return; }
    jz_str_drop(&v->u.s);
    *v = gv(GROOVE_V_UUID);
    v->u.uuid = u;
}

/* in place: coerce_literal_for_column_type(value, column_type) */
static void coerce_literal_for_column_type(groove_Value *v, const groove_ValueType *t) {
    if (v->tag == GROOVE_V_UUID && t->tag == GROOVE_VT_STRING) { *v = uuid_string(&v->u.uuid); return; }
    if (v->tag == GROOVE_V_STRING && t->tag == GROOVE_VT_UUID) { string_to_uuid_if_parses(v); return; }
    if (v->tag == GROOVE_V_U64 && t->tag == GROOVE_VT_I64) {
        if (v->u.u64_ <= (u64)INT64_MAX) { i64 x = (i64)v->u.u64_; *v = gv(GROOVE_V_I64); v->u.i64_ = x; }
        return;
    }
    if (v->tag == GROOVE_V_U64 && t->tag == GROOVE_VT_I32) {
        if (v->u.u64_ <= (u64)INT32_MAX) { i32 x = (i32)v->u.u64_; *v = gv(GROOVE_V_I32); v->u.i32_ = x; }
        return;
    }
    if (v->tag == GROOVE_V_NULLABLE && v->u.nullable && t->tag == GROOVE_VT_NULLABLE) { coerce_literal_for_column_type(v->u.nullable, t->u.inner); return; }
    if (v->tag == GROOVE_V_ARRAY && t->tag == GROOVE_VT_ARRAY) {
        JZ_VEC_FOR(groove_Value, it, v->u.items) coerce_literal_for_column_type(it, t->u.inner);
        return;
    }
    if (v->tag == GROOVE_V_TUPLE && t->tag == GROOVE_VT_TUPLE && v->u.items.len == t->u.tuple.len) {
        for (u32 i = 0; i < v->u.items.len; i++) coerce_literal_for_column_type(&v->u.items.ptr[i], &t->u.tuple.ptr[i]);
        return;
    }
    if (v->tag == GROOVE_V_NULLABLE && v->u.nullable) { coerce_literal_for_column_type(v->u.nullable, t); return; }
    if (t->tag == GROOVE_VT_NULLABLE) { coerce_literal_for_column_type(v, t->u.inner); return; }
}

static void coerce_literal_for_target(groove_Value *v, const TypedLiteralTarget *target) {
    if (target->tag == TARGET_CORE) coerce_literal_for_column_type(v, &target->core);
    else if (v->tag == GROOVE_V_STRING) string_to_uuid_if_parses(v);
}

static const groove_ScalarEnumSchema *groove_column_type_enum_schema(const groove_ValueType *t) {
    if (t->tag == GROOVE_VT_ENUM_TAG) return &t->u.enum_tag;
    if (t->tag == GROOVE_VT_NULLABLE) return groove_column_type_enum_schema(t->u.inner);
    return NULL;
}
static const groove_ScalarEnumSchema *column_enum_schema(jz_slice table, jz_slice column, const ColumnTypes *types) {
    const TypedLiteralTarget *t = column_target(types, table, column);
    if (!t || t->tag != TARGET_CORE) return NULL;
    return groove_column_type_enum_schema(&t->core);
}
static bool column_enum_literal_discriminant(jz_slice table, jz_slice column, const jazz_model_Operand *literal, const ColumnTypes *types, u8 *out) {
    if (literal->tag != JAZZ_OPERAND_LITERAL || literal->literal.tag != GROOVE_V_STRING) return false;
    const groove_ScalarEnumSchema *s = column_enum_schema(table, column, types);
    if (!s) return false;
    jz_err e = groove__records__values__ScalarEnumSchema__discriminant(s, literal->literal.u.s.ptr, literal->literal.u.s.len, out);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    return true;
}
static bool column_is_enum(jz_slice table, jz_slice column, const ColumnTypes *types) { return column_enum_schema(table, column, types) != NULL; }
static bool groove_column_type_is_string(const groove_ValueType *t) {
    if (t->tag == GROOVE_VT_STRING) return true;
    if (t->tag == GROOVE_VT_NULLABLE) return groove_column_type_is_string(t->u.inner);
    return false;
}
static bool column_is_string(jz_slice table, jz_slice column, const ColumnTypes *types) {
    const TypedLiteralTarget *t = column_target(types, table, column);
    return t && t->tag == TARGET_CORE && groove_column_type_is_string(&t->core);
}

static bool coerce_operand_pair_typed_literal(jz_slice table, const jazz_model_Operand *column_operand, jazz_model_Operand *literal_operand,
                                              const ColumnTypes *types) {
    if (column_operand->tag != JAZZ_OPERAND_COLUMN) return false;
    jz_slice column = SL(column_operand->name);
    TypedLiteralTarget target;
    if (column_type_for_operand(table, column, types, &target)) {
        if (literal_operand->tag == JAZZ_OPERAND_LITERAL) coerce_literal_for_target(&literal_operand->literal, &target);
        target_drop_v(&target);
    }
    u8 discriminant;
    if (column_enum_literal_discriminant(table, column, literal_operand, types, &discriminant)) {
        GV(drop)(&literal_operand->literal);
        literal_operand->literal = gv(GROOVE_V_ENUM_TAG);
        literal_operand->literal.u.enum_tag = discriminant;
        return false;
    }
    if (column_is_enum(table, column, types)) return true;
    if (!column_is_string(table, column, types)) return false;
    if (literal_operand->tag == JAZZ_OPERAND_LITERAL && literal_operand->literal.tag == GROOVE_V_UUID)
        literal_operand->literal = uuid_string(&literal_operand->literal.u.uuid);
    return false;
}

static void coerce_predicate_typed_literals(jz_slice table, jazz_model_Predicate *p, const ColumnTypes *types);
static void coerce_predicates_typed_literals(jz_slice table, jazz_model_PredicateVec *ps, const ColumnTypes *types) {
    JZ_VEC_FOR(jazz_model_Predicate, p, *ps) coerce_predicate_typed_literals(table, p, types);
}
typedef struct InRetainCtx { jz_slice table; const jazz_model_Operand *left; const ColumnTypes *types; } InRetainCtx;
static bool in_retain(void *item, void *ctx) {
    InRetainCtx *c = ctx;
    return !coerce_operand_pair_typed_literal(c->table, c->left, item, c->types);
}
static void coerce_predicate_typed_literals(jz_slice table, jazz_model_Predicate *p, const ColumnTypes *types) {
    switch (p->tag) {
    case JAZZ_PRED_ALL: case JAZZ_PRED_ANY: coerce_predicates_typed_literals(table, &p->children, types); break;
    case JAZZ_PRED_NOT: coerce_predicate_typed_literals(table, p->inner, types); break;
    case JAZZ_PRED_EQ: case JAZZ_PRED_NE: case JAZZ_PRED_LT: case JAZZ_PRED_LTE: case JAZZ_PRED_GT: case JAZZ_PRED_GTE:
    case JAZZ_PRED_CONTAINS:
        if (coerce_operand_pair_typed_literal(table, &p->left, &p->right, types) ||
            coerce_operand_pair_typed_literal(table, &p->right, &p->left, types)) {
            Q(Predicate__drop)(p);
            *p = Q(any_of)((jazz_model_PredicateVec){0});
        }
        break;
    case JAZZ_PRED_IN: {
        InRetainCtx ctx = {table, &p->left, types};
        jz_vec_retain(&p->values, sizeof(jazz_model_Operand), in_retain, &ctx, Q(Operand__drop_v));
        break;
    }
    case JAZZ_PRED_IS_NULL: break;
    case JAZZ_PRED_ENUM_MATCH: coerce_predicate_typed_literals(table, p->inner, types); break;
    }
}

static void coerce_join_typed_literals(jazz_model_JoinVia *join, const ColumnTypes *types) {
    coerce_predicates_typed_literals(SL(join->table), &join->filters, types);
    JZ_VEC_FOR(jazz_model_JoinVia, nested, join->nested_joins) coerce_join_typed_literals(nested, types);
}
static void coerce_reachable_typed_literals(jazz_model_ReachableViaVec *reachable, const ColumnTypes *types) {
    JZ_VEC_FOR(jazz_model_ReachableVia, r, *reachable) {
        coerce_predicates_typed_literals(SL(r->access_table), &r->access_filters, types);
        coerce_predicates_typed_literals(SL(r->edge_table), &r->edge_filters, types);
        if (r->seed) coerce_predicates_typed_literals(SL(r->seed->table), &r->seed->filters, types);
    }
}
void C(coerce_query_typed_literals)(jazz_model_Query *q, const ColumnTypes *types) {
    coerce_predicates_typed_literals(SL(q->table), &q->filters, types);
    JZ_VEC_FOR(jazz_model_JoinVia, join, q->joins) coerce_join_typed_literals(join, types);
    coerce_reachable_typed_literals(&q->reachable, types);
    JZ_VEC_FOR(jazz_model_PolicyBranch, branch, q->policy_branches) {
        coerce_predicates_typed_literals(SL(q->table), &branch->filters, types);
        JZ_VEC_FOR(jazz_model_JoinVia, join, branch->joins) coerce_join_typed_literals(join, types);
        coerce_reachable_typed_literals(&branch->reachable, types);
    }
}
static void coerce_optional_query_typed_literals(jazz_model_Query *q, const ColumnTypes *types) {
    if (q) C(coerce_query_typed_literals)(q, types);
}

static void coerce_typed_literals(const jazz_model_public_Schema *schema, jazz_model_TableSchemaVec *tables) {
    ColumnTypes types;
    jz_btree_init(&types, &COLUMN_TYPES_DESC);
    JZ_VEC_FOR(const jazz_model_TableSchema, t, *tables) {
        jz_btree columns;
        jz_btree_init(&columns, &STR_TARGET_DESC);
        jazz_model_TableName name = PS(TableName__new)(jz_str_clone(&t->name));
        const jazz_model_public_TableSchema *public_table = jz_btree_get(&schema->tables, &name);
        JZ_VEC_FOR(const jazz_model_ColumnSchema, c, t->columns) {
            const jazz_model_ColumnDescriptor *pc = public_table ? PS(RowDescriptor__column)(&public_table->columns, SL(c->name)) : NULL;
            TypedLiteralTarget target;
            if (pc && pc->column_type.tag == JAZZ_CT_ENUM) target.tag = TARGET_PUBLIC_ENUM;
            else { target.tag = TARGET_CORE; target.core = vt_clone(&c->column_type); }
            jz_str k = jz_str_clone(&c->name);
            TypedLiteralTarget old;
            if (jz_btree_insert(&columns, &k, &target, &old)) target_drop_v(&old);
        }
        jz_str k = jz_str_clone(&t->name);
        jz_btree old;
        if (jz_btree_insert(&types, &k, &columns, &old)) jz_btree_drop(&old);
    }
    JZ_VEC_FOR(jazz_model_TableSchema, t, *tables) {
        if (t->read_policy) C(coerce_query_typed_literals)(t->read_policy, &types);
        coerce_optional_query_typed_literals(t->write_policies.insert_check, &types);
        coerce_optional_query_typed_literals(t->write_policies.update_using, &types);
        coerce_optional_query_typed_literals(t->write_policies.update_check, &types);
        coerce_optional_query_typed_literals(t->write_policies.delete_using, &types);
    }
    jz_btree_drop(&types);
}

/* ---------------------------------------------------------------- columns */
static jz_err convert_default_for_column_type(jazz_model_TableName table, jz_slice column, const jazz_model_public_ColumnType *ct,
                                              const jazz_model_public_Value *value, groove_Value *out);

static jz_err validate_payload_enum_field(jazz_model_TableName table, jz_slice column, const jz_str *case_name, const jazz_model_ColumnDescriptor *field);
static jz_err fields_to_descriptor(jazz_model_TableName table, const jazz_model_ColumnDescriptorVec *fields, groove_RecordDescriptor *out,
                                   jz_slice column, const jz_str *case_name) {
    u32 n = fields->len;
    jz_str *names = jz_zalloc(sizeof(jz_str) * (n ? n : 1));
    groove_ValueType *types = jz_zalloc(sizeof(groove_ValueType) * (n ? n : 1));
    for (u32 i = 0; i < n; i++) {
        const jazz_model_ColumnDescriptor *f = &fields->ptr[i];
        jz_err e = validate_payload_enum_field(table, column, case_name, f);
        if (!JZ_IS_ERR(e)) e = convert_column_type(table, tn(f->name), &f->column_type, &types[i]);
        if (JZ_IS_ERR(e)) {
            for (u32 j = 0; j < i; j++) { jz_str_drop(&names[j]); VT(drop)(&types[j]); }
            jz_free(names); jz_free(types);
            return e;
        }
        if (f->nullable) types[i] = VT(nullable)(types[i]);
        names[i] = jz_str_from_slice(tn(f->name));
    }
    *out = groove__records__RecordDescriptor__new(names, types, n); /* moves in */
    jz_free(names); jz_free(types);
    return JZ_OK;
}

/* Enforce the same v1 payload-enum boundary for raw public-schema ingress as the TypeScript DSL. */
static jz_err validate_payload_enum_field(jazz_model_TableName table, jz_slice column, const jz_str *case_name, const jazz_model_ColumnDescriptor *field) {
    jz_str path = jz_format("$.%.*s.%.*s.%.*s.%.*s", TNF(table), (int)column.len, column.ptr, STRF(*case_name), TNF(field->name));
    if (slice_is(tn(field->name), "type")) return err(path, "enum payload field name \"type\" is reserved for the discriminant");
    if (field->name->len && field->name->ptr[0] == '$') return err(path, "enum payload field names starting with \"$\" are reserved for magic columns");
    if (field->references) return err(path, "enum payload fields cannot use references; use UUID values instead");
    switch (field->column_type.tag) {
    case JAZZ_CT_BOOLEAN: case JAZZ_CT_TEXT: case JAZZ_CT_TIMESTAMP: case JAZZ_CT_DOUBLE: case JAZZ_CT_UUID: case JAZZ_CT_BYTEA:
    case JAZZ_CT_JSON: case JAZZ_CT_INTEGER: case JAZZ_CT_BIGINT:
        jz_str_drop(&path);
        return JZ_OK;
    default:
        return err(path, "payload enum v1 fields must be scalar columns; arrays, rows, and nested enums are not supported");
    }
}

static jz_err convert_column_type(jazz_model_TableName table, jz_slice column, const jazz_model_public_ColumnType *t, groove_ValueType *out) {
    switch (t->tag) {
    case JAZZ_CT_BOOLEAN: *out = VT(scalar)(GROOVE_VT_BOOL); return JZ_OK;
    case JAZZ_CT_TEXT: *out = VT(scalar)(GROOVE_VT_STRING); return JZ_OK;
    case JAZZ_CT_TIMESTAMP: *out = VT(scalar)(GROOVE_VT_U64); return JZ_OK;
    case JAZZ_CT_DOUBLE: *out = VT(scalar)(GROOVE_VT_F64); return JZ_OK;
    case JAZZ_CT_UUID: *out = VT(scalar)(GROOVE_VT_UUID); return JZ_OK;
    case JAZZ_CT_BYTEA: *out = VT(scalar)(GROOVE_VT_BYTES); return JZ_OK;
    case JAZZ_CT_ENUM: *out = VT(scalar)(GROOVE_VT_STRING); return JZ_OK;
    case JAZZ_CT_SCALAR_ENUM: {
        groove_StrVec variants = {0};
        jz_vec_clone(&variants, &t->variants, sizeof(jz_str), jazz_model__str_clone_v);
        memset(out, 0, sizeof(*out));
        out->tag = GROOVE_VT_ENUM_TAG;
        jz_err e = groove__records__values__ScalarEnumSchema__new(jz_str_clone(&t->name), variants, &out->u.enum_tag);
        if (JZ_IS_ERR(e)) {
            jz_err c = column_err(table, column, "%.*s", err_len(&e), err_text(&e));
            jz_err_drop(&e);
            return c;
        }
        return JZ_OK;
    }
    case JAZZ_CT_ENUM_PAYLOAD:
    case JAZZ_CT_CATALOGUE_ENUM_PAYLOAD: {
        groove_EnumCaseVec cases = {0};
        JZ_VEC_FOR(const jazz_model_EnumCaseDescriptor, c, t->cases) {
            jz_err e = JZ_OK;
            if (c->name.len == 0) e = column_err(table, column, "enum case names cannot be empty");
            groove_RecordDescriptor payload;
            if (!JZ_IS_ERR(e)) e = fields_to_descriptor(table, &c->fields, &payload, column, &c->name);
            if (JZ_IS_ERR(e)) {
                JZ_VEC_FOR(groove_EnumCase, done, cases) jz_str_drop(&done->name);
                jz_vec_drop(&cases, sizeof(groove_EnumCase), NULL);
                return e;
            }
            groove_EnumCase ec = groove__records__values__EnumCase__new(jz_str_clone(&c->name), payload);
            JZ_VEC_PUSH(cases, ec);
        }
        jz_str enum_name = t->tag == JAZZ_CT_CATALOGUE_ENUM_PAYLOAD ? jz_str_clone(&t->name)
                                                                  : jz_format("%.*s_%.*s", TNF(table), (int)column.len, column.ptr);
        groove_EnumSchema *schema = JZ_NEW(groove_EnumSchema);
        jz_err e = groove__records__values__EnumSchema__new(enum_name, cases, schema);
        if (JZ_IS_ERR(e)) {
            jz_free(schema);
            jz_err c = column_err(table, column, "%.*s", err_len(&e), err_text(&e));
            jz_err_drop(&e);
            return c;
        }
        memset(out, 0, sizeof(*out));
        out->tag = GROOVE_VT_ENUM;
        out->u.enum_ = schema;
        return JZ_OK;
    }
    case JAZZ_CT_ARRAY: {
        groove_ValueType element;
        JZ_TRY(convert_column_type(table, column, t->element, &element));
        *out = VT(array_of)(element);
        return JZ_OK;
    }
    /* Public INTEGER / BIGINT follow PostgreSQL semantics: signed 32/64-bit integers. */
    case JAZZ_CT_INTEGER: *out = VT(scalar)(GROOVE_VT_I32); return JZ_OK;
    case JAZZ_CT_BIGINT: *out = VT(scalar)(GROOVE_VT_I64); return JZ_OK;
    case JAZZ_CT_TRANSACTION_ID: return column_err(table, column, "TransactionId columns are not supported by core schema conversion yet");
    case JAZZ_CT_JSON: *out = VT(scalar)(GROOVE_VT_STRING); return JZ_OK;
    default: return column_err(table, column, "nested Row columns are not supported by core schema conversion yet");
    }
}

static jz_err convert_default_for_column_type(jazz_model_TableName table, jz_slice column, const jazz_model_public_ColumnType *ct,
                                              const jazz_model_public_Value *value, groove_Value *out) {
    u8 t = ct->tag, v = value->tag;
    if (t == JAZZ_CT_BOOLEAN && v == JAZZ_PV_BOOLEAN) { *out = gv(GROOVE_V_BOOL); out->u.b = value->u.boolean; return JZ_OK; }
    if ((t == JAZZ_CT_TEXT || t == JAZZ_CT_JSON) && v == JAZZ_PV_TEXT) { *out = gv(GROOVE_V_STRING); out->u.s = jz_str_clone(&value->u.text); return JZ_OK; }
    if (t == JAZZ_CT_ENUM && v == JAZZ_PV_TEXT) {
        bool declared = false;
        JZ_VEC_FOR(const jz_str, var, ct->variants) if (jz_str_eq(var, &value->u.text)) { declared = true; break; }
        if (!declared) {
            jz_str dbg = {0};
            jazz_model__debug_str(&dbg, value->u.text.ptr, value->u.text.len);
            jz_err e = column_err(table, column, "enum default value %.*s is not declared", STRF(dbg));
            jz_str_drop(&dbg);
            return e;
        }
        *out = gv(GROOVE_V_STRING); out->u.s = jz_str_clone(&value->u.text);
        return JZ_OK;
    }
    if ((t == JAZZ_CT_ENUM_PAYLOAD || t == JAZZ_CT_CATALOGUE_ENUM_PAYLOAD) && v == JAZZ_PV_ENUM) {
        const jz_str *case_name = &value->u.enum_.case_;
        const jazz_model_public_ValueVec *values = &value->u.enum_.values;
        u32 case_index = 0;
        const jazz_model_EnumCaseDescriptor *entry = NULL;
        for (u32 i = 0; i < ct->cases.len; i++) if (jz_str_eq(&ct->cases.ptr[i].name, case_name)) { entry = &ct->cases.ptr[i]; case_index = i; break; }
        if (!entry) {
            jz_str dbg = {0};
            jazz_model__debug_str(&dbg, case_name->ptr, case_name->len);
            jz_err e = column_err(table, column, "payload enum default case %.*s is not declared", STRF(dbg));
            jz_str_drop(&dbg);
            return e;
        }
        if (entry->fields.len != values->len) return column_err(table, column, "payload enum default field count does not match its case");
        u32 n = entry->fields.len;
        jz_str *names = jz_zalloc(sizeof(jz_str) * (n ? n : 1));
        groove_ValueType *types = jz_zalloc(sizeof(groove_ValueType) * (n ? n : 1));
        groove_Value *converted = jz_zalloc(sizeof(groove_Value) * (n ? n : 1));
        jz_err e = JZ_OK;
        u32 done = 0;
        for (; done < n; done++) {
            const jazz_model_ColumnDescriptor *field = &entry->fields.ptr[done];
            const jazz_model_public_Value *fv = &values->ptr[done];
            e = convert_column_type(table, tn(field->name), &field->column_type, &types[done]);
            if (JZ_IS_ERR(e)) break;
            if (field->nullable) types[done] = VT(nullable)(types[done]);
            if (fv->tag == JAZZ_PV_NULL) {
                if (!field->nullable) {
                    VT(drop)(&types[done]);
                    e = err(jz_format("$.%.*s.%.*s.%.*s", TNF(table), (int)column.len, column.ptr, TNF(field->name)),
                            "payload enum default cannot use null for a required field");
                    break;
                }
                converted[done] = gv(GROOVE_V_NULLABLE);
            } else {
                groove_Value inner;
                e = convert_default_for_column_type(table, tn(field->name), &field->column_type, fv, &inner);
                if (JZ_IS_ERR(e)) { VT(drop)(&types[done]); break; }
                converted[done] = field->nullable ? gv_some(inner) : inner;
            }
            names[done] = jz_str_from_slice(tn(field->name));
        }
        if (JZ_IS_ERR(e)) {
            for (u32 j = 0; j < done; j++) { jz_str_drop(&names[j]); VT(drop)(&types[j]); GV(drop)(&converted[j]); }
        } else {
            groove_RecordDescriptor descriptor = groove__records__RecordDescriptor__new(names, types, n);
            *out = gv(GROOVE_V_ENUM);
            e = groove__records__values__EnumValue__create(case_index, descriptor, converted, n, &out->u.enum_);
            if (JZ_IS_ERR(e)) {
                jz_err c = column_err(table, column, "%.*s", err_len(&e), err_text(&e));
                jz_err_drop(&e);
                e = c;
            }
            for (u32 j = 0; j < n; j++) GV(drop)(&converted[j]);
        }
        jz_free(names); jz_free(types); jz_free(converted);
        return e;
    }
    if (t == JAZZ_CT_TIMESTAMP && v == JAZZ_PV_TIMESTAMP) { *out = gv(GROOVE_V_U64); out->u.u64_ = value->u.timestamp; return JZ_OK; }
    if (t == JAZZ_CT_DOUBLE && v == JAZZ_PV_DOUBLE) { *out = gv(GROOVE_V_F64); out->u.f64_ = value->u.dbl; return JZ_OK; }
    if (t == JAZZ_CT_UUID && v == JAZZ_PV_UUID) { *out = gv(GROOVE_V_UUID); out->u.uuid = *value->u.uuid; return JZ_OK; }
    if (t == JAZZ_CT_BYTEA && v == JAZZ_PV_BYTEA) {
        *out = gv(GROOVE_V_BYTES);
        jz_vec_clone(&out->u.bytes, &value->u.bytea, 1, NULL);
        return JZ_OK;
    }
    if (t == JAZZ_CT_INTEGER && v == JAZZ_PV_INTEGER) { *out = gv(GROOVE_V_I32); out->u.i32_ = value->u.integer; return JZ_OK; }
    if (t == JAZZ_CT_INTEGER && v == JAZZ_PV_BIGINT) {
        if (value->u.bigint < INT32_MIN || value->u.bigint > INT32_MAX)
            return column_err(table, column, "BIGINT default %lld is outside INTEGER range", (long long)value->u.bigint);
        *out = gv(GROOVE_V_I32); out->u.i32_ = (i32)value->u.bigint;
        return JZ_OK;
    }
    if (t == JAZZ_CT_BIGINT && v == JAZZ_PV_INTEGER) { *out = gv(GROOVE_V_I64); out->u.i64_ = value->u.integer; return JZ_OK; }
    if (t == JAZZ_CT_BIGINT && v == JAZZ_PV_BIGINT) { *out = gv(GROOVE_V_I64); out->u.i64_ = value->u.bigint; return JZ_OK; }
    if (t == JAZZ_CT_ARRAY && v == JAZZ_PV_ARRAY) {
        *out = gv(GROOVE_V_ARRAY);
        JZ_VEC_FOR(const jazz_model_public_Value, it, value->u.array) {
            groove_Value item;
            jz_err e = convert_default_for_column_type(table, column, ct->element, it, &item);
            if (JZ_IS_ERR(e)) { GV(drop)(out); return e; }
            JZ_VEC_PUSH(out->u.items, item);
        }
        return JZ_OK;
    }
    jz_str vd = {0}, td = {0};
    PV(Value__Debug__fmt)(&vd, value);
    PS(ColumnType__Debug__fmt)(&td, ct);
    jz_err e = column_err(table, column, "default %.*s does not match column type %.*s", STRF(vd), STRF(td));
    jz_str_drop(&vd); jz_str_drop(&td);
    return e;
}

static jz_err message_err(jazz_model_TableName table, jz_slice column, jz_err e) {
    jz_err c = column_err(table, column, "%.*s", err_len(&e), err_text(&e));
    jz_err_drop(&e);
    return c;
}

static jz_err convert_column_default(jazz_model_TableName table, const jazz_model_ColumnDescriptor *column, const jazz_model_public_Value *value,
                                     groove_Value *out) {
    jz_slice name = tn(column->name);
    jz_err e = jazz_model__model__public_schema__validate_json_value(value, &column->column_type, name);
    if (JZ_IS_ERR(e)) return message_err(table, name, e);
    if (value->tag == JAZZ_PV_NULL) {
        if (!column->nullable) return column_err(table, name, "null default requires a nullable column");
        *out = gv(GROOVE_V_NULLABLE);
        return JZ_OK;
    }
    /* Record cells carry logical signed values; Groove applies its order-preserving transform for keys. */
    groove_Value d;
    JZ_TRY(convert_default_for_column_type(table, name, &column->column_type, value, &d));
    *out = column->nullable ? gv_some(d) : d;
    return JZ_OK;
}

static jz_err convert_column(jazz_model_TableName table, const jazz_model_ColumnDescriptor *column, jazz_model_ColumnSchema *out) {
    jz_slice name = tn(column->name);
    jz_err e = jazz_model__model__public_schema__validate_json_schemas(&column->column_type, name);
    if (JZ_IS_ERR(e)) return message_err(table, name, e);
    groove_ValueType ty;
    JZ_TRY(convert_column_type(table, name, &column->column_type, &ty));
    if (column->nullable) ty = VT(nullable)(ty);
    jazz_model_ColumnSchema converted = SC(ColumnSchema__new)(jz_str_from_slice(name), ty);
    /* JSON is logically Groove's canonical string value, but its physical large-scalar interpretation
     * is schema-derived and deliberately distinct from text. */
    if (column->column_type.tag == JAZZ_CT_JSON) converted.large_value_kind = JAZZ_LVK_JSON;
    if (column->default_) {
        groove_Value d;
        e = convert_column_default(table, column, column->default_, &d);
        if (JZ_IS_ERR(e)) { SC(ColumnSchema__drop)(&converted); return e; }
        converted.has_default = true;
        converted.default_ = d;
    }
    *out = converted;
    return JZ_OK;
}

static jz_err convert_merge_strategy(jazz_model_TableName table, const jazz_model_ColumnDescriptor *column, u8 strategy, u8 *out) {
    jz_err e = PS(ColumnDescriptor__validate_merge_strategy)(column);
    if (JZ_IS_ERR(e)) return message_err(table, tn(column->name), e);
    *out = strategy == JAZZ_MERGE_COUNTER ? JAZZ_MERGE_STRATEGY_COUNTER : JAZZ_MERGE_STRATEGY_GSET;
    return JZ_OK;
}

/* ================================================================ policies */
typedef struct ExpansionKey { jz_str table; u8 operation; } ExpansionKey;
typedef JZ_VEC(ExpansionKey) ExpansionPath;

static jz_str sub_path(jz_slice path, const char *fmt, ...) {
    char tail[512];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(tail, sizeof tail, fmt, ap);
    va_end(ap);
    return jz_format("%.*s%s", (int)path.len, path.ptr, tail);
}
static jz_err perr(jazz_model_TableName table, jz_slice path, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    jz_err e = verr(jz_format("$.%.*s.%.*s", TNF(table), (int)path.len, path.ptr), fmt, ap);
    va_end(ap);
    return e;
}

/* Query builder steps inlined (Query::join_via*, Query::policy_branch) */
static void push_join(jazz_model_Query *q, jz_str table, jz_str on_column, u8 target, jazz_model_OptStr source_column,
                      jazz_model_JoinCorrelationVec correlated, jazz_model_PredicateVec filters, jazz_model_JoinViaVec nested) {
    jazz_model_JoinVia *j = JAZZ_VEC_SLOT(q->joins);
    j->table = table;
    j->on_column = on_column;
    j->target = target;
    j->source_column = source_column;
    j->correlated_filters = correlated;
    j->filters = filters;
    j->nested_joins = nested;
}
static jazz_model_OptStr some_str(jz_str s) { return (jazz_model_OptStr){true, s}; }
static jazz_model_Query query_from(jazz_model_TableName table) { return Q(Query__from)(jz_str_from_slice(tn(table)), (jazz_model_OptStr){0}); }
static jazz_model_Query false_query(jazz_model_TableName table) {
    jazz_model_Query q = query_from(table);
    Q(Query__filter)(&q, Q(any_of)((jazz_model_PredicateVec){0}));
    return q;
}

static jz_err convert_policy_predicate(jazz_model_TableName table, jz_slice path, const jazz_model_PolicyExpr *expr, jazz_model_Predicate *out);
static jz_err append_policy_clause(const jazz_model_public_Schema *schema, const jazz_model_public_TableSchema *table_schema,
                                   jazz_model_TableName table, jz_slice path, jazz_model_Query query, const jazz_model_PolicyExpr *expr,
                                   bool native_select_inherits, ExpansionPath *expansion, jazz_model_Query *out);
static jz_err append_inherited_policy(const jazz_model_public_Schema *schema, const jazz_model_public_TableSchema *table_schema,
                                      jazz_model_TableName table, jz_slice path, jazz_model_Query query, u8 operation, const jz_str *via_column,
                                      bool has_max_depth, u32 max_depth, bool native_select_inherits, ExpansionPath *expansion,
                                      jazz_model_Query *out);
static jz_err append_exists_rel_policy_clause(const jazz_model_public_Schema *schema, jazz_model_TableName table, jz_slice path,
                                              jazz_model_Query query, const jazz_model_RelExpr *rel, jazz_model_Query *out);
static jz_err validate_exists_rel_policy_join_conditions(jazz_model_TableName table, jz_slice path, const jazz_model_PolicyExpr *expr);

/* Convert a policy for a location where native SELECT inherits may or may not be representable. */
static jz_err convert_policy_with_native_select_inherits(const jazz_model_public_Schema *schema, const jazz_model_public_TableSchema *table_schema,
                                                         jazz_model_TableName table, jz_slice path, const jazz_model_PolicyExpr *expr,
                                                         bool native_select_inherits, ExpansionPath *expansion, jazz_model_Query *out) {
    /* Do this before choosing a lowering path: an ExistsRel nested below a boolean operator must
     * not escape this validation merely because that operator is rejected later. */
    JZ_TRY(validate_exists_rel_policy_join_conditions(table, path, expr));
    /* A fresh query has no earlier alternatives, so appending the policy is its standalone conversion. */
    return append_policy_clause(schema, table_schema, table, path, query_from(table), expr, native_select_inherits, expansion, out);
}

static jz_err convert_policy(const jazz_model_public_Schema *schema, const jazz_model_public_TableSchema *table_schema, jazz_model_TableName table,
                             jz_slice path, const jazz_model_PolicyExpr *expr, jazz_model_Query *out) {
    ExpansionPath expansion = {0};
    jz_err e = convert_policy_with_native_select_inherits(schema, table_schema, table, path, expr, true, &expansion, out);
    jz_vec_drop(&expansion, sizeof(ExpansionKey), NULL);
    return e;
}

static jz_err convert_optional_policy(const jazz_model_public_Schema *schema, const jazz_model_public_TableSchema *table_schema,
                                      jazz_model_TableName table, const char *path, const jazz_model_PolicyExpr *expr, jazz_model_Query **out) {
    *out = NULL;
    if (!expr) return JZ_OK;
    jazz_model_Query q;
    JZ_TRY(convert_policy(schema, table_schema, table, jz_slice_of(path, (u32)strlen(path)), expr, &q));
    *out = JZ_NEW(jazz_model_Query);
    **out = q;
    return JZ_OK;
}

/* Expand only the current dependency path; sibling branches may reuse a policy. */
static jz_err convert_expanded_inherited_policy(const jazz_model_public_Schema *schema, const jazz_model_public_TableSchema *table_schema,
                                                jazz_model_TableName table, jz_slice path, const jazz_model_PolicyExpr *expr, u8 operation,
                                                ExpansionPath *expansion, jazz_model_Query *out) {
    for (u32 start = 0; start < expansion->len; start++) {
        if (jz_slice_cmp(SL(expansion->ptr[start].table), tn(table)) != 0 || expansion->ptr[start].operation != operation) continue;
        jz_str cycle = {0};
        for (u32 i = start; i <= expansion->len; i++) {
            jz_slice t = i < expansion->len ? SL(expansion->ptr[i].table) : tn(table);
            u8 op = i < expansion->len ? expansion->ptr[i].operation : operation;
            if (i > start) jz_str_push_cstr(&cycle, " -> ");
            jz_str_push(&cycle, (const char *)t.ptr, t.len);
            jz_str_push_char(&cycle, '.');
            PE(Operation__Display__fmt)(&cycle, op);
        }
        jz_err e = perr(table, path, "cyclic policy expansion under INHERITS_REFERENCING is unsupported: %.*s", STRF(cycle));
        jz_str_drop(&cycle);
        return e;
    }
    ExpansionKey key = {jz_str_from_slice(tn(table)), operation};
    JZ_VEC_PUSH(*expansion, key);
    jz_err e = convert_policy_with_native_select_inherits(schema, table_schema, table, path, expr, false, expansion, out);
    expansion->len--;
    jz_str_drop(&expansion->ptr[expansion->len].table);
    return e;
}

static bool policy_requires_branch(const jazz_model_PolicyExpr *expr) {
    switch (expr->tag) {
    case JAZZ_PE_INHERITS: case JAZZ_PE_INHERITS_REFERENCING: case JAZZ_PE_EXISTS: case JAZZ_PE_EXISTS_REL: return true;
    case JAZZ_PE_AND: case JAZZ_PE_OR:
        JZ_VEC_FOR(const jazz_model_PolicyExpr, c, expr->exprs) if (policy_requires_branch(c)) return true;
        return false;
    case JAZZ_PE_NOT: return policy_requires_branch(expr->inner);
    default: return false;
    }
}

static void append_predicate_filters(jazz_model_Query *q, jazz_model_PredicateVec filters) {
    JZ_VEC_FOR(jazz_model_Predicate, f, filters) Q(Query__filter)(q, *f);
    jz_free(filters.ptr);
}

static jz_err predicate_filters_for_expr(jazz_model_TableName table, jz_slice path, const jazz_model_PolicyExpr *expr, jazz_model_PredicateVec *out) {
    if (expr->tag == JAZZ_PE_TRUE) return JZ_OK;
    if (expr->tag == JAZZ_PE_AND) {
        for (u32 i = 0; i < expr->exprs.len; i++) {
            jz_str p = sub_path(path, ".And[%u]", i);
            jz_err e = predicate_filters_for_expr(table, SL(p), &expr->exprs.ptr[i], out);
            jz_str_drop(&p);
            JZ_TRY(e);
        }
        return JZ_OK;
    }
    jazz_model_Predicate p;
    JZ_TRY(convert_policy_predicate(table, path, expr, &p));
    JZ_VEC_PUSH(*out, p);
    return JZ_OK;
}

static jazz_model_PolicyBranch concat_branches(const jazz_model_PolicyBranch *a, const jazz_model_PolicyBranch *b) {
    jazz_model_PolicyBranch r;
    memset(&r, 0, sizeof(r));
#define CAT(field, N, T)                                                                               \
    JZ_VEC_FOR(const T, x, a->field) Q(N##__Clone__clone)(JAZZ_VEC_SLOT(r.field), x);                   \
    JZ_VEC_FOR(const T, x, b->field) Q(N##__Clone__clone)(JAZZ_VEC_SLOT(r.field), x);
    CAT(filters, Predicate, jazz_model_Predicate)
    CAT(joins, JoinVia, jazz_model_JoinVia)
    CAT(reachable, ReachableVia, jazz_model_ReachableVia)
    CAT(inherits, InheritsVia, jazz_model_InheritsVia)
#undef CAT
    return r;
}

static void push_alternatives(jazz_model_Query *combined, jazz_model_Query branch) {
    jazz_model_PolicyBranchVec alts;
    Q(PolicyBranch__alternatives_from_query)(branch, &alts);
    jz_vec_extend(&combined->policy_branches, alts.ptr, alts.len, sizeof(jazz_model_PolicyBranch));
    jz_free(alts.ptr);
}

static jz_err append_inherited_referencing_policy(const jazz_model_public_Schema *schema, jazz_model_TableName table, jz_slice path,
                                                  jazz_model_Query query, u8 operation, const jz_str *source_table, const jz_str *via_column,
                                                  ExpansionPath *expansion, jazz_model_Query *out);
static jz_err append_exists_policy_clause(const jazz_model_public_Schema *schema, jazz_model_TableName table, jz_slice path, jazz_model_Query query,
                                          const jz_str *exists_table, const jazz_model_PolicyExpr *condition, jazz_model_Query *out);

static jz_err append_policy_clause(const jazz_model_public_Schema *schema, const jazz_model_public_TableSchema *table_schema,
                                   jazz_model_TableName table, jz_slice path, jazz_model_Query query, const jazz_model_PolicyExpr *expr,
                                   bool native_select_inherits, ExpansionPath *expansion, jazz_model_Query *out) {
    /* A clause conjoins the entire policy accumulated so far, including every alternative created by
     * an earlier OR. Appending only to the false base would let those alternatives bypass this clause. */
    if (query.policy_branches.len) {
        jazz_model_PolicyBranchVec alternatives;
        Q(PolicyBranch__alternatives_from_query)(query, &alternatives);
        jazz_model_Query combined = false_query(table);
        jz_err e = JZ_OK;
        u32 i = 0;
        for (; i < alternatives.len; i++) {
            jazz_model_PolicyBranch *alt = &alternatives.ptr[i];
            jazz_model_Query branch = query_from(table);
            branch.filters = alt->filters;
            branch.joins = alt->joins;
            branch.reachable = alt->reachable;
            branch.inherits = alt->inherits;
            memset(alt, 0, sizeof(*alt));
            jazz_model_Query appended;
            e = append_policy_clause(schema, table_schema, table, path, branch, expr, native_select_inherits, expansion, &appended);
            if (JZ_IS_ERR(e)) break;
            push_alternatives(&combined, appended);
        }
        VDROP(alternatives, PolicyBranch);
        if (JZ_IS_ERR(e)) { Q(Query__drop)(&combined); return e; }
        *out = combined;
        return JZ_OK;
    }
    switch (expr->tag) {
    case JAZZ_PE_AND: {
        jazz_model_Query q = query;
        for (u32 i = 0; i < expr->exprs.len; i++) {
            jz_str p = sub_path(path, ".And[%u]", i);
            jazz_model_Query next;
            jz_err e = append_policy_clause(schema, table_schema, table, SL(p), q, &expr->exprs.ptr[i], native_select_inherits, expansion, &next);
            jz_str_drop(&p);
            JZ_TRY(e);
            q = next;
        }
        *out = q;
        return JZ_OK;
    }
    case JAZZ_PE_INHERITS:
        return append_inherited_policy(schema, table_schema, table, path, query, expr->op, &expr->column, expr->has_max_depth, expr->max_depth,
                                       native_select_inherits, expansion, out);
    case JAZZ_PE_INHERITS_REFERENCING:
        return append_inherited_referencing_policy(schema, table, path, query, expr->op, &expr->table, &expr->column, expansion, out);
    case JAZZ_PE_EXISTS: return append_exists_policy_clause(schema, table, path, query, &expr->table, expr->inner, out);
    case JAZZ_PE_EXISTS_REL: return append_exists_rel_policy_clause(schema, table, path, query, &expr->rel, out);
    case JAZZ_PE_OR:
        if (policy_requires_branch(expr)) {
            jazz_model_PolicyBranchVec existing_branches;
            Q(PolicyBranch__alternatives_from_query)(query, &existing_branches);
            jazz_model_Query q = false_query(table);
            jz_err e = JZ_OK;
            for (u32 i = 0; i < expr->exprs.len && !JZ_IS_ERR(e); i++) {
                jz_str p = sub_path(path, ".Or[%u]", i);
                jazz_model_Query branch_query;
                e = convert_policy_with_native_select_inherits(schema, table_schema, table, SL(p), &expr->exprs.ptr[i], native_select_inherits,
                                                               expansion, &branch_query);
                jz_str_drop(&p);
                if (JZ_IS_ERR(e)) break;
                jazz_model_PolicyBranchVec branches;
                Q(PolicyBranch__alternatives_from_query)(branch_query, &branches);
                JZ_VEC_FOR(const jazz_model_PolicyBranch, branch, branches)
                    JZ_VEC_FOR(const jazz_model_PolicyBranch, ex, existing_branches)
                        JZ_VEC_PUSH(q.policy_branches, concat_branches(ex, branch));
                VDROP(branches, PolicyBranch);
            }
            VDROP(existing_branches, PolicyBranch);
            if (JZ_IS_ERR(e)) { Q(Query__drop)(&q); return e; }
            *out = q;
            return JZ_OK;
        }
        /* fallthrough */
    default: {
        jazz_model_PredicateVec filters = {0};
        jz_err e = predicate_filters_for_expr(table, path, expr, &filters);
        if (JZ_IS_ERR(e)) { VDROP(filters, Predicate); Q(Query__drop)(&query); return e; }
        append_predicate_filters(&query, filters);
        *out = query;
        return JZ_OK;
    }
    }
}

static const jazz_model_PolicyExpr *source_operation_policy(const jazz_model_public_TableSchema *t, u8 operation) {
    switch (operation) {
    case JAZZ_OP_SELECT: return t->policies.select.using_;
    case JAZZ_OP_INSERT: return t->policies.insert.with_check;
    case JAZZ_OP_UPDATE: return t->policies.update.using_ ? t->policies.update.using_ : t->policies.update.with_check;
    default: return t->policies.delete_.using_;
    }
}

static u8 convert_inherits_operation(u8 operation) {
    static const u8 OPS[] = {JAZZ_INHERITS_SELECT, JAZZ_INHERITS_INSERT, JAZZ_INHERITS_UPDATE, JAZZ_INHERITS_DELETE};
    return OPS[operation];
}

static jz_err append_inherited_referencing_policy_branches(jazz_model_Query query, const jz_str *source_table, const jz_str *via_column,
                                                           jazz_model_Query source_query, jazz_model_Query *out) {
    Q(Query__filter)(&query, Q(any_of)((jazz_model_PredicateVec){0}));
    jazz_model_PolicyBranchVec branches;
    Q(PolicyBranch__alternatives_from_query)(source_query, &branches);
    jz_err e = JZ_OK;
    JZ_VEC_FOR(jazz_model_PolicyBranch, branch, branches) {
        if (branch->reachable.len) {
            e = err(jz_format("$.%.*s.InheritsReferencing", STRF(*source_table)),
                    "core schema policies do not support INHERITS_REFERENCING through reachability yet");
            break;
        }
        jazz_model_Query branch_query = Q(Query__from)(jz_str_clone(&query.table), (jazz_model_OptStr){0});
        push_join(&branch_query, jz_str_clone(source_table), jz_str_clone(via_column), JAZZ_JOIN_TARGET_COLUMN, (jazz_model_OptStr){0},
                  (jazz_model_JoinCorrelationVec){0}, branch->filters, branch->joins);
        memset(&branch->filters, 0, sizeof(branch->filters));
        memset(&branch->joins, 0, sizeof(branch->joins));
        push_alternatives(&query, branch_query);
    }
    VDROP(branches, PolicyBranch);
    if (JZ_IS_ERR(e)) { Q(Query__drop)(&query); return e; }
    *out = query;
    return JZ_OK;
}

static const jazz_model_public_TableSchema *schema_get(const jazz_model_public_Schema *schema, const jz_str *name) {
    jazz_model_TableName k = PS(TableName__new)(jz_str_clone(name));
    return jz_btree_get(&schema->tables, &k);
}

static jz_err append_inherited_referencing_policy(const jazz_model_public_Schema *schema, jazz_model_TableName table, jz_slice path,
                                                  jazz_model_Query query, u8 operation, const jz_str *source_table, const jz_str *via_column,
                                                  ExpansionPath *expansion, jazz_model_Query *out) {
    jz_err e = JZ_OK;
    jazz_model_TableName source_table_name = PS(TableName__new)(jz_str_clone(source_table));
    const jazz_model_public_TableSchema *source_schema = jz_btree_get(&schema->tables, &source_table_name);
    if (!source_schema) { e = perr(table, path, "INHERITS_REFERENCING source_table '%.*s' was not found", STRF(*source_table)); goto fail; }
    const jazz_model_ColumnDescriptor *column = NULL;
    JZ_VEC_FOR(const jazz_model_ColumnDescriptor, c, source_schema->columns.columns) if (jz_str_eq(c->name, via_column)) { column = c; break; }
    if (!column) {
        e = perr(table, path, "INHERITS_REFERENCING via_column '%.*s' was not found on source_table '%.*s'", STRF(*via_column), STRF(*source_table));
        goto fail;
    }
    if (!column->references) { e = perr(table, path, "INHERITS_REFERENCING via_column '%.*s' has no FK reference", STRF(*via_column)); goto fail; }
    if (column->references != table && !jz_str_eq(column->references, table)) {
        e = perr(table, path, "INHERITS_REFERENCING via_column '%.*s' references '%.*s', expected '%.*s'", STRF(*via_column), TNF(column->references),
                 TNF(table));
        goto fail;
    }
    const jazz_model_PolicyExpr *source_policy = source_operation_policy(source_schema, operation);
    if (!source_policy) {
        jz_str op = {0};
        PE(Operation__Debug__fmt)(&op, operation);
        e = perr(table, path, "INHERITS_REFERENCING source_table '%.*s' has no %.*s policy", STRF(*source_table), STRF(op));
        jz_str_drop(&op);
        goto fail;
    }
    jz_str source_path = sub_path(path, ".InheritsReferencing[%.*s]", STRF(*source_table));
    jazz_model_Predicate source_filter;
    e = convert_policy_predicate(source_table_name, SL(source_path), source_policy, &source_filter);
    if (!JZ_IS_ERR(e)) {
        jz_str_drop(&source_path);
        jazz_model_PredicateVec filters = {0};
        JZ_VEC_PUSH(filters, source_filter);
        push_join(&query, jz_str_clone(source_table), jz_str_clone(via_column), JAZZ_JOIN_TARGET_COLUMN, (jazz_model_OptStr){0},
                  (jazz_model_JoinCorrelationVec){0}, filters, (jazz_model_JoinViaVec){0});
        *out = query;
        return JZ_OK;
    }
    if (!policy_requires_branch(source_policy)) { jz_str_drop(&source_path); goto fail; }
    jz_err_drop(&e);
    jazz_model_Query source_query;
    e = convert_expanded_inherited_policy(schema, source_schema, source_table_name, SL(source_path), source_policy, operation, expansion,
                                          &source_query);
    jz_str_drop(&source_path);
    if (JZ_IS_ERR(e)) goto fail;
    return append_inherited_referencing_policy_branches(query, source_table, via_column, source_query, out);
fail:
    Q(Query__drop)(&query);
    return e;
}

static void collect_policy_conjuncts(const jazz_model_PolicyExpr *expr, JZ_VEC(const jazz_model_PolicyExpr *) *output) {
    if (expr->tag == JAZZ_PE_AND) {
        JZ_VEC_FOR(const jazz_model_PolicyExpr, c, expr->exprs) collect_policy_conjuncts(c, (void *)output);
        return;
    }
    JZ_VEC_PUSH(*output, expr);
}

static jazz_model_JoinVia uncorrelated_exists_join(jz_str table, jazz_model_PredicateVec filters, jazz_model_JoinViaVec nested_joins) {
    jazz_model_JoinVia j;
    memset(&j, 0, sizeof(j));
    j.table = table;
    j.target = JAZZ_JOIN_TARGET_UNCORRELATED;
    j.filters = filters;
    j.nested_joins = nested_joins;
    return j;
}

static jz_err append_exists_policy_clause(const jazz_model_public_Schema *schema, jazz_model_TableName table, jz_slice path, jazz_model_Query query,
                                          const jz_str *exists_table, const jazz_model_PolicyExpr *condition, jazz_model_Query *out) {
    jazz_model_TableName exists_table_name = PS(TableName__new)(jz_str_clone(exists_table));
    if (!jz_btree_contains(&schema->tables, &exists_table_name)) {
        Q(Query__drop)(&query);
        return perr(table, path, "EXISTS references unknown table '%.*s'", STRF(*exists_table));
    }
    jazz_model_JoinCorrelationVec outer_correlations = {0};
    jazz_model_PredicateVec filters = {0};
    typedef struct { u32 index; const jazz_model_PolicyExpr *expr; } Nested;
    JZ_VEC(Nested) nested_exists = {0}, nested_exists_rel = {0};
    JZ_VEC(const jazz_model_PolicyExpr *) conditions = {0};
    collect_policy_conjuncts(condition, (void *)&conditions);
    jz_err e = JZ_OK;
    for (u32 index = 0; index < conditions.len; index++) {
        const jazz_model_PolicyExpr *x = conditions.ptr[index];
        if (x->tag == JAZZ_PE_CMP && x->op == JAZZ_CMP_EQ && x->pvalue.tag == JAZZ_POLVAL_SESSION_REF && x->pvalue.path.len == 2 &&
            jz_str_eq_cstr(&x->pvalue.path.ptr[0], "__jazz_outer_row")) {
            jazz_model_JoinCorrelation *c = JAZZ_VEC_SLOT(outer_correlations);
            c->join_column = jz_str_clone(&x->column);
            c->source_column = jz_str_clone(&x->pvalue.path.ptr[1]);
        } else if (x->tag == JAZZ_PE_EXISTS) {
            Nested n = {index, x};
            JZ_VEC_PUSH(nested_exists, n);
        } else if (x->tag == JAZZ_PE_EXISTS_REL) {
            Nested n = {index, x};
            JZ_VEC_PUSH(nested_exists_rel, n);
        } else {
            jz_str p = sub_path(path, ".Exists[%u]", index);
            jazz_model_Predicate pred;
            e = convert_policy_predicate(exists_table_name, SL(p), x, &pred);
            jz_str_drop(&p);
            if (JZ_IS_ERR(e)) break;
            JZ_VEC_PUSH(filters, pred);
        }
    }
    jz_vec_drop(&conditions, sizeof(void *), NULL);
    if (JZ_IS_ERR(e)) {
        VDROP(outer_correlations, JoinCorrelation);
        VDROP(filters, Predicate);
        Q(Query__drop)(&query);
        goto done;
    }
    if (outer_correlations.len == 0) {
        JZ_VEC_PUSH(query.joins, uncorrelated_exists_join(jz_str_clone(exists_table), filters, (jazz_model_JoinViaVec){0}));
        jz_vec_drop(&outer_correlations, sizeof(jazz_model_JoinCorrelation), NULL);
    } else {
        u32 primary_index = 0;
        for (u32 i = 0; i < outer_correlations.len; i++) if (jz_str_eq_cstr(&outer_correlations.ptr[i].join_column, "id")) { primary_index = i; break; }
        jazz_model_JoinCorrelation primary;
        jz_vec_remove(&outer_correlations, primary_index, &primary, sizeof(jazz_model_JoinCorrelation));
        if (jz_str_eq_cstr(&primary.join_column, "id")) {
            jz_str_drop(&primary.join_column);
            push_join(&query, jz_str_clone(exists_table), jz_str_from("id"), JAZZ_JOIN_TARGET_ROW_ID, some_str(primary.source_column),
                      outer_correlations, filters, (jazz_model_JoinViaVec){0});
        } else {
            push_join(&query, jz_str_clone(exists_table), primary.join_column, JAZZ_JOIN_TARGET_COLUMN, some_str(primary.source_column),
                      outer_correlations, filters, (jazz_model_JoinViaVec){0});
        }
    }
    /* Legacy Exists has one outer-row scope, even when an all-of nests another EXISTS. Each nested
     * existential is an additional proof about the protected row, lowered through the same path. */
    for (u32 i = 0; i < nested_exists.len && !JZ_IS_ERR(e); i++) {
        jz_str p = sub_path(path, ".Exists[%u]", nested_exists.ptr[i].index);
        jazz_model_Query next;
        e = append_exists_policy_clause(schema, table, SL(p), query, &nested_exists.ptr[i].expr->table, nested_exists.ptr[i].expr->inner, &next);
        jz_str_drop(&p);
        if (!JZ_IS_ERR(e)) query = next;
    }
    for (u32 i = 0; i < nested_exists_rel.len && !JZ_IS_ERR(e); i++) {
        jz_str p = sub_path(path, ".Exists[%u]", nested_exists_rel.ptr[i].index);
        jazz_model_Query next;
        e = append_exists_rel_policy_clause(schema, table, SL(p), query, &nested_exists_rel.ptr[i].expr->rel, &next);
        jz_str_drop(&p);
        if (!JZ_IS_ERR(e)) query = next;
    }
    if (!JZ_IS_ERR(e)) *out = query;
done:
    jz_vec_drop(&nested_exists, sizeof(Nested), NULL);
    jz_vec_drop(&nested_exists_rel, sizeof(Nested), NULL);
    return e;
}

/* ================================================================ ExistsRel lowering */
enum { LRV_NONE, LRV_OPERAND, LRV_OUTER_ROW, LRV_FRONTIER_ROW };
typedef struct LoweredRelPredicate {
    jazz_model_Predicate predicate;
    jazz_model_OptStr column;
    u8 value;                    /* LRV_* */
    jazz_model_Operand operand;  /* Operand */
    jz_str outer;                /* OuterRow */
} LoweredRelPredicate;
typedef JZ_VEC(LoweredRelPredicate) LoweredRelPredicateVec;
static void lrp_drop(LoweredRelPredicate *p) {
    Q(Predicate__drop)(&p->predicate);
    jazz_model__OptStr__drop(&p->column);
    if (p->value == LRV_OPERAND) Q(Operand__drop)(&p->operand);
    if (p->value == LRV_OUTER_ROW) jz_str_drop(&p->outer);
}
static void lrp_drop_v(void *p) { lrp_drop(p); }
static LoweredRelPredicate lrp_bare(jazz_model_Predicate predicate) {
    LoweredRelPredicate p;
    memset(&p, 0, sizeof(p));
    p.predicate = predicate;
    return p;
}
/* A predicate over `column` with no bound value. */
static LoweredRelPredicate lrp_on(jz_slice column, jazz_model_Predicate predicate) {
    LoweredRelPredicate p = lrp_bare(predicate);
    p.column = some_str(jz_str_from_slice(column));
    return p;
}

typedef struct PendingReachable {
    jazz_model_Operand from;
    jazz_model_ReachableSeed *seed; /* NULL = None */
    jz_str edge_table, edge_member_column, edge_parent_column;
    jazz_model_PredicateVec edge_filters;
    u64 max_depth;
} PendingReachable;
static void pending_drop(PendingReachable *p) {
    Q(Operand__drop)(&p->from);
    if (p->seed) { Q(ReachableSeed__drop)(p->seed); jz_free(p->seed); }
    jz_str_drop(&p->edge_table); jz_str_drop(&p->edge_member_column); jz_str_drop(&p->edge_parent_column);
    VDROP(p->edge_filters, Predicate);
}

typedef JZ_VEC(u32) RouteVec;
/* Compiler-only paths into `joins`; None marks an ambiguous repeated scope. */
typedef struct ScopePath { bool some; RouteVec route; } ScopePath;
static void scope_path_drop_v(void *p) { jz_vec_drop(&((ScopePath *)p)->route, sizeof(u32), NULL); }
static const jz_btree_desc SCOPE_PATHS_DESC = {sizeof(jz_str), sizeof(ScopePath), jz_cmp_str, jazz_model__str_drop_v, scope_path_drop_v};

typedef struct LoweredRel {
    jz_btree scope_paths;
    /* Row-producing relations name their output table. Scalar-key gathers do not. */
    jazz_model_OptStr table;
    LoweredRelPredicateVec filters;
    jazz_model_JoinViaVec joins;
    jazz_model_ReachableViaVec reachable;
    PendingReachable *pending_reachable; /* NULL = None */
} LoweredRel;
static void lowered_init(LoweredRel *l) { memset(l, 0, sizeof(*l)); jz_btree_init(&l->scope_paths, &SCOPE_PATHS_DESC); }
static void lowered_drop(LoweredRel *l) {
    jz_btree_drop(&l->scope_paths);
    jazz_model__OptStr__drop(&l->table);
    jz_vec_drop(&l->filters, sizeof(LoweredRelPredicate), lrp_drop_v);
    VDROP(l->joins, JoinVia);
    VDROP(l->reachable, ReachableVia);
    if (l->pending_reachable) { pending_drop(l->pending_reachable); jz_free(l->pending_reachable); l->pending_reachable = NULL; }
}
static jazz_model_PredicateVec lowered_predicates(LoweredRelPredicateVec *filters) {
    jazz_model_PredicateVec out = {0};
    JZ_VEC_FOR(LoweredRelPredicate, f, *filters) {
        JZ_VEC_PUSH(out, f->predicate);
        f->predicate = Q(all_of)((jazz_model_PredicateVec){0});
        lrp_drop(f);
    }
    jz_free(filters->ptr);
    memset(filters, 0, sizeof(*filters));
    return out;
}

static bool label_ok(const jz_str *label, jz_btree *labels) {
    if (label->len == 0 || label->len > 4096 || memchr(label->ptr, 0, label->len)) return false;
    jz_str k = jz_str_clone(label);
    return !jz_btree_insert(labels, &k, NULL, NULL);
}

static jz_err validate_exists_rel_join_conditions(jazz_model_TableName table, jz_slice path, const jazz_model_RelExpr *rel) {
    switch (rel->tag) {
    case JAZZ_REL_TABLE_SCAN: return JZ_OK;
    case JAZZ_REL_FILTER: case JAZZ_REL_PROJECT: return validate_exists_rel_join_conditions(table, path, rel->input);
    case JAZZ_REL_UNION: {
        jz_btree labels;
        jz_btree_init(&labels, &jazz_model__STR_SET_DESC);
        jz_err e = JZ_OK;
        JZ_VEC_FOR(const jazz_model_RelUnionArm, arm, rel->inputs) {
            if (!label_ok(&arm->label, &labels)) {
                e = perr(table, path, "core schema ExistsRel union labels must be unique, NUL-free, and 1..=4096 bytes");
                break;
            }
            e = validate_exists_rel_join_conditions(table, path, &arm->input);
            if (JZ_IS_ERR(e)) break;
        }
        jz_btree_drop(&labels);
        return e;
    }
    case JAZZ_REL_JOIN:
        if (rel->on.len > 1) return perr(table, path, "core schema ExistsRel joins support exactly one column equality");
        JZ_TRY(validate_exists_rel_join_conditions(table, path, rel->input));
        return validate_exists_rel_join_conditions(table, path, rel->right);
    default: /* Gather */
        JZ_TRY(validate_exists_rel_join_conditions(table, path, rel->input));
        return validate_exists_rel_join_conditions(table, path, rel->right);
    }
}

static jz_err validate_exists_rel_policy_join_conditions(jazz_model_TableName table, jz_slice path, const jazz_model_PolicyExpr *expr) {
    switch (expr->tag) {
    case JAZZ_PE_EXISTS_REL: return validate_exists_rel_join_conditions(table, path, &expr->rel);
    case JAZZ_PE_AND: case JAZZ_PE_OR:
        for (u32 i = 0; i < expr->exprs.len; i++) {
            jz_str p = sub_path(path, expr->tag == JAZZ_PE_AND ? ".And[%u]" : ".Or[%u]", i);
            jz_err e = validate_exists_rel_policy_join_conditions(table, SL(p), &expr->exprs.ptr[i]);
            jz_str_drop(&p);
            JZ_TRY(e);
        }
        return JZ_OK;
    case JAZZ_PE_NOT: {
        jz_str p = sub_path(path, ".Not");
        jz_err e = validate_exists_rel_policy_join_conditions(table, SL(p), expr->inner);
        jz_str_drop(&p);
        return e;
    }
    default: return JZ_OK;
    }
}

/* Outer references inside boolean predicates must not be mistaken for an independent proof. */
static bool value_is_outer(const jazz_model_RelValueRef *v) {
    return v->tag == JAZZ_RVR_OUTER_COLUMN || (v->tag == JAZZ_RVR_ROW_ID && v->row_id == JAZZ_ROWID_OUTER);
}
static bool predicate_has_outer(const jazz_model_RelPredicateExpr *p) {
    switch (p->tag) {
    case JAZZ_RPE_CMP: case JAZZ_RPE_CONTAINS: return value_is_outer(&p->right);
    case JAZZ_RPE_IN: JZ_VEC_FOR(const jazz_model_RelValueRef, v, p->values) if (value_is_outer(v)) return true; return false;
    case JAZZ_RPE_AND: case JAZZ_RPE_OR: JZ_VEC_FOR(const jazz_model_RelPredicateExpr, c, p->items) if (predicate_has_outer(c)) return true; return false;
    case JAZZ_RPE_NOT: case JAZZ_RPE_ENUM_MATCH: return predicate_has_outer(p->inner);
    default: return false;
    }
}
static bool rel_has_outer_references(const jazz_model_RelExpr *rel) {
    switch (rel->tag) {
    case JAZZ_REL_TABLE_SCAN: return false;
    case JAZZ_REL_FILTER: return rel_has_outer_references(rel->input) || predicate_has_outer(&rel->predicate);
    case JAZZ_REL_PROJECT: return rel_has_outer_references(rel->input);
    case JAZZ_REL_JOIN: return rel_has_outer_references(rel->input) || rel_has_outer_references(rel->right);
    case JAZZ_REL_UNION: JZ_VEC_FOR(const jazz_model_RelUnionArm, arm, rel->inputs) if (rel_has_outer_references(&arm->input)) return true; return false;
    default: return true;
    }
}
static bool join_has_outer_correlations(const jazz_model_JoinVia *j) {
    if (j->correlated_filters.len) return true;
    JZ_VEC_FOR(const jazz_model_JoinVia, n, j->nested_joins) if (join_has_outer_correlations(n)) return true;
    return false;
}
static bool has_outer_row_filter(const LoweredRel *l) {
    JZ_VEC_FOR(const LoweredRelPredicate, f, l->filters) if (f->value == LRV_OUTER_ROW) return true;
    return false;
}

/* Orient an inner-join tree from the source correlated to the protected row. Reversing an equality
 * edge preserves existence, provided each source keeps its filters and every off-path branch stays attached. */
static jz_err root_exists_rel_at_correlation(jazz_model_TableName table, jz_slice path, LoweredRel *lowered) {
    /* Gather uses a separate access/frontier lowering path. */
    if (lowered->reachable.len || lowered->pending_reachable) return JZ_OK;
    while (!has_outer_row_filter(lowered)) {
        u32 index = 0;
        bool found = false;
        for (; index < lowered->joins.len; index++) if (join_has_outer_correlations(&lowered->joins.ptr[index])) { found = true; break; }
        if (!found) break;
        jazz_model_JoinVia next;
        jz_vec_remove(&lowered->joins, index, &next, sizeof(jazz_model_JoinVia));
        if (!next.source_column.some) {
            Q(JoinVia__drop)(&next);
            return perr(table, path, "core schema ExistsRel nested join is missing its source column");
        }
        jz_str source_column = next.source_column.s;
        next.source_column = (jazz_model_OptStr){0};
        if (!lowered->table.some) {
            jz_str_drop(&source_column);
            Q(JoinVia__drop)(&next);
            return perr(table, path, "core schema ExistsRel correlation requires a row-producing relation");
        }
        jz_str previous_table = lowered->table.s;
        lowered->table.s = next.table;
        next.table = (jz_str){0};
        jazz_model_JoinVia reverse;
        memset(&reverse, 0, sizeof(reverse));
        reverse.table = previous_table;
        reverse.target = jz_str_eq_cstr(&source_column, "id") ? JAZZ_JOIN_TARGET_ROW_ID : JAZZ_JOIN_TARGET_COLUMN;
        reverse.on_column = source_column;
        reverse.source_column = some_str(next.on_column);
        next.on_column = (jz_str){0};
        reverse.filters = lowered_predicates(&lowered->filters);
        reverse.nested_joins = lowered->joins;
        memset(&lowered->joins, 0, sizeof(lowered->joins));
        JZ_VEC_FOR(jazz_model_Predicate, f, next.filters) JZ_VEC_PUSH(lowered->filters, lrp_bare(*f));
        jz_free(next.filters.ptr);
        next.filters = (jazz_model_PredicateVec){0};
        JZ_VEC_FOR(jazz_model_JoinCorrelation, c, next.correlated_filters) {
            LoweredRelPredicate p = lrp_bare(Q(all_of)((jazz_model_PredicateVec){0}));
            p.column = some_str(c->join_column);
            p.value = LRV_OUTER_ROW;
            p.outer = c->source_column;
            JZ_VEC_PUSH(lowered->filters, p);
        }
        jz_free(next.correlated_filters.ptr);
        JZ_VEC_PUSH(next.nested_joins, reverse);
        lowered->joins = next.nested_joins;
        /* Scope routing is complete; these paths described the old orientation. */
        jz_btree_clear(&lowered->scope_paths);
    }
    return JZ_OK;
}

static jz_err lower_exists_rel(const jazz_model_public_Schema *schema, jazz_model_TableName table, jz_slice path, const jazz_model_RelExpr *rel,
                               LoweredRel *out);

static jz_err append_exists_rel_policy_clause(const jazz_model_public_Schema *schema, jazz_model_TableName table, jz_slice path,
                                              jazz_model_Query query, const jazz_model_RelExpr *rel, jazz_model_Query *out) {
    LoweredRel lowered;
    jz_err e = lower_exists_rel(schema, table, path, rel, &lowered);
    if (JZ_IS_ERR(e)) { Q(Query__drop)(&query); return e; }
    e = root_exists_rel_at_correlation(table, path, &lowered);
    if (JZ_IS_ERR(e)) goto fail;
    i64 correlation_index = -1;
    for (u32 i = 0; i < lowered.filters.len; i++) {
        const LoweredRelPredicate *f = &lowered.filters.ptr[i];
        if (f->value == LRV_OUTER_ROW && f->column.some && jz_str_eq_cstr(&f->column.s, "id")) { correlation_index = i; break; }
    }
    if (correlation_index < 0)
        for (u32 i = 0; i < lowered.filters.len; i++) if (lowered.filters.ptr[i].value == LRV_OUTER_ROW) { correlation_index = i; break; }
    if (correlation_index < 0) {
        bool outer_joins = false;
        JZ_VEC_FOR(const jazz_model_JoinVia, j, lowered.joins) if (join_has_outer_correlations(j)) outer_joins = true;
        if (rel_has_outer_references(rel) || lowered.reachable.len || lowered.pending_reachable || outer_joins) {
            e = perr(table, path, "uncorrelated ExistsRel cannot contain unresolved outer references or reachability");
            goto fail;
        }
        if (!lowered.table.some) { e = perr(table, path, "uncorrelated ExistsRel requires a table relation"); goto fail; }
        JZ_VEC_PUSH(query.joins, uncorrelated_exists_join(lowered.table.s, lowered_predicates(&lowered.filters), lowered.joins));
        lowered.table = (jazz_model_OptStr){0};
        memset(&lowered.joins, 0, sizeof(lowered.joins));
        lowered_drop(&lowered);
        *out = query;
        return JZ_OK;
    }
    LoweredRelPredicate correlation;
    jz_vec_remove(&lowered.filters, (u32)correlation_index, &correlation, sizeof(LoweredRelPredicate));
    if (!correlation.column.some) { lrp_drop(&correlation); e = perr(table, path, "core schema ExistsRel policies must correlate a concrete column"); goto fail; }
    if (correlation.value != LRV_OUTER_ROW) {
        lrp_drop(&correlation);
        e = perr(table, path, "core schema ExistsRel policies must correlate to an outer row reference");
        goto fail;
    }
    jz_str correlation_column = jz_str_clone(&correlation.column.s);
    jz_str source_column = jz_str_clone(&correlation.outer);
    lrp_drop(&correlation);

    /* An ExistsRel can correlate more than one key from the protected row. Keep every extra outer-row
     * equality as part of the join predicate. */
    jazz_model_JoinCorrelationVec correlated_filters = {0};
    jazz_model_PredicateVec remaining_filters = {0};
    JZ_VEC_FOR(const LoweredRelPredicate, f, lowered.filters) {
        if (f->column.some && f->value == LRV_OUTER_ROW) {
            jazz_model_JoinCorrelation *c = JAZZ_VEC_SLOT(correlated_filters);
            c->join_column = jz_str_clone(&f->column.s);
            c->source_column = jz_str_clone(&f->outer);
        } else {
            Q(Predicate__Clone__clone)(JAZZ_VEC_SLOT(remaining_filters), &f->predicate);
        }
    }
    JZ_VEC_FOR(jazz_model_ReachableVia, r, lowered.reachable) {
        if (jz_str_eq_cstr(&r->access_row_column, "__pending_outer_row")) {
            jz_str_drop(&r->access_row_column);
            r->access_row_column = jz_str_clone(&correlation_column);
            JZ_VEC_FOR(const jazz_model_Predicate, p, remaining_filters) Q(Predicate__Clone__clone)(JAZZ_VEC_SLOT(r->access_filters), p);
        }
    }
    if (lowered.pending_reachable) {
        PendingReachable *pending = lowered.pending_reachable;
        jazz_model_ReachableVia *r = JAZZ_VEC_SLOT(lowered.reachable);
        r->access_table = jz_str_from_slice(tn(table));
        r->access_row_column = jz_str_from("id");
        r->access_team_column = jz_str_clone(&source_column);
        r->access_team_target = jz_str_eq_cstr(&source_column, "id") ? JAZZ_JOIN_TARGET_ROW_ID : JAZZ_JOIN_TARGET_COLUMN;
        r->from = pending->from;
        jz_vec_clone(&r->access_filters, &remaining_filters, sizeof(jazz_model_Predicate), Q(Predicate__Clone__clone_v));
        r->edge_table = pending->edge_table;
        r->edge_member_column = pending->edge_member_column;
        r->edge_parent_column = pending->edge_parent_column;
        r->edge_filters = pending->edge_filters;
        r->bound.tag = JAZZ_BOUND_MAX_DEPTH;
        r->bound.max_depth = pending->max_depth;
        r->seed = pending->seed;
        jz_free(pending);
        lowered.pending_reachable = NULL;
    }
    if (lowered.reachable.len) {
        jz_vec_extend(&query.reachable, lowered.reachable.ptr, lowered.reachable.len, sizeof(jazz_model_ReachableVia));
        jz_free(lowered.reachable.ptr);
        memset(&lowered.reachable, 0, sizeof(lowered.reachable));
        VDROP(correlated_filters, JoinCorrelation);
        VDROP(remaining_filters, Predicate);
        jz_str_drop(&correlation_column);
        jz_str_drop(&source_column);
        lowered_drop(&lowered);
        *out = query;
        return JZ_OK;
    }
    if (!lowered.table.some) {
        e = perr(table, path, "core schema scalar Gather output must be consumed by an access join");
        goto fail_corr;
    }
    if (lowered.joins.len) {
        /* The oriented root is the row correlated to the protected row; its joins stay nested beneath it.
         * Lift outer correlations on a nested join through its equality edge. */
        JZ_VEC_FOR(jazz_model_JoinVia, nested, lowered.joins) {
            JZ_VEC_FOR(const jazz_model_JoinVia, child, nested->nested_joins) {
                if (child->correlated_filters.len) {
                    e = perr(table, path, "core schema ExistsRel does not yet support outer correlations beyond one nested join");
                    goto fail_corr;
                }
            }
            if (!nested->source_column.some) { e = perr(table, path, "core schema ExistsRel nested join is missing its source column"); goto fail_corr; }
            JZ_VEC_FOR(jazz_model_JoinCorrelation, c, nested->correlated_filters) {
                if (!jz_str_eq(&c->join_column, &nested->on_column)) {
                    e = perr(table, path, "core schema ExistsRel nested outer correlation must use its join key");
                    goto fail_corr;
                }
                jazz_model_JoinCorrelation *lifted = JAZZ_VEC_SLOT(correlated_filters);
                lifted->join_column = jz_str_clone(&nested->source_column.s);
                lifted->source_column = jz_str_clone(&c->source_column);
            }
            VDROP(nested->correlated_filters, JoinCorrelation);
        }
        u8 target = jz_str_eq_cstr(&correlation_column, "id") ? JAZZ_JOIN_TARGET_ROW_ID : JAZZ_JOIN_TARGET_COLUMN;
        push_join(&query, jz_str_clone(&lowered.table.s), correlation_column, target, some_str(source_column), correlated_filters,
                  remaining_filters, lowered.joins);
        memset(&lowered.joins, 0, sizeof(lowered.joins));
        lowered_drop(&lowered);
        *out = query;
        return JZ_OK;
    }
    VDROP(remaining_filters, Predicate);
    jazz_model_PredicateVec filters = lowered_predicates(&lowered.filters);
    if (jz_str_eq_cstr(&correlation_column, "id")) {
        jz_str_drop(&correlation_column);
        push_join(&query, jz_str_clone(&lowered.table.s), jz_str_from("id"), JAZZ_JOIN_TARGET_ROW_ID, some_str(source_column), correlated_filters,
                  filters, (jazz_model_JoinViaVec){0});
    } else {
        push_join(&query, jz_str_clone(&lowered.table.s), correlation_column, JAZZ_JOIN_TARGET_COLUMN, some_str(source_column), correlated_filters,
                  filters, (jazz_model_JoinViaVec){0});
    }
    lowered_drop(&lowered);
    *out = query;
    return JZ_OK;
fail_corr:
    VDROP(correlated_filters, JoinCorrelation);
    VDROP(remaining_filters, Predicate);
    jz_str_drop(&correlation_column);
    jz_str_drop(&source_column);
fail:
    lowered_drop(&lowered);
    Q(Query__drop)(&query);
    return e;
}

/* Resolve a column before dropping its relation scope from the core join tree. */
static jz_err resolve_rel_column_route(jazz_model_TableName table, jz_slice path, const LoweredRel *lowered, const jazz_model_RelColumnRef *column,
                                       RouteVec *out) {
    memset(out, 0, sizeof(*out));
    if (!column->scope.some) return JZ_OK;
    const ScopePath *sp = jz_btree_get(&lowered->scope_paths, &column->scope.s);
    if (!sp) return perr(table, path, "ExistsRel column references unknown scope '%.*s'", STRF(column->scope.s));
    if (!sp->some) return perr(table, path, "ExistsRel column scope '%.*s' is ambiguous; use distinct aliases", STRF(column->scope.s));
    jz_vec_clone(out, &sp->route, sizeof(u32), NULL);
    return JZ_OK;
}

static void collect_rel_predicate_columns(const jazz_model_RelPredicateExpr *p, JZ_VEC(const jazz_model_RelColumnRef *) *columns) {
    switch (p->tag) {
    case JAZZ_RPE_AND: case JAZZ_RPE_OR:
        JZ_VEC_FOR(const jazz_model_RelPredicateExpr, c, p->items) collect_rel_predicate_columns(c, (void *)columns);
        break;
    case JAZZ_RPE_NOT: collect_rel_predicate_columns(p->inner, (void *)columns); break;
    case JAZZ_RPE_TRUE: case JAZZ_RPE_FALSE: break;
    /* Enum payload fields are relative to the matched record, not relation sources. */
    default: { const jazz_model_RelColumnRef *c = &p->left; JZ_VEC_PUSH(*columns, c); break; }
    }
}

static jz_err rel_predicate_to_policy(jazz_model_TableName table, jz_slice path, const jazz_model_RelPredicateExpr *p, LoweredRelPredicateVec *out);

static bool route_eq(const RouteVec *a, const RouteVec *b) { return a->len == b->len && (a->len == 0 || memcmp(a->ptr, b->ptr, a->len * sizeof(u32)) == 0); }

/* Assign conjuncts to their source before converting scoped columns to local names. OR/NOT stay intact. */
static jz_err append_scoped_rel_predicate(jazz_model_TableName table, jz_slice path, LoweredRel *lowered, const jazz_model_RelPredicateExpr *p) {
    if (p->tag == JAZZ_RPE_AND) {
        JZ_VEC_FOR(const jazz_model_RelPredicateExpr, c, p->items) JZ_TRY(append_scoped_rel_predicate(table, path, lowered, c));
        return JZ_OK;
    }
    JZ_VEC(const jazz_model_RelColumnRef *) columns = {0};
    collect_rel_predicate_columns(p, (void *)&columns);
    JZ_VEC(RouteVec) routes = {0};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < columns.len; i++) {
        RouteVec route;
        e = resolve_rel_column_route(table, path, lowered, columns.ptr[i], &route);
        if (JZ_IS_ERR(e)) break;
        bool dup = false;
        JZ_VEC_FOR(const RouteVec, r, routes) if (route_eq(r, &route)) { dup = true; break; }
        if (dup) jz_vec_drop(&route, sizeof(u32), NULL);
        else JZ_VEC_PUSH(routes, route);
    }
    jz_vec_drop(&columns, sizeof(void *), NULL);
    RouteVec route = {0};
    LoweredRelPredicateVec filters = {0};
    if (!JZ_IS_ERR(e) && routes.len > 1) e = perr(table, path, "ExistsRel Boolean filters spanning multiple sources are not supported");
    if (!JZ_IS_ERR(e)) {
        if (routes.len == 1) { route = routes.ptr[0]; routes.len = 0; }
        e = rel_predicate_to_policy(table, path, p, &filters);
    }
    JZ_VEC_FOR(RouteVec, r, routes) jz_vec_drop(r, sizeof(u32), NULL);
    jz_vec_drop(&routes, sizeof(RouteVec), NULL);
    if (JZ_IS_ERR(e)) { jz_vec_drop(&route, sizeof(u32), NULL); return e; }
    if (route.len == 0) {
        jz_vec_extend(&lowered->filters, filters.ptr, filters.len, sizeof(LoweredRelPredicate));
        jz_free(filters.ptr);
        return JZ_OK;
    }
    jazz_model_JoinViaVec *joins = &lowered->joins;
    for (u32 position = 0; position < route.len; position++) {
        jazz_model_JoinVia *join = &joins->ptr[route.ptr[position]];
        if (position + 1 == route.len) {
            JZ_VEC_FOR(LoweredRelPredicate, f, filters) {
                if (f->column.some && f->value == LRV_OUTER_ROW) {
                    jazz_model_JoinCorrelation *c = JAZZ_VEC_SLOT(join->correlated_filters);
                    c->join_column = f->column.s;
                    c->source_column = f->outer;
                    f->column = (jazz_model_OptStr){0};
                    f->value = LRV_NONE;
                    Q(Predicate__drop)(&f->predicate);
                } else {
                    JZ_VEC_PUSH(join->filters, f->predicate);
                    jazz_model__OptStr__drop(&f->column);
                    if (f->value == LRV_OPERAND) Q(Operand__drop)(&f->operand);
                    if (f->value == LRV_OUTER_ROW) jz_str_drop(&f->outer);
                }
            }
            jz_free(filters.ptr);
            jz_vec_drop(&route, sizeof(u32), NULL);
            return JZ_OK;
        }
        joins = &join->nested_joins;
    }
    jz_vec_drop(&filters, sizeof(LoweredRelPredicate), lrp_drop_v);
    jz_vec_drop(&route, sizeof(u32), NULL);
    return JZ_OK;
}

/* ---------------------------------------------------------------- lower_exists_rel */
typedef JZ_VEC(const jazz_model_RelPredicateExpr *) PredRefVec;
typedef struct LoweredRelValue { u8 tag; jazz_model_Operand operand; jz_str outer; } LoweredRelValue;
static void lrv_drop(LoweredRelValue *v) {
    if (v->tag == LRV_OPERAND) Q(Operand__drop)(&v->operand);
    if (v->tag == LRV_OUTER_ROW) jz_str_drop(&v->outer);
}
typedef struct GatherStep {
    jz_str edge_table;
    jazz_model_OptStr output_table;
    jz_str edge_member_column, edge_parent_column;
    jazz_model_PredicateVec edge_filters;
} GatherStep;

static jz_err lower_gather_rel(const jazz_model_public_Schema *schema, jazz_model_TableName table, jz_slice path, const jazz_model_RelExpr *seed,
                               const jazz_model_RelExpr *step, const jazz_model_RelRecursionBound *bound, LoweredRel *out);
static jz_err rel_predicates_to_policy(jazz_model_TableName table, jz_slice path, const PredRefVec *ps, LoweredRelPredicateVec *out);
static jz_err rel_value_to_policy_operand(jazz_model_TableName table, jz_slice path, const jazz_model_RelValueRef *v, LoweredRelValue *out);
static jz_err convert_policy_literal(jazz_model_TableName table, jz_slice path, const jazz_model_public_Value *v, groove_Value *out);
static jz_err convert_session_path_operand(jazz_model_TableName table, jz_slice path, const jazz_model_StrVec *segs, jazz_model_Operand *out);
static jz_err convert_policy_operand(jazz_model_TableName table, jz_slice path, const jazz_model_PolicyValue *v, jazz_model_Operand *out);

static bool opt_scope_is(const jazz_model_OptStr *scope, jz_slice s) { return !scope->some || jz_slice_cmp(SL(scope->s), s) == 0; }
static bool key_is_current_row(const jazz_model_RelKeyRef *k) { return k->tag == JAZZ_KEYREF_ROW_ID && k->row_id == JAZZ_ROWID_CURRENT; }
static void scope_insert_root(jz_btree *paths, jz_str key) {
    ScopePath sp = {true, {0}};
    jz_btree_insert(paths, &key, &sp, NULL);
}

static jz_err lower_exists_rel(const jazz_model_public_Schema *schema, jazz_model_TableName table, jz_slice path, const jazz_model_RelExpr *rel,
                               LoweredRel *out) {
    jz_err e;
    switch (rel->tag) {
    case JAZZ_REL_TABLE_SCAN:
        lowered_init(out);
        scope_insert_root(&out->scope_paths, rel->alias.some ? jz_str_clone(&rel->alias.s) : jz_str_from_slice(tn(rel->table)));
        out->table = some_str(jz_str_from_slice(tn(rel->table)));
        return JZ_OK;
    case JAZZ_REL_FILTER:
        JZ_TRY(lower_exists_rel(schema, table, path, rel->input, out));
        e = append_scoped_rel_predicate(table, path, out, &rel->predicate);
        if (JZ_IS_ERR(e)) lowered_drop(out);
        return e;
    case JAZZ_REL_PROJECT: return lower_exists_rel(schema, table, path, rel->input, out);
    case JAZZ_REL_GATHER:
        /* Reachability walks row ids. A gather keyed on anything else cannot be lowered to it, so it must not
         * be silently treated as one. */
        if (!key_is_current_row(&rel->frontier_key) || rel->dedupe_key.len != 1 || !key_is_current_row(&rel->dedupe_key.ptr[0]))
            return perr(table, path, "Gather keyed on anything but the current row id is not supported in policies yet");
        return lower_gather_rel(schema, table, path, rel->input, rel->right, &rel->bound, out);
    case JAZZ_REL_UNION: return perr(table, path, "core schema ExistsRel policies do not support Union yet");
    default: break;
    }
    /* Join */
    if (rel->join_kind != JAZZ_JOIN_INNER) return perr(table, path, "core schema ExistsRel policies only support inner joins");
    LoweredRel left, right;
    JZ_TRY(lower_exists_rel(schema, table, path, rel->input, &left));
    e = lower_exists_rel(schema, table, path, rel->right, &right);
    if (JZ_IS_ERR(e)) { lowered_drop(&left); return e; }
    RouteVec source_route = {0}, target_route = {0};
    /* Only a Gather on the left feeds the access join below; one on the right would otherwise be dropped,
     * leaving a plain join that authorizes without the reachability check. */
    if (right.pending_reachable) { e = perr(table, path, "Gather on the right side of a join is not supported in policies yet"); goto fail; }
    if (!rel->on.len) { e = perr(table, path, "core schema ExistsRel joins require a column equality"); goto fail; }
    const jazz_model_RelJoinCondition *on = &rel->on.ptr[0];
    e = resolve_rel_column_route(table, path, &left, &on->left, &source_route);
    if (JZ_IS_ERR(e)) goto fail;
    e = resolve_rel_column_route(table, path, &right, &on->right, &target_route);
    if (JZ_IS_ERR(e)) goto fail;
    if (target_route.len) { e = perr(table, path, "core schema ExistsRel join target must be the right relation's root source"); goto fail; }
    u8 target = jz_str_eq_cstr(&on->right.column, "id") ? JAZZ_JOIN_TARGET_ROW_ID : JAZZ_JOIN_TARGET_COLUMN;
    if (left.pending_reachable) {
        if (!jz_str_eq_cstr(&on->left.column, "id")) { e = perr(table, path, "core schema ExistsRel reachable joins must join from reachable id"); goto fail; }
        if (!right.table.some) { e = perr(table, path, "core schema ExistsRel reachable access must be a row-producing relation"); goto fail; }
        PendingReachable *pending = left.pending_reachable;
        left.pending_reachable = NULL;
        jazz_model_ReachableVia reachable;
        memset(&reachable, 0, sizeof(reachable));
        reachable.access_table = jz_str_clone(&right.table.s);
        reachable.access_row_column = jz_str_from("__pending_outer_row");
        reachable.access_team_column = jz_str_clone(&on->right.column);
        reachable.access_team_target = target;
        reachable.from = pending->from;
        JZ_VEC_FOR(const LoweredRelPredicate, f, right.filters) Q(Predicate__Clone__clone)(JAZZ_VEC_SLOT(reachable.access_filters), &f->predicate);
        reachable.edge_table = pending->edge_table;
        reachable.edge_member_column = pending->edge_member_column;
        reachable.edge_parent_column = pending->edge_parent_column;
        reachable.edge_filters = pending->edge_filters;
        reachable.bound.tag = JAZZ_BOUND_MAX_DEPTH;
        reachable.bound.max_depth = pending->max_depth;
        reachable.seed = pending->seed;
        jz_free(pending);
        JZ_VEC_FOR(const LoweredRelPredicate, f, left.filters) Q(Predicate__Clone__clone)(JAZZ_VEC_SLOT(reachable.access_filters), &f->predicate);
        /* Filters above this access join constrain the access rows, not the scalar frontier produced by Gather. */
        LoweredRel result;
        memset(&result, 0, sizeof(result));
        result.scope_paths = right.scope_paths;
        jz_btree_init(&right.scope_paths, &SCOPE_PATHS_DESC);
        result.table = left.table;
        left.table = (jazz_model_OptStr){0};
        result.joins = left.joins;
        memset(&left.joins, 0, sizeof(left.joins));
        result.reachable = left.reachable;
        memset(&left.reachable, 0, sizeof(left.reachable));
        jz_vec_extend(&result.reachable, right.reachable.ptr, right.reachable.len, sizeof(jazz_model_ReachableVia));
        jz_free(right.reachable.ptr);
        memset(&right.reachable, 0, sizeof(right.reachable));
        JZ_VEC_PUSH(result.reachable, reachable);
        lowered_drop(&left);
        lowered_drop(&right);
        jz_vec_drop(&source_route, sizeof(u32), NULL);
        *out = result;
        return JZ_OK;
    }

    jazz_model_JoinCorrelationVec correlated_filters = {0};
    jazz_model_PredicateVec filters = {0};
    JZ_VEC_FOR(LoweredRelPredicate, f, right.filters) {
        if (f->column.some && f->value == LRV_OUTER_ROW) {
            jazz_model_JoinCorrelation *c = JAZZ_VEC_SLOT(correlated_filters);
            c->join_column = f->column.s;
            c->source_column = f->outer;
            Q(Predicate__drop)(&f->predicate);
        } else {
            JZ_VEC_PUSH(filters, f->predicate);
            jazz_model__OptStr__drop(&f->column);
            if (f->value == LRV_OPERAND) Q(Operand__drop)(&f->operand);
            if (f->value == LRV_OUTER_ROW) jz_str_drop(&f->outer);
        }
    }
    jz_free(right.filters.ptr);
    memset(&right.filters, 0, sizeof(right.filters));
    if (!right.table.some) {
        VDROP(correlated_filters, JoinCorrelation);
        VDROP(filters, Predicate);
        e = perr(table, path, "core schema ExistsRel joins require a row-producing right relation");
        goto fail;
    }
    jazz_model_JoinVia join;
    memset(&join, 0, sizeof(join));
    join.table = right.table.s;
    right.table = (jazz_model_OptStr){0};
    join.on_column = jz_str_clone(&on->right.column);
    join.target = target;
    join.source_column = some_str(jz_str_clone(&on->left.column));
    join.correlated_filters = correlated_filters;
    join.filters = filters;
    join.nested_joins = right.joins;
    memset(&right.joins, 0, sizeof(right.joins));
    jazz_model_JoinViaVec *joins = &left.joins;
    JZ_VEC_FOR(const u32, index, source_route) joins = &joins->ptr[*index].nested_joins;
    RouteVec join_route = source_route;
    JZ_VEC_PUSH(join_route, joins->len);
    JZ_VEC_PUSH(*joins, join);
    const jz_str *scope;
    ScopePath *route;
    JZ_BTREE_FOR(&right.scope_paths, scope, route) {
        ScopePath full = {route->some, {0}};
        if (route->some) {
            jz_vec_clone(&full.route, &join_route, sizeof(u32), NULL);
            jz_vec_extend(&full.route, route->route.ptr, route->route.len, sizeof(u32));
        }
        ScopePath *existing = jz_btree_get(&left.scope_paths, scope);
        if (existing) {
            jz_vec_drop(&existing->route, sizeof(u32), NULL);
            existing->some = false;
            jz_vec_drop(&full.route, sizeof(u32), NULL);
        } else {
            jz_str k = jz_str_clone(scope);
            jz_btree_insert(&left.scope_paths, &k, &full, NULL);
        }
    }
    jz_vec_drop(&join_route, sizeof(u32), NULL);
    jz_vec_extend(&left.reachable, right.reachable.ptr, right.reachable.len, sizeof(jazz_model_ReachableVia));
    jz_free(right.reachable.ptr);
    memset(&right.reachable, 0, sizeof(right.reachable));
    lowered_drop(&right);
    *out = left;
    return JZ_OK;
fail:
    jz_vec_drop(&source_route, sizeof(u32), NULL);
    jz_vec_drop(&target_route, sizeof(u32), NULL);
    lowered_drop(&left);
    lowered_drop(&right);
    return e;
}

static const jazz_model_RelExpr *unwrap_rel_filter(const jazz_model_RelExpr *rel, PredRefVec *filters) {
    if (rel->tag == JAZZ_REL_FILTER) {
        const jazz_model_RelPredicateExpr *p = &rel->predicate;
        JZ_VEC_PUSH(*filters, p);
        return rel->input;
    }
    return rel;
}

static jz_err lower_gather_seed(jazz_model_TableName table, jz_slice path, const jazz_model_RelExpr *seed, jazz_model_Operand *from,
                                jazz_model_ReachableSeed *out_seed);
static jz_err lower_gather_step(jazz_model_TableName table, jz_slice path, const jazz_model_RelExpr *step, GatherStep *out);

static jz_err lower_gather_rel(const jazz_model_public_Schema *schema, jazz_model_TableName table, jz_slice path, const jazz_model_RelExpr *seed_rel,
                               const jazz_model_RelExpr *step, const jazz_model_RelRecursionBound *bound, LoweredRel *out) {
    jazz_model_Operand from;
    jazz_model_ReachableSeed seed;
    JZ_TRY(lower_gather_seed(table, path, seed_rel, &from, &seed));
    GatherStep st;
    jz_err e = lower_gather_step(table, path, step, &st);
    if (JZ_IS_ERR(e)) { Q(Operand__drop)(&from); Q(ReachableSeed__drop)(&seed); return e; }
    /* A scalar frontier has no materialized output table, but its projected FK still identifies the logical
     * source used by scoped joins from Gather. Keep this separate from `table`: setting that would turn the
     * key-only frontier into a row-producing relation and change reachability semantics. */
    jazz_model_OptStr output_scope = {0};
    if (st.output_table.some) output_scope = some_str(jz_str_clone(&st.output_table.s));
    else {
        const jazz_model_public_TableSchema *edge = schema_get(schema, &st.edge_table);
        if (edge) {
            JZ_VEC_FOR(const jazz_model_ColumnDescriptor, c, edge->columns.columns) {
                if (jz_str_eq(c->name, &st.edge_parent_column)) {
                    if (c->references) output_scope = some_str(jz_str_from_slice(tn(c->references)));
                    break;
                }
            }
        }
    }
    jazz_model_ReachableSeed *seed_out = NULL;
    if (st.output_table.some && jz_str_eq(&st.output_table.s, &seed.table) && seed.user_column.some &&
        jz_str_eq_cstr(&seed.user_column.s, "id") && jz_str_eq_cstr(&seed.team_column, "id") && seed.filters.len == 0) {
        Q(ReachableSeed__drop)(&seed);
    } else {
        seed_out = JZ_NEW(jazz_model_ReachableSeed);
        *seed_out = seed;
    }
    if (bound->tag == JAZZ_BOUND_FIXPOINT) {
        if (seed_out) { Q(ReachableSeed__drop)(seed_out); jz_free(seed_out); }
        Q(Operand__drop)(&from);
        jz_str_drop(&st.edge_table); jazz_model__OptStr__drop(&st.output_table);
        jz_str_drop(&st.edge_member_column); jz_str_drop(&st.edge_parent_column);
        VDROP(st.edge_filters, Predicate);
        jazz_model__OptStr__drop(&output_scope);
        return perr(table, path, "server shell public schema conversion does not support Gather Fixpoint yet");
    }
    lowered_init(out);
    if (output_scope.some) scope_insert_root(&out->scope_paths, output_scope.s);
    out->table = st.output_table;
    PendingReachable *p = JZ_NEW(PendingReachable);
    p->from = from;
    p->seed = seed_out;
    p->edge_table = st.edge_table;
    p->edge_member_column = st.edge_member_column;
    p->edge_parent_column = st.edge_parent_column;
    p->edge_filters = st.edge_filters;
    p->max_depth = bound->max_depth;
    out->pending_reachable = p;
    return JZ_OK;
}

static jz_err unwrap_joined_seed_projection(jazz_model_TableName table, jz_slice path, const jazz_model_RelExpr *left, const jazz_model_RelExpr *right,
                                            const jazz_model_RelJoinConditionVec *on, u8 join_kind, const jazz_model_RelColumnRef *projected,
                                            jazz_model_OptStr *team_column, jz_slice *out_seed_scope) {
    if (right->tag != JAZZ_REL_TABLE_SCAN) return perr(table, path, "Gather projected seed hop must join to a table scan");
    PredRefVec tmp = {0};
    const jazz_model_RelExpr *seed_scan = unwrap_rel_filter(left, &tmp);
    jz_vec_drop(&tmp, sizeof(void *), NULL);
    if (seed_scan->tag != JAZZ_REL_TABLE_SCAN) return perr(table, path, "Gather projected seed hop must start from a filtered table scan");
    jz_slice seed_scope = seed_scan->alias.some ? SL(seed_scan->alias.s) : tn(seed_scan->table);
    jz_slice target_scope = right->alias.some ? SL(right->alias.s) : tn(right->table);
    /* Like a recursive hop, the seed hop names the target row only when it is one inner equality from a
     * seed-table FK to that row's id. */
    if (on->len != 1) return perr(table, path, "Gather seed hops without exactly one join equality are not supported in policies yet");
    const jazz_model_RelJoinCondition *join = &on->ptr[0];
    if (join_kind != JAZZ_JOIN_INNER || !jz_str_eq_cstr(&join->right.column, "id") || !opt_scope_is(&join->right.scope, target_scope) ||
        !opt_scope_is(&join->left.scope, seed_scope))
        return perr(table, path, "Gather seed hops other than an inner join from a seed FK to the target id are not supported in policies yet");
    bool same_scope = projected->scope.some == right->alias.some && (!projected->scope.some || jz_str_eq(&projected->scope.s, &right->alias.s));
    if (!same_scope || !jz_str_eq_cstr(&projected->column, "id"))
        return perr(table, path, "Gather projected seed id must come from the hop target row id");
    *team_column = some_str(jz_str_clone(&join->left.column));
    *out_seed_scope = seed_scope;
    return JZ_OK;
}

static const jazz_model_RelProjectColumn *find_id_projection(const jazz_model_RelProjectColumnVec *columns) {
    JZ_VEC_FOR(const jazz_model_RelProjectColumn, c, *columns) if (jz_str_eq_cstr(&c->alias, "id")) return c;
    return NULL;
}

static jz_err unwrap_seed_projection(jazz_model_TableName table, jz_slice path, const jazz_model_RelExpr *seed, const jazz_model_RelExpr **out_input,
                                     jazz_model_OptStr *team_column, PredRefVec *filters) {
    memset(team_column, 0, sizeof(*team_column));
    memset(filters, 0, sizeof(*filters));
    if (seed->tag != JAZZ_REL_PROJECT) { *out_input = seed; return JZ_OK; }
    const jazz_model_RelProjectColumn *column = find_id_projection(&seed->columns);
    if (!column) return perr(table, path, "Gather projected seed must expose an id column");
    if (column->expr.tag != JAZZ_KEYREF_COLUMN) return perr(table, path, "Gather projected seed id must be a column projection");
    const jazz_model_RelColumnRef *projected = &column->expr.column;
    PredRefVec pf = {0};
    const jazz_model_RelExpr *project_input = unwrap_rel_filter(seed->input, &pf);
    if (project_input->tag == JAZZ_REL_JOIN) {
        jz_slice seed_scope;
        jz_err e = unwrap_joined_seed_projection(table, path, project_input->input, project_input->right, &project_input->on,
                                                 project_input->join_kind, projected, team_column, &seed_scope);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&pf, sizeof(void *), NULL); return e; }
        /* These filters are applied to the seed table, so they must name it. */
        JZ_VEC(const jazz_model_RelColumnRef *) columns = {0};
        JZ_VEC_FOR(const jazz_model_RelPredicateExpr *const, f, pf) collect_rel_predicate_columns(*f, (void *)&columns);
        bool foreign = false;
        JZ_VEC_FOR(const jazz_model_RelColumnRef *const, c, columns) if (!opt_scope_is(&(*c)->scope, seed_scope)) foreign = true;
        jz_vec_drop(&columns, sizeof(void *), NULL);
        if (foreign) {
            jazz_model__OptStr__drop(team_column);
            jz_vec_drop(&pf, sizeof(void *), NULL);
            return perr(table, path, "Gather seed filters on the hop target are not supported in policies yet");
        }
        *out_input = project_input->input;
        *filters = pf;
        return JZ_OK;
    }
    jz_vec_drop(&pf, sizeof(void *), NULL);
    *out_input = seed->input;
    *team_column = some_str(jz_str_clone(&projected->column));
    return JZ_OK;
}

static jz_err lower_gather_seed(jazz_model_TableName table, jz_slice path, const jazz_model_RelExpr *seed, jazz_model_Operand *from,
                                jazz_model_ReachableSeed *out_seed) {
    const jazz_model_RelExpr *input;
    jazz_model_OptStr projected_team_column;
    PredRefVec projected_filters;
    JZ_TRY(unwrap_seed_projection(table, path, seed, &input, &projected_team_column, &projected_filters));
    PredRefVec filters = {0};
    input = unwrap_rel_filter(input, &filters);
    jz_vec_extend(&filters, projected_filters.ptr, projected_filters.len, sizeof(void *));
    jz_vec_drop(&projected_filters, sizeof(void *), NULL);
    jz_err e = JZ_OK;
    LoweredRelPredicateVec lowered = {0};
    if (input->tag != JAZZ_REL_TABLE_SCAN) {
        e = perr(table, path, "Gather seeded reachability requires a filtered seed table scan");
        jz_vec_drop(&filters, sizeof(void *), NULL);
        goto fail;
    }
    e = rel_predicates_to_policy(table, path, &filters, &lowered);
    jz_vec_drop(&filters, sizeof(void *), NULL);
    if (JZ_IS_ERR(e)) goto fail;
    i64 seed_index = -1;
    for (u32 i = 0; i < lowered.len; i++) {
        const LoweredRelPredicate *f = &lowered.ptr[i];
        if (f->predicate.tag == JAZZ_PRED_EQ && f->value == LRV_OPERAND && f->operand.tag == JAZZ_OPERAND_CLAIM) { seed_index = i; break; }
    }
    if (seed_index < 0) { e = perr(table, path, "Gather same-table seeds require one claim-keyed equality filter"); goto fail; }
    const LoweredRelPredicate *seed_filter = &lowered.ptr[seed_index];
    if (!seed_filter->column.some) { e = perr(table, path, "Gather seed claim filter must name a seed-table column"); goto fail; }
    if (!(seed_filter->value == LRV_OPERAND && seed_filter->operand.tag == JAZZ_OPERAND_CLAIM)) {
        e = perr(table, path, "Gather seed claim filter must compare against a claim");
        goto fail;
    }
    jz_str user_column = jz_str_clone(&seed_filter->column.s);
    jz_str user_claim = jz_str_clone(&seed_filter->operand.name);
    jazz_model_PredicateVec rest = {0};
    for (u32 i = 0; i < lowered.len; i++) {
        if ((i64)i == seed_index) continue;
        JZ_VEC_PUSH(rest, lowered.ptr[i].predicate);
        lowered.ptr[i].predicate = Q(all_of)((jazz_model_PredicateVec){0});
    }
    jz_vec_drop(&lowered, sizeof(LoweredRelPredicate), lrp_drop_v);
    *from = Q(claim)(jz_str_clone(&user_claim));
    out_seed->table = jz_str_from_slice(tn(input->table));
    out_seed->user_column = some_str(user_column);
    out_seed->user_claim = some_str(user_claim);
    out_seed->team_column = projected_team_column.some ? projected_team_column.s : jz_str_from("id");
    out_seed->filters = rest;
    return JZ_OK;
fail:
    jz_vec_drop(&lowered, sizeof(LoweredRelPredicate), lrp_drop_v);
    jazz_model__OptStr__drop(&projected_team_column);
    return e;
}

static jz_err lower_gather_step(jazz_model_TableName table, jz_slice path, const jazz_model_RelExpr *step, GatherStep *out) {
    if (step->tag != JAZZ_REL_PROJECT) return perr(table, path, "Gather policies require projected recursive hops");
    const jazz_model_RelExpr *input = step->input;
    const jazz_model_RelProjectColumnVec *columns = &step->columns;
    /* General relation gathers return rows by joining the edge parent to an output table. Policy reachability
     * instead carries only the projected parent key; accepting that canonical scalar form avoids requiring every
     * reachable parent to have another row in the seed-membership table. */
    const jazz_model_RelExpr *edge_rel;
    jazz_model_OptStr output_table = {0};
    jz_slice edge_parent_column;
    const jazz_model_OptStr *projected_scope;
    if (input->tag == JAZZ_REL_JOIN) {
        const jazz_model_RelExpr *right = input->right;
        if (right->tag != JAZZ_REL_TABLE_SCAN) return perr(table, path, "Gather recursive hop must join to the output table");
        if (input->on.len != 1)
            return perr(table, path, "Gather recursive hops without exactly one join equality are not supported in policies yet");
        const jazz_model_RelJoinCondition *on = &input->on.ptr[0];
        /* The hop names the parent row only when it joins that row's id. */
        jz_slice output_scope = right->alias.some ? SL(right->alias.s) : tn(right->table);
        if (input->join_kind != JAZZ_JOIN_INNER || !jz_str_eq_cstr(&on->right.column, "id") || !opt_scope_is(&on->right.scope, output_scope))
            return perr(table, path,
                        "Gather recursive hops other than an inner join from an edge FK to the output id are not supported in policies yet");
        /* The next frontier is the hop target's row id. A projection may carry target columns, but an `id`
         * alias must still be that row's id or the step walks somewhere else. */
        JZ_VEC_FOR(const jazz_model_RelProjectColumn, c, *columns) {
            if (!jz_str_eq_cstr(&c->alias, "id")) continue;
            bool is_output_id = c->expr.tag == JAZZ_KEYREF_COLUMN && jz_str_eq_cstr(&c->expr.column.column, "id") &&
                                opt_scope_is(&c->expr.column.scope, output_scope);
            if (!is_output_id)
                return perr(table, path,
                            "Gather recursive hops projecting anything but the output row id as id are not supported in policies yet");
        }
        edge_rel = input->input;
        output_table = some_str(jz_str_from_slice(tn(right->table)));
        edge_parent_column = SL(on->left.column);
        projected_scope = &on->left.scope;
    } else {
        const jazz_model_RelProjectColumn *projected = find_id_projection(columns);
        if (!projected) return perr(table, path, "Gather scalar recursive hop must project its parent key as id");
        if (projected->expr.tag != JAZZ_KEYREF_COLUMN) return perr(table, path, "Gather scalar recursive hop id must be a column projection");
        edge_rel = input;
        edge_parent_column = SL(projected->expr.column.column);
        projected_scope = &projected->expr.column.scope;
    }

    PredRefVec filters = {0};
    LoweredRelPredicateVec lowered = {0};
    jz_err e = JZ_OK;
    const jazz_model_RelExpr *edge_input = unwrap_rel_filter(edge_rel, &filters);
    if (edge_input->tag != JAZZ_REL_TABLE_SCAN) { e = perr(table, path, "Gather recursive hop must start from an edge table scan"); goto fail; }
    jz_slice edge_scope = edge_input->alias.some ? SL(edge_input->alias.s) : tn(edge_input->table);
    if (!opt_scope_is(projected_scope, edge_scope)) {
        e = perr(table, path, "Gather recursive hop keys from outside the edge table are not supported in policies yet");
        goto fail;
    }
    e = rel_predicates_to_policy(table, path, &filters, &lowered);
    if (JZ_IS_ERR(e)) goto fail;
    i64 frontier_index = -1;
    for (u32 i = 0; i < lowered.len; i++) if (lowered.ptr[i].value == LRV_FRONTIER_ROW) { frontier_index = i; break; }
    if (frontier_index < 0) { e = perr(table, path, "Gather recursive hop requires a frontier equality"); goto fail; }
    const LoweredRelPredicate *frontier = &lowered.ptr[frontier_index];
    if (!frontier->column.some) { e = perr(table, path, "Gather frontier equality must name an edge column"); goto fail; }
    out->edge_member_column = jz_str_clone(&frontier->column.s);
    memset(&out->edge_filters, 0, sizeof(out->edge_filters));
    for (u32 i = 0; i < lowered.len; i++) {
        if ((i64)i == frontier_index) continue;
        JZ_VEC_PUSH(out->edge_filters, lowered.ptr[i].predicate);
        lowered.ptr[i].predicate = Q(all_of)((jazz_model_PredicateVec){0});
    }
    jz_vec_drop(&lowered, sizeof(LoweredRelPredicate), lrp_drop_v);
    jz_vec_drop(&filters, sizeof(void *), NULL);
    out->edge_table = jz_str_from_slice(tn(edge_input->table));
    out->output_table = output_table;
    out->edge_parent_column = jz_str_from_slice(edge_parent_column);
    return JZ_OK;
fail:
    jz_vec_drop(&lowered, sizeof(LoweredRelPredicate), lrp_drop_v);
    jz_vec_drop(&filters, sizeof(void *), NULL);
    jazz_model__OptStr__drop(&output_table);
    return e;
}

/* ---------------------------------------------------------------- relation predicates -> policy predicates */
static u8 cmp_predicate_tag(u8 op) {
    static const u8 tags[] = {JAZZ_PRED_EQ, JAZZ_PRED_NE, JAZZ_PRED_LT, JAZZ_PRED_LTE, JAZZ_PRED_GT, JAZZ_PRED_GTE};
    return tags[op];
}
static jazz_model_Operand col_of(const jazz_model_RelColumnRef *c) { return Q(col)(jz_str_clone(&c->column)); }

static jz_err rel_predicate_to_policy(jazz_model_TableName table, jz_slice path, const jazz_model_RelPredicateExpr *p, LoweredRelPredicateVec *out) {
    memset(out, 0, sizeof(*out));
    LoweredRelPredicateVec part;
    jz_err e;
    switch (p->tag) {
    case JAZZ_RPE_TRUE: return JZ_OK;
    case JAZZ_RPE_FALSE: JZ_VEC_PUSH(*out, lrp_bare(Q(any_of)((jazz_model_PredicateVec){0}))); return JZ_OK;
    case JAZZ_RPE_AND:
        JZ_VEC_FOR(const jazz_model_RelPredicateExpr, c, p->items) {
            e = rel_predicate_to_policy(table, path, c, &part);
            if (JZ_IS_ERR(e)) { jz_vec_drop(out, sizeof(LoweredRelPredicate), lrp_drop_v); return e; }
            jz_vec_extend(out, part.ptr, part.len, sizeof(LoweredRelPredicate));
            jz_free(part.ptr);
        }
        return JZ_OK;
    case JAZZ_RPE_OR: {
        jazz_model_PredicateVec predicates = {0};
        JZ_VEC_FOR(const jazz_model_RelPredicateExpr, c, p->items) {
            e = rel_predicate_to_policy(table, path, c, &part);
            if (JZ_IS_ERR(e)) { VDROP(predicates, Predicate); return e; }
            JZ_VEC_PUSH(predicates, Q(all_of)(lowered_predicates(&part)));
        }
        JZ_VEC_PUSH(*out, lrp_bare(Q(any_of)(predicates)));
        return JZ_OK;
    }
    case JAZZ_RPE_NOT:
        JZ_TRY(rel_predicate_to_policy(table, path, p->inner, &part));
        JZ_VEC_PUSH(*out, lrp_bare(Q(not)(Q(all_of)(lowered_predicates(&part)))));
        return JZ_OK;
    case JAZZ_RPE_CMP: {
        if (p->right.tag == JAZZ_RVR_LITERAL && p->right.literal.tag == JAZZ_PV_NULL) {
            jazz_model_Predicate predicate;
            switch (p->op) {
            case JAZZ_PCMP_EQ: predicate = Q(is_null)(col_of(&p->left)); break;
            case JAZZ_PCMP_NE: predicate = Q(not)(Q(is_null)(col_of(&p->left))); break;
            default: return perr(table, path, "core schema ExistsRel NULL comparisons only support equality and inequality");
            }
            JZ_VEC_PUSH(*out, lrp_on(SL(p->left.column), predicate));
            return JZ_OK;
        }
        LoweredRelValue value;
        JZ_TRY(rel_value_to_policy_operand(table, path, &p->right, &value));
        jazz_model_Predicate predicate;
        if (value.tag == LRV_OPERAND) {
            jazz_model_Operand right;
            Q(Operand__Clone__clone)(&right, &value.operand);
            predicate = Q(binary)(cmp_predicate_tag(p->op), col_of(&p->left), right);
        } else if (p->op == JAZZ_PCMP_EQ) {
            predicate = Q(all_of)((jazz_model_PredicateVec){0});
        } else {
            lrv_drop(&value);
            return perr(table, path, "core schema ExistsRel special row-id comparisons only support equality");
        }
        LoweredRelPredicate lp;
        memset(&lp, 0, sizeof(lp));
        lp.predicate = predicate;
        lp.column = some_str(jz_str_clone(&p->left.column));
        lp.value = value.tag;
        lp.operand = value.operand;
        lp.outer = value.outer;
        JZ_VEC_PUSH(*out, lp);
        return JZ_OK;
    }
    case JAZZ_RPE_IS_NULL: JZ_VEC_PUSH(*out, lrp_on(SL(p->left.column), Q(is_null)(col_of(&p->left)))); return JZ_OK;
    case JAZZ_RPE_IS_NOT_NULL: JZ_VEC_PUSH(*out, lrp_on(SL(p->left.column), Q(not)(Q(is_null)(col_of(&p->left))))); return JZ_OK;
    case JAZZ_RPE_CONTAINS: {
        LoweredRelValue value;
        JZ_TRY(rel_value_to_policy_operand(table, path, &p->right, &value));
        if (value.tag != LRV_OPERAND) {
            lrv_drop(&value);
            return perr(table, path, "core schema ExistsRel Contains does not support row-id operands");
        }
        JZ_VEC_PUSH(*out, lrp_on(SL(p->left.column), Q(binary)(JAZZ_PRED_CONTAINS, col_of(&p->left), value.operand)));
        return JZ_OK;
    }
    case JAZZ_RPE_ENUM_MATCH: {
        JZ_TRY(rel_predicate_to_policy(table, path, p->inner, &part));
        jazz_model_Predicate predicate;
        memset(&predicate, 0, sizeof(predicate));
        predicate.tag = JAZZ_PRED_ENUM_MATCH;
        predicate.column = jz_str_clone(&p->left.column);
        predicate.case_ = jz_str_clone(&p->case_);
        predicate.inner = JZ_NEW(jazz_model_Predicate);
        *predicate.inner = Q(all_of)(lowered_predicates(&part));
        JZ_VEC_PUSH(*out, lrp_on(SL(p->left.column), predicate));
        return JZ_OK;
    }
    case JAZZ_RPE_IN: {
        jazz_model_OperandVec values = {0};
        JZ_VEC_FOR(const jazz_model_RelValueRef, v, p->values) {
            LoweredRelValue value;
            e = rel_value_to_policy_operand(table, path, v, &value);
            if (!JZ_IS_ERR(e) && value.tag != LRV_OPERAND) {
                lrv_drop(&value);
                e = perr(table, path, "core schema ExistsRel In does not support row-id operands");
            }
            if (JZ_IS_ERR(e)) { VDROP(values, Operand); return e; }
            JZ_VEC_PUSH(values, value.operand);
        }
        JZ_VEC_PUSH(*out, lrp_on(SL(p->left.column), Q(in_list)(col_of(&p->left), values)));
        return JZ_OK;
    }
    default: return JZ_OK;
    }
}

static jz_err rel_predicates_to_policy(jazz_model_TableName table, jz_slice path, const PredRefVec *ps, LoweredRelPredicateVec *out) {
    memset(out, 0, sizeof(*out));
    JZ_VEC_FOR(const jazz_model_RelPredicateExpr *const, p, *ps) {
        LoweredRelPredicateVec part;
        jz_err e = rel_predicate_to_policy(table, path, *p, &part);
        if (JZ_IS_ERR(e)) { jz_vec_drop(out, sizeof(LoweredRelPredicate), lrp_drop_v); return e; }
        jz_vec_extend(out, part.ptr, part.len, sizeof(LoweredRelPredicate));
        jz_free(part.ptr);
    }
    return JZ_OK;
}

static jz_err rel_value_to_policy_operand(jazz_model_TableName table, jz_slice path, const jazz_model_RelValueRef *v, LoweredRelValue *out) {
    memset(out, 0, sizeof(*out));
    switch (v->tag) {
    case JAZZ_RVR_LITERAL: {
        groove_Value g;
        JZ_TRY(convert_policy_literal(table, path, &v->literal, &g));
        out->tag = LRV_OPERAND;
        out->operand = Q(lit)(g);
        return JZ_OK;
    }
    case JAZZ_RVR_SESSION_REF:
        JZ_TRY(convert_session_path_operand(table, path, &v->session_ref, &out->operand));
        out->tag = LRV_OPERAND;
        return JZ_OK;
    case JAZZ_RVR_OUTER_COLUMN:
        out->tag = LRV_OUTER_ROW;
        out->outer = jz_str_clone(&v->outer_column.column);
        return JZ_OK;
    default:
        if (v->row_id == JAZZ_ROWID_OUTER) { out->tag = LRV_OUTER_ROW; out->outer = jz_str_from("id"); return JZ_OK; }
        if (v->row_id == JAZZ_ROWID_FRONTIER) { out->tag = LRV_FRONTIER_ROW; return JZ_OK; }
        return perr(table, path, "core schema ExistsRel policies do not support Current row-id operands here");
    }
}

/* ---------------------------------------------------------------- INHERITS */
static jz_err append_inherited_policy_expanded_fallback(const jazz_model_public_Schema *schema, const jazz_model_public_TableSchema *parent_schema,
                                                        jazz_model_TableName table, jz_slice path, jazz_model_Query query,
                                                        jazz_model_TableName parent_table, const jz_str *via_column,
                                                        const jazz_model_PolicyExpr *parent_policy, u8 operation, ExpansionPath *expansion,
                                                        jazz_model_Query *out);

static jz_err append_inherited_policy(const jazz_model_public_Schema *schema, const jazz_model_public_TableSchema *table_schema,
                                      jazz_model_TableName table, jz_slice path, jazz_model_Query query, u8 operation, const jz_str *via_column,
                                      bool has_max_depth, u32 max_depth, bool native_select_inherits, ExpansionPath *expansion,
                                      jazz_model_Query *out) {
    jz_err e;
    const jazz_model_ColumnDescriptor *column = NULL;
    JZ_VEC_FOR(const jazz_model_ColumnDescriptor, c, table_schema->columns.columns) if (jz_str_eq(c->name, via_column)) { column = c; break; }
    if (!column) { e = perr(table, path, "INHERITS via_column '%.*s' was not found", STRF(*via_column)); goto fail; }
    jazz_model_TableName parent_table = column->references;
    if (!parent_table) { e = perr(table, path, "INHERITS via_column '%.*s' has no FK reference", STRF(*via_column)); goto fail; }
    const jazz_model_public_TableSchema *parent_schema = schema_get(schema, parent_table);
    if (!parent_schema) {
        e = perr(table, path, "INHERITS via_column '%.*s' references unknown table '%.*s'", STRF(*via_column), TNF(parent_table));
        goto fail;
    }
    const jazz_model_PolicyExpr *parent_policy = source_operation_policy(parent_schema, operation);
    if (!parent_policy) {
        jz_str op = {0};
        PE(Operation__Debug__fmt)(&op, operation);
        e = perr(table, path, "INHERITS via_column '%.*s' references table '%.*s' without a %.*s policy", STRF(*via_column), TNF(parent_table),
                 STRF(op));
        jz_str_drop(&op);
        goto fail;
    }
    /* A top-level SELECT inherit lowers through derivation collapse. When its policy is embedded under
     * INHERITS_REFERENCING, though, an inherit atom would be evaluated against the outer row; expand it into
     * the source join chain instead. */
    if (operation == JAZZ_OP_SELECT && !native_select_inherits) {
        if (has_max_depth) {
            e = perr(table, path,
                     "bounded SELECT INHERITS under INHERITS_REFERENCING is unsupported because its expanded join chain cannot preserve max_depth");
            goto fail;
        }
        return append_inherited_policy_expanded_fallback(schema, parent_schema, table, path, query, parent_table, via_column, parent_policy,
                                                         operation, expansion, out);
    }
    jazz_model_InheritsVia *iv = JAZZ_VEC_SLOT(query.inherits);
    iv->parent_column = jz_str_clone(via_column);
    iv->operation = convert_inherits_operation(operation);
    iv->has_max_depth = has_max_depth;
    iv->max_depth = max_depth;
    *out = query;
    return JZ_OK;
fail:
    Q(Query__drop)(&query);
    return e;
}

static jz_err append_inherited_policy_branches(jazz_model_TableName table, jz_slice path, jazz_model_Query query, jazz_model_TableName parent_table,
                                               const jz_str *via_column, jazz_model_Query parent_query, jazz_model_Query *out);

static jz_err append_inherited_policy_expanded_fallback(const jazz_model_public_Schema *schema, const jazz_model_public_TableSchema *parent_schema,
                                                        jazz_model_TableName table, jz_slice path, jazz_model_Query query,
                                                        jazz_model_TableName parent_table, const jz_str *via_column,
                                                        const jazz_model_PolicyExpr *parent_policy, u8 operation, ExpansionPath *expansion,
                                                        jazz_model_Query *out) {
    jz_str sub = sub_path(path, ".Inherits[%.*s]", TNF(parent_table));
    jazz_model_Predicate parent_filter;
    jz_err e = convert_policy_predicate(parent_table, SL(sub), parent_policy, &parent_filter);
    if (!JZ_IS_ERR(e)) {
        jz_str_drop(&sub);
        jazz_model_PredicateVec filters = {0};
        JZ_VEC_PUSH(filters, parent_filter);
        Q(Query__join_via_row_id)(&query, jz_str_from_slice(tn(parent_table)), jz_str_clone(via_column), filters);
        *out = query;
        return JZ_OK;
    }
    if (!policy_requires_branch(parent_policy)) {
        jz_str_drop(&sub);
        Q(Query__drop)(&query);
        return e;
    }
    jz_err_drop(&e);
    jazz_model_Query parent_query;
    e = convert_expanded_inherited_policy(schema, parent_schema, parent_table, SL(sub), parent_policy, operation, expansion, &parent_query);
    jz_str_drop(&sub);
    if (JZ_IS_ERR(e)) { Q(Query__drop)(&query); return e; }
    return append_inherited_policy_branches(table, path, query, parent_table, via_column, parent_query, out);
}

static jz_err inherited_parent_branch_to_child_query(jazz_model_TableName table, jz_slice path, jazz_model_TableName parent_table,
                                                     const jz_str *via_column, u32 index, jazz_model_PolicyBranch *branch, jazz_model_Query *out);

static jz_err append_inherited_policy_branches(jazz_model_TableName table, jz_slice path, jazz_model_Query query, jazz_model_TableName parent_table,
                                               const jz_str *via_column, jazz_model_Query parent_query, jazz_model_Query *out) {
    Q(Query__filter)(&query, Q(any_of)((jazz_model_PredicateVec){0}));
    jazz_model_PolicyBranchVec branches;
    Q(PolicyBranch__alternatives_from_query)(parent_query, &branches);
    jz_err e = JZ_OK;
    for (u32 index = 0; index < branches.len; index++) {
        jazz_model_Query branch_query;
        e = inherited_parent_branch_to_child_query(table, path, parent_table, via_column, index, &branches.ptr[index], &branch_query);
        if (JZ_IS_ERR(e)) break;
        push_alternatives(&query, branch_query);
    }
    VDROP(branches, PolicyBranch);
    if (JZ_IS_ERR(e)) { Q(Query__drop)(&query); return e; }
    *out = query;
    return JZ_OK;
}

static jazz_model_JoinSourceLookup *parent_lookup(jazz_model_TableName parent_table, const jz_str *via_column, jz_str value_column) {
    jazz_model_JoinSourceLookup *l = JZ_NEW(jazz_model_JoinSourceLookup);
    l->table = jz_str_from_slice(tn(parent_table));
    l->row_id_source_column = jz_str_clone(via_column);
    l->value_column = value_column;
    return l;
}

/* Consumes `branch`'s filters and joins (left zeroed for the caller's drop). */
static jz_err inherited_parent_branch_to_child_query(jazz_model_TableName table, jz_slice path, jazz_model_TableName parent_table,
                                                     const jz_str *via_column, u32 index, jazz_model_PolicyBranch *branch, jazz_model_Query *out) {
    if (branch->reachable.len)
        return err(jz_format("$.%.*s.%.*s.InheritsBranch[%u]", TNF(table), (int)path.len, path.ptr, index),
                   "core schema policies do not support inherited SELECT branches with reachability yet");
    jazz_model_Query query = query_from(table);
    if (branch->filters.len) {
        Q(Query__join_via_row_id)(&query, jz_str_from_slice(tn(parent_table)), jz_str_clone(via_column), branch->filters);
        memset(&branch->filters, 0, sizeof(branch->filters));
    }
    JZ_VEC_FOR(jazz_model_JoinVia, join, branch->joins) {
        jazz_model_OptStr source_column = join->source_column;
        if (join->source_lookup) {
            jazz_model__OptStr__drop(&source_column);
            source_column = some_str(jz_str_clone(&join->source_lookup->row_id_source_column));
            jz_str_drop(&join->source_lookup->table);
            jz_str_drop(&join->source_lookup->row_id_source_column);
            jz_str_drop(&join->source_lookup->value_column);
            jz_free(join->source_lookup);
        }
        join->source_lookup = NULL;
        join->source_column = (jazz_model_OptStr){0};
        if (join->target == JAZZ_JOIN_TARGET_UNCORRELATED) {
            /* An independent parent proof still requires the referenced parent row to exist. It cannot
             * authorize a dangling FK. */
            Q(Query__join_via_row_id)(&query, jz_str_from_slice(tn(parent_table)), jz_str_clone(via_column), (jazz_model_PredicateVec){0});
            JZ_VEC_PUSH(query.joins, uncorrelated_exists_join(join->table, join->filters, join->nested_joins));
            jz_str_drop(&join->on_column);
            VDROP(join->correlated_filters, JoinCorrelation);
            jazz_model__OptStr__drop(&source_column);
        } else if (source_column.some) {
            jazz_model_JoinSourceLookup *lookup = parent_lookup(parent_table, via_column, source_column.s);
            jazz_model_JoinVia *j = JAZZ_VEC_SLOT(query.joins);
            j->table = join->table;
            j->on_column = join->on_column;
            j->target = join->target;
            j->source_column = some_str(jz_str_clone(&lookup->value_column));
            j->source_lookup = lookup;
            j->correlated_filters = join->correlated_filters;
            j->filters = join->filters;
            j->nested_joins = join->nested_joins;
        } else if (join->target == JAZZ_JOIN_TARGET_COLUMN) {
            push_join(&query, join->table, join->on_column, JAZZ_JOIN_TARGET_COLUMN, some_str(jz_str_clone(via_column)), join->correlated_filters,
                      join->filters, join->nested_joins);
        } else {
            if (jz_slice_cmp(SL(join->table), tn(parent_table)) != 0) {
                Q(Query__drop)(&query);
                return err(jz_format("$.%.*s.%.*s.InheritsBranch[%u]", TNF(table), (int)path.len, path.ptr, index),
                           "core schema policies do not support inherited SELECT row-id joins to non-parent tables yet");
            }
            Q(Query__join_via_row_id)(&query, join->table, jz_str_clone(via_column), join->filters);
            jazz_model_JoinVia *last = &query.joins.ptr[query.joins.len - 1];
            last->correlated_filters = join->correlated_filters;
            last->nested_joins = join->nested_joins;
            jz_str_drop(&join->on_column);
        }
        memset(join, 0, sizeof(*join));
    }
    *out = query;
    return JZ_OK;
}

/* ---------------------------------------------------------------- policy predicates */
static jz_err convert_policy_list(jazz_model_TableName table, jz_slice path, const char *label, const jazz_model_PolicyExprVec *exprs,
                                  jazz_model_PredicateVec *out) {
    memset(out, 0, sizeof(*out));
    for (u32 index = 0; index < exprs->len; index++) {
        jz_str sub = sub_path(path, ".%s[%u]", label, index);
        jazz_model_Predicate p;
        jz_err e = convert_policy_predicate(table, SL(sub), &exprs->ptr[index], &p);
        jz_str_drop(&sub);
        if (JZ_IS_ERR(e)) { VDROP(*out, Predicate); return e; }
        JZ_VEC_PUSH(*out, p);
    }
    return JZ_OK;
}

static jz_err convert_policy_predicate(jazz_model_TableName table, jz_slice path, const jazz_model_PolicyExpr *expr, jazz_model_Predicate *out) {
    jazz_model_PredicateVec ps;
    jazz_model_Operand left, right;
    groove_Value g;
    jz_err e;
    switch (expr->tag) {
    case JAZZ_PE_TRUE: *out = Q(all_of)((jazz_model_PredicateVec){0}); return JZ_OK;
    case JAZZ_PE_FALSE: *out = Q(any_of)((jazz_model_PredicateVec){0}); return JZ_OK;
    case JAZZ_PE_AND: JZ_TRY(convert_policy_list(table, path, "And", &expr->exprs, &ps)); *out = Q(all_of)(ps); return JZ_OK;
    case JAZZ_PE_OR: JZ_TRY(convert_policy_list(table, path, "Or", &expr->exprs, &ps)); *out = Q(any_of)(ps); return JZ_OK;
    case JAZZ_PE_NOT: {
        jz_str sub = sub_path(path, ".Not");
        jazz_model_Predicate inner;
        e = convert_policy_predicate(table, SL(sub), expr->inner, &inner);
        jz_str_drop(&sub);
        JZ_TRY(e);
        *out = Q(not)(inner);
        return JZ_OK;
    }
    case JAZZ_PE_CMP:
        left = Q(col)(jz_str_clone(&expr->column));
        if (expr->pvalue.tag == JAZZ_POLVAL_LITERAL && expr->pvalue.literal.tag == JAZZ_PV_NULL) {
            /* Match `session_where`: equality against the public NULL literal is the ergonomic spelling of a null test. */
            if (expr->op == JAZZ_CMP_EQ) { *out = Q(is_null)(left); return JZ_OK; }
            if (expr->op == JAZZ_CMP_NE) { *out = Q(not)(Q(is_null)(left)); return JZ_OK; }
        }
        e = convert_policy_operand(table, path, &expr->pvalue, &right);
        if (JZ_IS_ERR(e)) { Q(Operand__drop)(&left); return e; }
        *out = Q(binary)(cmp_predicate_tag(expr->op), left, right);
        return JZ_OK;
    case JAZZ_PE_SESSION_CMP:
        JZ_TRY(convert_session_path_operand(table, path, &expr->path, &left));
        if (expr->value.tag == JAZZ_PV_NULL) {
            if (expr->op == JAZZ_CMP_EQ) { *out = Q(is_null)(left); return JZ_OK; }
            if (expr->op == JAZZ_CMP_NE) { *out = Q(not)(Q(is_null)(left)); return JZ_OK; }
        }
        e = convert_policy_literal(table, path, &expr->value, &g);
        if (JZ_IS_ERR(e)) { Q(Operand__drop)(&left); return e; }
        right = Q(lit)(g);
        if (expr->op == JAZZ_CMP_EQ || expr->op == JAZZ_CMP_NE) {
            *out = Q(binary)(cmp_predicate_tag(expr->op), left, right);
            return JZ_OK;
        }
        Q(Operand__drop)(&left);
        Q(Operand__drop)(&right);
        return perr(table, path, "core schema policies only support Eq/Ne SessionCmp operators yet");
    case JAZZ_PE_SESSION_IS_NULL:
        JZ_TRY(convert_session_path_operand(table, path, &expr->path, &left));
        *out = Q(is_null)(left);
        return JZ_OK;
    case JAZZ_PE_SESSION_IS_NOT_NULL:
        JZ_TRY(convert_session_path_operand(table, path, &expr->path, &left));
        *out = Q(not)(Q(is_null)(left));
        return JZ_OK;
    case JAZZ_PE_IS_NULL: *out = Q(is_null)(Q(col)(jz_str_clone(&expr->column))); return JZ_OK;
    case JAZZ_PE_IS_NOT_NULL: *out = Q(not)(Q(is_null)(Q(col)(jz_str_clone(&expr->column)))); return JZ_OK;
    case JAZZ_PE_CONTAINS:
        JZ_TRY(convert_policy_operand(table, path, &expr->pvalue, &right));
        *out = Q(binary)(JAZZ_PRED_CONTAINS, Q(col)(jz_str_clone(&expr->column)), right);
        return JZ_OK;
    case JAZZ_PE_IN:
        JZ_TRY(convert_session_path_operand(table, path, &expr->path, &left));
        *out = Q(binary)(JAZZ_PRED_CONTAINS, left, Q(col)(jz_str_clone(&expr->column)));
        return JZ_OK;
    case JAZZ_PE_IN_LIST: {
        jazz_model_OperandVec values = {0};
        for (u32 index = 0; index < expr->pvalues.len; index++) {
            jz_str sub = sub_path(path, ".InList[%u]", index);
            jazz_model_Operand v;
            e = convert_policy_operand(table, SL(sub), &expr->pvalues.ptr[index], &v);
            jz_str_drop(&sub);
            if (JZ_IS_ERR(e)) { VDROP(values, Operand); return e; }
            JZ_VEC_PUSH(values, v);
        }
        *out = Q(in_list)(Q(col)(jz_str_clone(&expr->column)), values);
        return JZ_OK;
    }
    case JAZZ_PE_SESSION_CONTAINS:
        JZ_TRY(convert_session_path_operand(table, path, &expr->path, &left));
        e = convert_policy_literal(table, path, &expr->value, &g);
        if (JZ_IS_ERR(e)) { Q(Operand__drop)(&left); return e; }
        *out = Q(binary)(JAZZ_PRED_CONTAINS, left, Q(lit)(g));
        return JZ_OK;
    case JAZZ_PE_SESSION_IN_LIST: {
        jazz_model_OperandVec values = {0};
        for (u32 index = 0; index < expr->values.len; index++) {
            jz_str sub = sub_path(path, ".SessionInList[%u]", index);
            e = convert_policy_literal(table, SL(sub), &expr->values.ptr[index], &g);
            jz_str_drop(&sub);
            if (JZ_IS_ERR(e)) { VDROP(values, Operand); return e; }
            JZ_VEC_PUSH(values, Q(lit)(g));
        }
        e = convert_session_path_operand(table, path, &expr->path, &left);
        if (JZ_IS_ERR(e)) { VDROP(values, Operand); return e; }
        *out = Q(in_list)(left, values);
        return JZ_OK;
    }
    default: {
        jz_str dbg = {0};
        PE(PolicyExpr__Debug__fmt)(&dbg, expr);
        e = perr(table, path, "core schema policies do not support %.*s yet", STRF(dbg));
        jz_str_drop(&dbg);
        return e;
    }
    }
}

static jz_err convert_policy_operand(jazz_model_TableName table, jz_slice path, const jazz_model_PolicyValue *v, jazz_model_Operand *out) {
    if (v->tag == JAZZ_POLVAL_SESSION_REF) return convert_session_path_operand(table, path, &v->path, out);
    groove_Value g;
    JZ_TRY(convert_policy_literal(table, path, &v->literal, &g));
    *out = Q(lit)(g);
    return JZ_OK;
}

static bool path_is_one_of(const jazz_model_StrVec *segs, const char *const *names, u32 n) {
    if (segs->len != 1) return false;
    for (u32 i = 0; i < n; i++) if (jz_str_eq_cstr(&segs->ptr[0], names[i])) return true;
    return false;
}

static jz_err convert_session_path_operand(jazz_model_TableName table, jz_slice path, const jazz_model_StrVec *segs, jazz_model_Operand *out) {
    jz_str name;
    if (Q(author_claim_path_key)(segs->ptr, segs->len, &name)) { *out = Q(claim)(name); return JZ_OK; }
    if (path_is_one_of(segs, PUBLIC_USER_SESSION_PATHS, sizeof(PUBLIC_USER_SESSION_PATHS) / sizeof(*PUBLIC_USER_SESSION_PATHS))) {
        *out = Q(claim)(jz_str_from(DIRECT_USER_CLAIM));
        return JZ_OK;
    }
    if (path_is_one_of(segs, PUBLIC_AUTH_MODE_SESSION_PATHS, sizeof(PUBLIC_AUTH_MODE_SESSION_PATHS) / sizeof(*PUBLIC_AUTH_MODE_SESSION_PATHS))) {
        *out = Q(claim)(jz_str_from(DIRECT_AUTH_MODE_CLAIM));
        return JZ_OK;
    }
    if (segs->len >= 2 && jz_str_eq_cstr(&segs->ptr[0], "claims")) {
        *out = Q(claim)(Q(provider_claim_path_operand_key)(segs->ptr + 1, segs->len - 1));
        return JZ_OK;
    }
    jz_str joined = {0};
    for (u32 i = 0; i < segs->len; i++) {
        if (i) jz_str_push_char(&joined, '.');
        jz_str_push(&joined, segs->ptr[i].ptr, segs->ptr[i].len);
    }
    jz_err e = perr(table, path,
                    "core schema policies only support session.user, session.authMode, and provider claims through session.claims; got session.%.*s",
                    STRF(joined));
    jz_str_drop(&joined);
    return e;
}

static jz_err convert_policy_literal(jazz_model_TableName table, jz_slice path, const jazz_model_public_Value *v, groove_Value *out) {
    switch (v->tag) {
    case JAZZ_PV_NULL: *out = gv(GROOVE_V_NULLABLE); return JZ_OK;
    case JAZZ_PV_BOOLEAN: *out = gv(GROOVE_V_BOOL); out->u.b = v->u.boolean; return JZ_OK;
    case JAZZ_PV_TEXT: *out = gv(GROOVE_V_STRING); out->u.s = jz_str_clone(&v->u.text); return JZ_OK;
    case JAZZ_PV_INTEGER: *out = gv(GROOVE_V_I32); out->u.i32_ = v->u.integer; return JZ_OK;
    case JAZZ_PV_BIGINT: *out = gv(GROOVE_V_I64); out->u.i64_ = v->u.bigint; return JZ_OK;
    case JAZZ_PV_DOUBLE:
        if (!__builtin_isfinite(v->u.dbl)) return perr(table, path, "core schema policy floating-point literals must be finite numbers");
        *out = gv(GROOVE_V_F64);
        out->u.f64_ = v->u.dbl;
        return JZ_OK;
    case JAZZ_PV_TIMESTAMP: *out = gv(GROOVE_V_U64); out->u.u64_ = v->u.timestamp; return JZ_OK;
    case JAZZ_PV_UUID: *out = gv(GROOVE_V_UUID); out->u.uuid = *v->u.uuid; return JZ_OK;
    case JAZZ_PV_BYTEA: *out = gv(GROOVE_V_BYTES); out->u.bytes = jz_bytes_clone(&v->u.bytea); return JZ_OK;
    case JAZZ_PV_ARRAY: {
        groove_ValueVec items = {0};
        for (u32 index = 0; index < v->u.array.len; index++) {
            jz_str sub = sub_path(path, ".Array[%u]", index);
            groove_Value item;
            jz_err e = convert_policy_literal(table, SL(sub), &v->u.array.ptr[index], &item);
            jz_str_drop(&sub);
            if (JZ_IS_ERR(e)) { groove__records__values__ValueVec__drop(&items); return e; }
            JZ_VEC_PUSH(items, item);
        }
        *out = gv(GROOVE_V_ARRAY);
        out->u.items = items;
        return JZ_OK;
    }
    default: {
        jz_str dbg = {0};
        PV(Value__Debug__fmt)(&dbg, v);
        jz_err e = perr(table, path, "core schema policies do not support %.*s literals yet", STRF(dbg));
        jz_str_drop(&dbg);
        return e;
    }
    }
}

/* ---------------------------------------------------------------- convert_table */
static void column_schema_drop_v(void *c) { SC(ColumnSchema__drop)(c); }
static const jazz_model_ColumnDescriptor *descriptor_column(const jazz_model_RowDescriptor *row, const jz_str *name) {
    JZ_VEC_FOR(const jazz_model_ColumnDescriptor, c, row->columns) if (jz_str_eq(c->name, name)) return c;
    return NULL;
}
typedef struct PendingStrategy { const jz_str *column; u8 strategy; } PendingStrategy;

static jz_err convert_table(const jazz_model_public_Schema *schema, jazz_model_TableName name, const jazz_model_public_TableSchema *table,
                            jazz_model_TableSchema *out) {
    jz_btree references;
    jz_btree_init(&references, &jazz_model__schema__STR_STR_DESC);
    jazz_model_ColumnSchemaVec columns = {0};
    JZ_VEC(PendingStrategy) merge_strategies = {0};
    jz_btree column_names;
    jz_btree_init(&column_names, &jazz_model__STR_SET_DESC);
    jz_err e = JZ_OK;
    JZ_VEC_FOR(const jazz_model_ColumnDescriptor, column, table->columns.columns) {
        jz_slice cname = tn(column->name);
        if (slice_is(cname, "id")) {
            e = err(jz_format("$.%.*s.columns.id", TNF(name)), "column name \"id\" is reserved for the automatically generated UUID row ID");
            goto fail_columns;
        }
        if (str_has_prefix(cname, RESERVED_AGGREGATE_OUTPUT_PREFIX)) {
            e = err(jz_format("$.%.*s.columns.%.*s", TNF(name), TNF(column->name)), "column name uses the reserved aggregate-output namespace");
            goto fail_columns;
        }
        if (cname.len && cname.ptr[0] == '$') {
            e = err(jz_format("$.%.*s.columns.%.*s", TNF(name), TNF(column->name)),
                    "column names starting with \"$\" are reserved for magic columns");
            goto fail_columns;
        }
        jazz_model_ColumnSchema converted;
        e = convert_column(name, column, &converted);
        if (JZ_IS_ERR(e)) goto fail_columns;
        u8 strategy = 0;
        if (column->has_merge_strategy) {
            e = convert_merge_strategy(name, column, column->merge_strategy, &strategy);
            if (JZ_IS_ERR(e)) { SC(ColumnSchema__drop)(&converted); goto fail_columns; }
        }
        jz_str key = jz_str_clone(column->name);
        if (jz_btree_insert(&column_names, &key, NULL, NULL)) {
            SC(ColumnSchema__drop)(&converted);
            e = err(jz_format("$.%.*s.columns.%.*s", TNF(name), TNF(column->name)), "duplicate column name");
            goto fail_columns;
        }
        if (column->references) {
            jz_str k = jz_str_clone(column->name), v = jz_str_from_slice(tn(column->references));
            jz_btree_insert(&references, &k, &v, NULL);
        }
        if (column->has_merge_strategy) JZ_VEC_PUSH(merge_strategies, ((PendingStrategy){column->name, strategy}));
        JZ_VEC_PUSH(columns, converted);
    }

    jazz_model_TableSchema converted = SC(TableSchema__new)(jz_str_from_slice(tn(name)), columns);
    JZ_VEC_FOR(const jazz_model_ColumnName, c, table->branch_by) JZ_VEC_PUSH(converted.branch_by, jz_str_from_slice(tn(*c)));
    jz_btree_drop(&converted.references);
    converted.references = references;
    if (table->has_indexed_columns)
        JZ_VEC_FOR(const jazz_model_ColumnName, c, table->indexed_columns) {
            jz_str k = jz_str_from_slice(tn(*c));
            jz_btree_insert(&converted.indexed_columns, &k, NULL, NULL);
        }
    for (u32 index = 0; index < table->composite_indexes.len; index++) {
        const jazz_model_ColumnNameVec *group = &table->composite_indexes.ptr[index];
        if (group->len < 2) {
            e = err(jz_format("$.%.*s.composite_indexes.%u", TNF(name), index), "a composite index needs at least two columns");
            goto fail;
        }
        jz_btree seen;
        jz_btree_init(&seen, &jazz_model__STR_SET_DESC);
        JZ_VEC_FOR(const jazz_model_ColumnName, column, *group) {
            jz_str k = jz_str_clone(*column);
            if (!jz_btree_contains(&column_names, *column) || jz_btree_insert(&seen, &k, NULL, NULL)) {
                e = err(jz_format("$.%.*s.composite_indexes.%u", TNF(name), index), "composite index columns must be distinct declared columns");
                break;
            }
            const jazz_model_ColumnDescriptor *d = descriptor_column(&table->columns, *column);
            if (d) {
                u8 t = d->column_type.tag;
                if (t == JAZZ_CT_BYTEA || t == JAZZ_CT_ARRAY || t == JAZZ_CT_ROW || t == JAZZ_CT_ENUM_PAYLOAD || t == JAZZ_CT_CATALOGUE_ENUM_PAYLOAD) {
                    e = err(jz_format("$.%.*s.composite_indexes.%u", TNF(name), index), "composite index columns must have ordered scalar values");
                    break;
                }
            }
        }
        jz_btree_drop(&seen);
        if (JZ_IS_ERR(e)) goto fail;
        /* Duplicate declarations would hash and serialize twice while the runtime keeps one index; reject them
         * rather than alias two schema identities to one physical layout. */
        jazz_model_StrVec key = {0};
        JZ_VEC_FOR(const jazz_model_ColumnName, column, *group) JZ_VEC_PUSH(key, jz_str_clone(*column));
        if (jz_btree_insert(&converted.composite_indexes, &key, NULL, NULL)) {
            e = err(jz_format("$.%.*s.composite_indexes.%u", TNF(name), index), "duplicate composite index");
            goto fail;
        }
    }
    JZ_VEC_FOR(const PendingStrategy, s, merge_strategies) {
        jz_str k = jz_str_clone(s->column);
        u8 v = s->strategy;
        jz_btree_insert(&converted.merge_strategies, &k, &v, NULL);
    }
    jz_vec_drop(&merge_strategies, sizeof(PendingStrategy), NULL);
    jz_btree_drop(&column_names);
    e = convert_optional_policy(schema, table, name, "policies.select.using", table->policies.select.using_, &converted.read_policy);
    if (JZ_IS_ERR(e)) goto fail_policies;
    jazz_model_Query *update_using, *delete_using;
    e = convert_optional_policy(schema, table, name, "policies.update.using", table->policies.update.using_, &update_using);
    if (JZ_IS_ERR(e)) goto fail_policies;
    converted.write_policies.update_using = update_using;
    e = convert_optional_policy(schema, table, name, "policies.delete.using", table->policies.delete_.using_, &delete_using);
    if (JZ_IS_ERR(e)) goto fail_policies;
    converted.write_policies.delete_using = delete_using;
    e = convert_optional_policy(schema, table, name, "policies.insert.with_check", table->policies.insert.with_check,
                                &converted.write_policies.insert_check);
    if (JZ_IS_ERR(e)) goto fail_policies;
    e = convert_optional_policy(schema, table, name, "policies.update.with_check", table->policies.update.with_check,
                                &converted.write_policies.update_check);
    if (JZ_IS_ERR(e)) goto fail_policies;
    *out = converted;
    return JZ_OK;
fail:
    jz_vec_drop(&merge_strategies, sizeof(PendingStrategy), NULL);
    jz_btree_drop(&column_names);
fail_policies:
    SC(TableSchema__drop)(&converted);
    return e;
fail_columns:
    jz_vec_drop(&columns, sizeof(jazz_model_ColumnSchema), column_schema_drop_v);
    jz_vec_drop(&merge_strategies, sizeof(PendingStrategy), NULL);
    jz_btree_drop(&column_names);
    jz_btree_drop(&references);
    return e;
}
