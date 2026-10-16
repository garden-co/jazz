/* groove::schema (crates/groove/src/schema.rs)
 * serde Serialize/Deserialize derives are not ported: they have no rows in
 * the browser build. Layout follows the mirror in _ext_groove-ivm-db.h. */
#ifndef GROOVE_SCHEMA_H
#define GROOVE_SCHEMA_H
#include "jz_rt.h"
#include "records/values.h"
#include "records/mod.h"

typedef groove_ValueType groove_ColumnType;

typedef struct groove_ColumnSchema { jz_str name; groove_ColumnType column_type; } groove_ColumnSchema;
typedef JZ_VEC(groove_ColumnSchema) groove_ColumnSchemaVec;

enum { GROOVE_PK_INTEGER = 0, GROOVE_PK_BOOL, GROOVE_PK_STRING, GROOVE_PK_BYTES, GROOVE_PK_UUID };
enum { GROOVE_INT_KEY_U8 = 0, GROOVE_INT_KEY_U16, GROOVE_INT_KEY_U32, GROOVE_INT_KEY_U64 };
typedef struct groove_PrimaryKeyType { u8 tag; u8 integer; } groove_PrimaryKeyType;
typedef struct groove_PrimaryKeyColumn { jz_str column; groove_PrimaryKeyType key_type; } groove_PrimaryKeyColumn;
typedef JZ_VEC(groove_PrimaryKeyColumn) groove_PrimaryKeyColumnVec;
typedef struct groove_PrimaryKey { groove_PrimaryKeyColumnVec columns; bool generated; } groove_PrimaryKey;

typedef struct groove_TableVariantField { jz_str name; groove_ValueType value_type; bool has_shared_column; jz_str shared_column; } groove_TableVariantField;
typedef JZ_VEC(groove_TableVariantField) groove_TableVariantFieldVec;
typedef struct groove_TableVariant { u32 tag; groove_StrVec fields; groove_TableVariantFieldVec payload_fields; } groove_TableVariant;
typedef JZ_VEC(groove_TableVariant) groove_TableVariantVec;

typedef struct groove_IndexSchema { jz_str name; groove_StrVec columns; bool unique; } groove_IndexSchema;
typedef JZ_VEC(groove_IndexSchema) groove_IndexSchemaVec;
typedef struct groove_ForeignKey { jz_str name; groove_StrVec columns; jz_str referenced_table; groove_StrVec referenced_columns; } groove_ForeignKey;
typedef JZ_VEC(groove_ForeignKey) groove_ForeignKeyVec;

typedef struct groove_TableSchema {
    jz_str name;
    u64 variant_registry_id;
    jz_btree value_variant_registries; /* BTreeMap<u64, VariantRegistry> */
    groove_ColumnSchemaVec columns;
    bool has_primary_key;
    groove_PrimaryKey primary_key;
    groove_IndexSchemaVec indices;
    groove_ForeignKeyVec foreign_keys;
    groove_TableVariantVec variants;
} groove_TableSchema;
typedef JZ_VEC(groove_TableSchema) groove_TableSchemaVec;

typedef struct groove_KeyField { jz_str name; groove_ValueType value_type; } groove_KeyField;
typedef JZ_VEC(groove_KeyField) groove_KeyFieldVec;
typedef struct groove_DirectRecordStoreSchema { jz_str name; groove_KeyFieldVec key; groove_KeyFieldVec value; } groove_DirectRecordStoreSchema;
typedef JZ_VEC(groove_DirectRecordStoreSchema) groove_DirectRecordStoreSchemaVec;
typedef struct groove_DatabaseSchema { groove_TableSchemaVec tables; groove_DirectRecordStoreSchemaVec direct_record_stores; } groove_DatabaseSchema;

/* DatabaseSchema (consumes tables) */
groove_DatabaseSchema groove__schema__DatabaseSchema__new(groove_TableSchemaVec tables);
groove_DatabaseSchema groove__schema__DatabaseSchema__with_direct_record_store(groove_DatabaseSchema self, groove_DirectRecordStoreSchema store);
const groove_TableSchema *groove__schema__DatabaseSchema__table(const groove_DatabaseSchema *s, const char *name, u32 len);
const groove_DirectRecordStoreSchema *groove__schema__DatabaseSchema__direct_record_store(const groove_DatabaseSchema *s, const char *name, u32 len);
typedef JZ_VEC(jz_slice) groove_SliceVec;
groove_SliceVec groove__schema__DatabaseSchema__column_families(const groove_DatabaseSchema *s);
void groove__schema__DatabaseSchema__Clone__clone(groove_DatabaseSchema *dst, const groove_DatabaseSchema *src);
void groove__schema__DatabaseSchema__drop(groove_DatabaseSchema *s);
bool groove__schema__DatabaseSchema__PartialEq__eq(const groove_DatabaseSchema *a, const groove_DatabaseSchema *b);

/* DirectRecordStoreSchema */
groove_DirectRecordStoreSchema groove__schema__DirectRecordStoreSchema__new(jz_str name, groove_RecordDescriptor key, groove_RecordDescriptor value);
groove_RecordDescriptor groove__schema__DirectRecordStoreSchema__key_descriptor(const groove_DirectRecordStoreSchema *s);
groove_RecordDescriptor groove__schema__DirectRecordStoreSchema__value_descriptor(const groove_DirectRecordStoreSchema *s);
void groove__schema__DirectRecordStoreSchema__Clone__clone(void *dst, const void *src);
void groove__schema__DirectRecordStoreSchema__drop(void *s);

/* TableSchema (constructors consume columns) */
groove_TableSchema groove__schema__TableSchema__new(jz_str name, groove_ColumnSchemaVec columns);
groove_TableSchema groove__schema__TableSchema__new_with_bound_registries(jz_str name, groove_ColumnSchemaVec columns);
groove_TableSchema groove__schema__TableSchema__with_primary_key(groove_TableSchema self, groove_PrimaryKey primary_key);
groove_TableSchema groove__schema__TableSchema__with_index(groove_TableSchema self, groove_IndexSchema index);
groove_TableSchema groove__schema__TableSchema__with_foreign_key(groove_TableSchema self, groove_ForeignKey foreign_key);
groove_TableSchema groove__schema__TableSchema__with_variant(groove_TableSchema self, u32 tag, groove_StrVec fields);
groove_TableSchema groove__schema__TableSchema__with_variant_payload(groove_TableSchema self, u32 tag, groove_TableVariantFieldVec fields);
bool groove__schema__TableSchema__has_variants(const groove_TableSchema *t);
const groove_TableVariant *groove__schema__TableSchema__variant(const groove_TableSchema *t, u32 tag);
bool groove__schema__TableSchema__record_schema_for_variant(const groove_TableSchema *t, u32 tag, groove_RecordDescriptor *out);
groove_RecordDescriptor groove__schema__TableSchema__record_schema(const groove_TableSchema *t);
void groove__schema__TableSchema__Clone__clone(groove_TableSchema *dst, const groove_TableSchema *src);
void groove__schema__TableSchema__drop(groove_TableSchema *t);
void groove__schema__TableSchema__drop_v(void *t);
bool groove__schema__TableSchema__PartialEq__eq(const groove_TableSchema *a, const groove_TableSchema *b);

/* TableVariant / TableVariantField */
groove_TableVariant groove__schema__TableVariant__new(u32 tag, groove_StrVec fields);
groove_TableVariant groove__schema__TableVariant__with_payload(u32 tag, groove_TableVariantFieldVec fields);
const jz_str *groove__schema__TableVariant__payload_name_for_shared(const groove_TableVariant *v, const jz_str *shared);
void groove__schema__TableVariant__Clone__clone(groove_TableVariant *dst, const groove_TableVariant *src);
void groove__schema__TableVariant__drop(groove_TableVariant *v);
groove_TableVariantField groove__schema__TableVariantField__local(jz_str name, groove_ValueType value_type);
groove_TableVariantField groove__schema__TableVariantField__shared(jz_str name, groove_ValueType value_type, jz_str shared_column);

/* ColumnSchema */
groove_ColumnSchema groove__schema__ColumnSchema__new(jz_str name, groove_ColumnType column_type);
bool groove__schema__ColumnSchema__PartialEq__eq(const groove_ColumnSchema *a, const groove_ColumnSchema *b);
void groove__schema__ColumnSchema__Clone__clone(groove_ColumnSchema *dst, const groove_ColumnSchema *src);
void groove__schema__ColumnSchema__drop(void *c);

/* PrimaryKey / PrimaryKeyColumn / PrimaryKeyType / IntegerKeyType */
groove_PrimaryKey groove__schema__PrimaryKey__new(jz_str column, u8 integer_type);
groove_PrimaryKey groove__schema__PrimaryKey__composite(groove_PrimaryKeyColumnVec columns);
groove_PrimaryKey groove__schema__PrimaryKey__user_supplied(groove_PrimaryKey self);
groove_PrimaryKeyColumn groove__schema__PrimaryKeyColumn__new(jz_str column, groove_PrimaryKeyType key_type);
groove_PrimaryKeyColumn groove__schema__PrimaryKeyColumn__integer(jz_str column, u8 integer_type);
groove_PrimaryKeyColumn groove__schema__PrimaryKeyColumn__bytes(jz_str column);
groove_PrimaryKeyColumn groove__schema__PrimaryKeyColumn__uuid(jz_str column);
groove_ColumnType groove__schema__PrimaryKeyType__column_type(const groove_PrimaryKeyType *t);
groove_ColumnType groove__schema__IntegerKeyType__column_type(u8 integer_type);

/* IndexSchema / ForeignKey */
groove_IndexSchema groove__schema__IndexSchema__new(jz_str name, groove_StrVec columns);
groove_IndexSchema groove__schema__IndexSchema__unique(groove_IndexSchema self);
groove_ForeignKey groove__schema__ForeignKey__new(jz_str name, groove_StrVec columns, jz_str referenced_table, groove_StrVec referenced_columns);

#endif
