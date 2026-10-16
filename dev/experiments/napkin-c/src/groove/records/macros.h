/* groove::records::macros (crates/groove/src/records/macros.rs)
 * RecordField trait -> vtable; one C copy per primitive impl, Option<T>/(A,B) take inner vtables. */
#ifndef GROOVE_RECORDS_MACROS_H
#define GROOVE_RECORDS_MACROS_H
#include "jz_rt.h"
#include "values.h"
#include "mod.h"

typedef enum {
    GROOVE_FK_U64, GROOVE_FK_U48, GROOVE_FK_I64, GROOVE_FK_U32, GROOVE_FK_I32, GROOVE_FK_F64, GROOVE_FK_U8,
    GROOVE_FK_BOOL, GROOVE_FK_BYTES, GROOVE_FK_UUID, GROOVE_FK_STRING, GROOVE_FK_ENUM, GROOVE_FK_PAYLOAD_ENUM,
    GROOVE_FK_TUPLE, GROOVE_FK_ARRAY, GROOVE_FK_NULLABLE, GROOVE_FK_RECORD,
} groove_FieldKind;
bool groove__records__macros__FieldKind__matches(groove_FieldKind kind, const groove_ValueType *t);

typedef struct groove_RecordFieldVtbl groove_RecordFieldVtbl;
struct groove_RecordFieldVtbl {
    usize size; /* sizeof(Self) */
    jz_err (*read)(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *record, u32 idx, void *out);
    groove_Value (*to_value)(const groove_RecordFieldVtbl *vt, const void *self);
    groove_FieldKind column_kind;
    jz_err (*read_raw)(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out);
    jz_err (*read_tuple_raw)(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out);
    const groove_RecordFieldVtbl *inner_a; /* Option<T>: T; (A, B): A */
    const groove_RecordFieldVtbl *inner_b; /* (A, B): B */
};

extern const groove_RecordFieldVtbl groove__records__macros__RecordField_u64;
extern const groove_RecordFieldVtbl groove__records__macros__RecordField_U48;
extern const groove_RecordFieldVtbl groove__records__macros__RecordField_i64;
extern const groove_RecordFieldVtbl groove__records__macros__RecordField_i32;
extern const groove_RecordFieldVtbl groove__records__macros__RecordField_u32;
extern const groove_RecordFieldVtbl groove__records__macros__RecordField_f64;
extern const groove_RecordFieldVtbl groove__records__macros__RecordField_u8;
extern const groove_RecordFieldVtbl groove__records__macros__RecordField_bool;
extern const groove_RecordFieldVtbl groove__records__macros__RecordField_bytes;
extern const groove_RecordFieldVtbl groove__records__macros__RecordField_uuid;
extern const groove_RecordFieldVtbl groove__records__macros__RecordField_string;
extern const groove_RecordFieldVtbl groove__records__macros__RecordField_value;
extern const groove_RecordFieldVtbl groove__records__macros__RecordField_value_vec;
extern const groove_RecordFieldVtbl groove__records__macros__RecordField_owned_record;
extern const groove_RecordFieldVtbl groove__records__macros__RecordField_enum_value;

/* Option<T> value: bool has + T payload laid out after it (aligned to 8) */
typedef struct groove_OptionField { bool has; u8 _pad[7]; u8 payload[]; } groove_OptionField;
jz_err groove__records__macros__Option__RecordField__read(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *record, u32 idx, void *out);
groove_Value groove__records__macros__Option__RecordField__to_value(const groove_RecordFieldVtbl *vt, const void *self);
jz_err groove__records__macros__Option__RecordField__read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out);
jz_err groove__records__macros__Option__RecordField__read_tuple_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out);
/* (A, B): A at offset 0, B at offset round_up(sizeof A, 8) */
jz_err groove__records__macros__Pair__RecordField__read(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *record, u32 idx, void *out);
groove_Value groove__records__macros__Pair__RecordField__to_value(const groove_RecordFieldVtbl *vt, const void *self);
jz_err groove__records__macros__Pair__RecordField__read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out);

jz_err groove__records__macros__U48__new(u64 value, u64 *out);
void groove__records__macros__assert_record_field_layout(groove_RecordDescriptor d, u32 idx, const char *name, groove_FieldKind kind);
#endif
