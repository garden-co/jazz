/* crates/jazz/layers/types/src/time.rs */
#include "jazz_types.h"

static jz_err hlc_overflow(void) {
    return jz_err_fmt(JAZZ_TYPES_ERR_HLC_OVERFLOW, "packed HLC exhausted at physical millisecond %llu",
                      (unsigned long long)JAZZ_HLC_MAX_PHYSICAL_MS);
}

/* fn tick_packed(registered_physical_ms, registered_counter, now_ms) -> Result<(u64, u32), HlcOverflow> */
static jz_err jazz_types__time__tick_packed(u64 registered_physical_ms, u32 registered_counter, u64 now_ms,
                                            u64 *out_ms, u32 *out_counter) {
    if (now_ms > registered_physical_ms) {
        if (now_ms > JAZZ_HLC_MAX_PHYSICAL_MS) return hlc_overflow();
        *out_ms = now_ms; *out_counter = 0;
        return JZ_OK;
    }
    if (registered_counter < JAZZ_HLC_MAX_LOGICAL_COUNTER) {
        *out_ms = registered_physical_ms; *out_counter = registered_counter + 1;
        return JZ_OK;
    }
    if (registered_physical_ms == UINT64_MAX) return hlc_overflow(); /* checked_add */
    u64 physical_ms = registered_physical_ms + 1;
    if (physical_ms > JAZZ_HLC_MAX_PHYSICAL_MS) return hlc_overflow();
    *out_ms = physical_ms; *out_counter = 0;
    return JZ_OK;
}

static inline u64 pack(u64 physical_ms, u32 counter) { return (physical_ms << JAZZ_HLC_COUNTER_BITS) | (u64)counter; }

bool jazz_types__time__GlobalTime__new(u64 physical_ms, u32 counter, jazz_types_GlobalTime *out) {
    if (physical_ms <= JAZZ_HLC_MAX_PHYSICAL_MS && counter <= JAZZ_HLC_MAX_LOGICAL_COUNTER) {
        *out = pack(physical_ms, counter);
        return true;
    }
    return false;
}
u64 jazz_types__time__GlobalTime__physical_ms(jazz_types_GlobalTime t) { return t >> JAZZ_HLC_COUNTER_BITS; }
u32 jazz_types__time__GlobalTime__counter(jazz_types_GlobalTime t) { return (u32)(t & (u64)JAZZ_HLC_MAX_LOGICAL_COUNTER); }

jz_err jazz_types__time__GlobalTime__tick(jazz_types_GlobalTime reg, u64 now_ms, jazz_types_GlobalTime *out) {
    u64 ms; u32 c;
    JZ_TRY(jazz_types__time__tick_packed(jazz_types__time__GlobalTime__physical_ms(reg),
                                         jazz_types__time__GlobalTime__counter(reg), now_ms, &ms, &c));
    *out = pack(ms, c);
    return JZ_OK;
}

u64 jazz_types__time__GlobalTime__authority_now_ms(u64 now_ms, u64 fallback_ms) {
    if (now_ms <= JAZZ_HLC_MAX_PHYSICAL_MS) return now_ms;
    return fallback_ms < JAZZ_HLC_MAX_PHYSICAL_MS ? fallback_ms : JAZZ_HLC_MAX_PHYSICAL_MS;
}

jazz_types_TxTime jazz_types__time__TxTime__new(u64 physical_ms, u32 counter) {
    JZ_ASSERT(physical_ms <= JAZZ_HLC_MAX_PHYSICAL_MS, "HLC physical component exceeds 46-bit packed range");
    JZ_ASSERT(counter <= JAZZ_HLC_MAX_LOGICAL_COUNTER, "HLC logical counter exceeds 18-bit packed range");
    return pack(physical_ms, counter);
}

jz_err jazz_types__time__TxTime__from_physical_ms(u64 ms, jazz_types_TxTime *out) {
    if (ms <= JAZZ_HLC_MAX_PHYSICAL_MS) {
        *out = ms << JAZZ_HLC_COUNTER_BITS;
        return JZ_OK;
    }
    return jz_err_fmt(JAZZ_TYPES_ERR_HLC_PHYSICAL_OUT_OF_RANGE,
                      "physical millisecond %llu exceeds the packed HLC maximum %llu",
                      (unsigned long long)ms, (unsigned long long)JAZZ_HLC_MAX_PHYSICAL_MS);
}
u64 jazz_types__time__TxTime__physical_ms(jazz_types_TxTime t) { return t >> JAZZ_HLC_COUNTER_BITS; }
u32 jazz_types__time__TxTime__counter(jazz_types_TxTime t) { return (u32)(t & (u64)JAZZ_HLC_MAX_LOGICAL_COUNTER); }

jz_err jazz_types__time__TxTime__tick_after(jazz_types_TxTime t, jazz_types_TxTime *out) {
    u64 ms; u32 c;
    JZ_TRY(jazz_types__time__tick_packed(jazz_types__time__TxTime__physical_ms(t), jazz_types__time__TxTime__counter(t),
                                         jazz_types__time__TxTime__physical_ms(t), &ms, &c));
    *out = pack(ms, c);
    return JZ_OK;
}
jz_err jazz_types__time__TxTime__tick(jazz_types_TxTime reg, u64 now_ms, jazz_types_TxTime *out) {
    u64 ms; u32 c;
    JZ_TRY(jazz_types__time__tick_packed(jazz_types__time__TxTime__physical_ms(reg), jazz_types__time__TxTime__counter(reg),
                                         now_ms, &ms, &c));
    *out = pack(ms, c);
    return JZ_OK;
}
jazz_types_TxTime jazz_types__time__TxTime__From_u64__from(u64 ms) {
    jazz_types_TxTime t;
    jz_err e = jazz_types__time__TxTime__from_physical_ms(ms, &t);
    if (e.code) jz_panic("TxTime::from requires a physical millisecond in the packed HLC range");
    return t;
}

/* serde: newtype structs over u64 (postcard varint) */
void jazz_types__time__TxTime__Serialize__serialize(jz_pc_w *w, jazz_types_TxTime t) { jz_pc_varint(w, t); }
jz_err jazz_types__time__TxTime__Deserialize__deserialize(jz_pc_r *r, jazz_types_TxTime *out) { return jz_pc_r_varint(r, out); }
void jazz_types__time__GlobalTime__Serialize__serialize(jz_pc_w *w, jazz_types_GlobalTime t) { jz_pc_varint(w, t); }
jz_err jazz_types__time__GlobalTime__Deserialize__deserialize(jz_pc_r *r, jazz_types_GlobalTime *out) { return jz_pc_r_varint(r, out); }

void jazz_types__time__TxTime__Debug__fmt(jz_str *out, jazz_types_TxTime t) { jz_str_appendf(out, "TxTime(%llu)", (unsigned long long)t); }
void jazz_types__time__GlobalTime__Debug__fmt(jz_str *out, jazz_types_GlobalTime t) { jz_str_appendf(out, "GlobalTime(%llu)", (unsigned long long)t); }

/* groove::impl_record_field_u64!(TxTime); groove::impl_record_field_u64!(GlobalTime):
 * RecordField impls map to Value::U64 / ValueType::U64. */
groove_Value jazz_types__time__TxTime__RecordField__to_value(jazz_types_TxTime t) {
    groove_Value v; memset(&v, 0, sizeof v); v.tag = GROOVE_V_U64; v.u.u64_ = t; return v;
}
jz_err jazz_types__time__TxTime__RecordField__from_record(const groove_BorrowedRecord *r, u32 idx, jazz_types_TxTime *out) {
    return groove__records__BorrowedRecord__get_u64(r, idx, out);
}
groove_Value jazz_types__time__GlobalTime__RecordField__to_value(jazz_types_GlobalTime t) {
    groove_Value v; memset(&v, 0, sizeof v); v.tag = GROOVE_V_U64; v.u.u64_ = t; return v;
}
jz_err jazz_types__time__GlobalTime__RecordField__from_record(const groove_BorrowedRecord *r, u32 idx, jazz_types_GlobalTime *out) {
    return groove__records__BorrowedRecord__get_u64(r, idx, out);
}
