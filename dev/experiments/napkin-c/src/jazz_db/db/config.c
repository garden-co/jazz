/* config.c: crates/jazz/layers/db/src/db/config.rs
 * Database construction identity, storage, and row-id configuration. */
#include "config.h"

/* Build a config using the production row id source. */
void jazz_db__db__config__DbConfig__new(jazz_db_DbConfig *out, jazz_model_JazzSchema schema, groove_BoxedStorage *storage, jazz_db_DbIdentity identity) {
    out->schema = schema;
    out->storage = storage;
    out->identity = identity;
    out->has_id_source = false;
    memset(&out->id_source, 0, sizeof out->id_source);
}

/* Override the row id source, typically with SeededRowIdSource in tests. */
void jazz_db__db__config__DbConfig__with_id_source(jazz_db_DbConfig *self, jazz_db_RowIdSource id_source) {
    if (self->has_id_source && self->id_source.vt->drop) self->id_source.vt->drop(self->id_source.self);
    self->has_id_source = true;
    self->id_source = id_source;
}

/* Production row id source using the system clock and OS randomness. */
jazz_types_RowUuid jazz_db__db__config__ProductionRowIdSource__RowIdSource__next_row_id(void *self) {
    (void)self;
    u8 rand[10];
    jz_random_bytes(rand, sizeof rand);
    return jz_uuid_v7(jz_now_ms(), rand);
}
static const jazz_db_RowIdSourceVt production_row_id_source_vt = {
    jazz_db__db__config__ProductionRowIdSource__RowIdSource__next_row_id, NULL,
};
jazz_db_RowIdSource jazz_db__db__config__ProductionRowIdSource__boxed(void) {
    jazz_db_RowIdSource s = { NULL, &production_row_id_source_vt };
    return s;
}

/* Create a deterministic source from a caller-provided seed. */
void jazz_db__db__config__SeededRowIdSource__new(jazz_db_SeededRowIdSource *out, u64 seed) {
    out->millis = seed & ((1ull << 48) - 1);
    out->state = seed ^ 0x9e3779b97f4a7c15ull;
}

static u64 jazz_db__db__config__splitmix64(u64 *state) {
    *state += 0x9e3779b97f4a7c15ull;
    u64 z = *state;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ull;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebull;
    return z ^ (z >> 31);
}

jazz_types_RowUuid jazz_db__db__config__SeededRowIdSource__RowIdSource__next_row_id(void *p) {
    jazz_db_SeededRowIdSource *self = p;
    u64 millis = self->millis & ((1ull << 48) - 1);
    self->millis += 1;
    u16 rand_a = (u16)(jazz_db__db__config__splitmix64(&self->state) & 0x0fff);
    u64 rand_b = jazz_db__db__config__splitmix64(&self->state) & ((1ull << 62) - 1);
    jz_uuid out;
    for (int i = 0; i < 6; ++i) out.b[i] = (u8)(millis >> (8 * (5 - i)));
    u16 version_and_rand_a = (u16)(0x7000u | rand_a);
    out.b[6] = (u8)(version_and_rand_a >> 8);
    out.b[7] = (u8)version_and_rand_a;
    u64 variant_and_rand_b = 0x8000000000000000ull | rand_b;
    for (int i = 0; i < 8; ++i) out.b[8 + i] = (u8)(variant_and_rand_b >> (8 * (7 - i)));
    return out;
}

static void seeded_drop(void *p) { jz_free(p); }
static const jazz_db_RowIdSourceVt seeded_row_id_source_vt = {
    jazz_db__db__config__SeededRowIdSource__RowIdSource__next_row_id, seeded_drop,
};
jazz_db_RowIdSource jazz_db__db__config__SeededRowIdSource__boxed(u64 seed) {
    jazz_db_SeededRowIdSource *s = JZ_NEW(jazz_db_SeededRowIdSource);
    jazz_db__db__config__SeededRowIdSource__new(s, seed);
    jazz_db_RowIdSource out = { s, &seeded_row_id_source_vt };
    return out;
}
