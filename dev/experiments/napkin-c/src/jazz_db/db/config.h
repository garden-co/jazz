/* config.h: crates/jazz/layers/db/src/db/config.rs */
#ifndef JAZZ_DB_DB_CONFIG_H
#define JAZZ_DB_DB_CONFIG_H
#include "../_ext_db-b.h"

/* Identity attached to locally-authored writes. */
typedef struct jazz_db_DbIdentity { jazz_types_NodeUuid node; jazz_types_AuthorSubject author; } jazz_db_DbIdentity;

/* RowIdSource trait object (Box<dyn RowIdSource>) */
typedef struct jazz_db_RowIdSourceVt { jazz_types_RowUuid (*next_row_id)(void *self); void (*drop)(void *self); } jazz_db_RowIdSourceVt;
typedef struct jazz_db_RowIdSource { void *self; const jazz_db_RowIdSourceVt *vt; } jazz_db_RowIdSource;

/* storage S = BoxedStorage (opaque groove storage handle) */
typedef struct groove_BoxedStorage groove_BoxedStorage;

typedef struct jazz_db_DbConfig {
    jazz_model_JazzSchema schema;
    groove_BoxedStorage *storage;
    jazz_db_DbIdentity identity;
    bool has_id_source; jazz_db_RowIdSource id_source; /* None selects the production source */
} jazz_db_DbConfig;
void jazz_db__db__config__DbConfig__new(jazz_db_DbConfig *out, jazz_model_JazzSchema schema, groove_BoxedStorage *storage, jazz_db_DbIdentity identity);
void jazz_db__db__config__DbConfig__with_id_source(jazz_db_DbConfig *self, jazz_db_RowIdSource id_source);

typedef struct jazz_db_SeededRowIdSource { u64 millis; u64 state; } jazz_db_SeededRowIdSource;
jazz_db_RowIdSource jazz_db__db__config__ProductionRowIdSource__boxed(void);
jazz_types_RowUuid jazz_db__db__config__ProductionRowIdSource__RowIdSource__next_row_id(void *self);
void jazz_db__db__config__SeededRowIdSource__new(jazz_db_SeededRowIdSource *out, u64 seed);
jazz_db_RowIdSource jazz_db__db__config__SeededRowIdSource__boxed(u64 seed);
jazz_types_RowUuid jazz_db__db__config__SeededRowIdSource__RowIdSource__next_row_id(void *self);
#endif
