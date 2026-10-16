/* Port of crates/jazz/layers/node/src/node/database_slot.rs */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

jazz_node_DatabaseSlot jazz_node__node__database_slot__DatabaseSlot__new(groove_Database *db) {
    jazz_node_DatabaseSlot s = { db };
    return s;
}

groove_Database *jazz_node__node__database_slot__DatabaseSlot__take(jazz_node_DatabaseSlot *s) {
    groove_Database *db = s->database;
    if (!db) jz_panic("node database slot must be populated outside rebuild");
    s->database = NULL;
    return db;
}

groove_Database *jazz_node__node__database_slot__DatabaseSlot__into_inner(jazz_node_DatabaseSlot s) {
    return jazz_node__node__database_slot__DatabaseSlot__take(&s);
}

groove_Database *jazz_node__node__database_slot__DatabaseSlot__Deref__deref(const jazz_node_DatabaseSlot *s) {
    if (!s->database) jz_panic("node database slot must be populated");
    return s->database;
}

groove_Database *jazz_node__node__database_slot__DatabaseSlot__DerefMut__deref_mut(jazz_node_DatabaseSlot *s) {
    if (!s->database) jz_panic("node database slot must be populated");
    return s->database;
}
