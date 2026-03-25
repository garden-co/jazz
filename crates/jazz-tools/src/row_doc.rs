use crate::object::ObjectId;
use std::collections::HashMap;
use yrs::{Doc, MapRef};

pub struct RowDoc {
    pub id: ObjectId,
    pub doc: Doc,
    pub root_map: MapRef, // cached at construction — avoids deadlock with active transactions
    pub metadata: HashMap<String, String>,
    pub branches: HashMap<String, ObjectId>,
    pub origin: Option<(ObjectId, Vec<u8>)>, // (parent_id, state_vector_at_fork)
}

impl RowDoc {
    pub fn new(id: ObjectId, metadata: HashMap<String, String>) -> Self {
        let doc = Doc::new();
        let root_map = doc.get_or_insert_map("row");
        Self {
            id,
            doc,
            root_map,
            metadata,
            branches: HashMap::new(),
            origin: None,
        }
    }
}
