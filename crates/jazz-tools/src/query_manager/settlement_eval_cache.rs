use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use ahash::AHashSet;

use crate::object::ObjectId;

use super::policy::Operation;
use super::types::{LoadedRow, SchemaHash, TableName, Tuple};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) struct RefAccessSubexprKey {
    pub(crate) branch: String,
    pub(crate) table: TableName,
    pub(crate) id: ObjectId,
    pub(crate) operation: Operation,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) struct RelationSubexprKey {
    pub(crate) site_fingerprint: u64,
    pub(crate) input_fingerprint: u64,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) struct AuthRowLoadKey {
    pub(crate) branch: String,
    pub(crate) auth_schema_hash: SchemaHash,
    pub(crate) id: ObjectId,
}

pub(crate) type AuthRowLoadCache = Rc<RefCell<HashMap<AuthRowLoadKey, Option<LoadedRow>>>>;

#[derive(Debug, Default)]
pub(crate) struct SettlementEvalCache {
    ref_access: HashMap<RefAccessSubexprKey, bool>,
    relation_results: HashMap<RelationSubexprKey, AHashSet<Tuple>>,
    auth_row_loads: AuthRowLoadCache,
}

impl SettlementEvalCache {
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.ref_access.is_empty()
            && self.relation_results.is_empty()
            && self.auth_row_loads.borrow().is_empty()
    }

    pub(crate) fn ref_access_get(&self, key: &RefAccessSubexprKey) -> Option<bool> {
        self.ref_access.get(key).copied()
    }

    pub(crate) fn ref_access_insert(&mut self, key: RefAccessSubexprKey, result: bool) {
        self.ref_access.insert(key, result);
    }

    pub(crate) fn relation_result_get(&self, key: &RelationSubexprKey) -> Option<AHashSet<Tuple>> {
        self.relation_results.get(key).cloned()
    }

    pub(crate) fn relation_result_insert(
        &mut self,
        key: RelationSubexprKey,
        value: AHashSet<Tuple>,
    ) {
        self.relation_results.insert(key, value);
    }

    pub(crate) fn auth_row_load_cache(&self) -> AuthRowLoadCache {
        Rc::clone(&self.auth_row_loads)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ref_access_cache_key_is_branch_scoped() {
        let row_id = ObjectId::new();
        let main_key = RefAccessSubexprKey {
            branch: "main".to_string(),
            table: TableName::new("parent"),
            id: row_id,
            operation: Operation::Select,
        };
        let other_branch_key = RefAccessSubexprKey {
            branch: "preview".to_string(),
            ..main_key.clone()
        };

        let mut cache = SettlementEvalCache::default();
        cache.ref_access_insert(main_key.clone(), true);

        assert_eq!(cache.ref_access_get(&main_key), Some(true));
        assert_eq!(cache.ref_access_get(&other_branch_key), None);
    }
}
