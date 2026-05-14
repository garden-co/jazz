use std::collections::HashMap;

use crate::object::ObjectId;

use super::policy::Operation;
use super::types::TableName;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) struct RefAccessSubexprKey {
    pub(crate) table: TableName,
    pub(crate) id: ObjectId,
    pub(crate) operation: Operation,
    pub(crate) parent_eval_depth: usize,
}

#[derive(Debug, Default)]
pub(crate) struct SettlementEvalCache {
    ref_access: HashMap<RefAccessSubexprKey, bool>,
}

impl SettlementEvalCache {
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.ref_access.is_empty()
    }

    pub(crate) fn ref_access_get(&self, key: &RefAccessSubexprKey) -> Option<bool> {
        self.ref_access.get(key).copied()
    }

    pub(crate) fn ref_access_insert(&mut self, key: RefAccessSubexprKey, result: bool) {
        self.ref_access.insert(key, result);
    }
}
