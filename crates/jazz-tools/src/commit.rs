use serde::{Deserialize, Serialize};

/// Stable identifier for a row batch member in the history DAG.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CommitId(pub [u8; 32]);

#[cfg(test)]
mod tests {
    use super::CommitId;

    #[test]
    fn commit_id_orders_lexicographically() {
        assert!(CommitId([1; 32]) < CommitId([2; 32]));
    }
}
