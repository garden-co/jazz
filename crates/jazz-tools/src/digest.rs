use serde::{Deserialize, Serialize};

/// Fixed-width 32-byte digest used for row and batch freshness checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Digest32(pub [u8; 32]);

#[cfg(test)]
mod tests {
    use super::Digest32;

    #[test]
    fn digest32_orders_lexicographically() {
        assert!(Digest32([1; 32]) < Digest32([2; 32]));
    }
}
