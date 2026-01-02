/// Merge strategy for combining divergent branches.
pub trait MergeStrategy {
    /// Merge multiple tip contents into one.
    /// `base` is the LCA content (if any), `tips` are the divergent tip contents.
    fn merge(&self, base: Option<&[u8]>, tips: &[&[u8]]) -> Result<Box<[u8]>, &'static str>;
}

/// Simple "last writer wins" merge strategy based on timestamp.
pub struct LastWriterWins;

impl MergeStrategy for LastWriterWins {
    fn merge(&self, _base: Option<&[u8]>, tips: &[&[u8]]) -> Result<Box<[u8]>, &'static str> {
        // Just pick the last tip (caller should sort by timestamp)
        tips.last()
            .map(|t| t.to_vec().into_boxed_slice())
            .ok_or("no tips to merge")
    }
}
