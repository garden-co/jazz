//! Canonical structured query results at the Jazz output boundary.

use std::collections::BTreeMap;

use crate::ids::RowUuid;
use crate::node::CurrentRow;
use crate::tools::OutputOccurrenceId;

/// Ordered recursive result of a query with relation arrays.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResultTree {
    /// Root output occurrences in query order.
    pub roots: Vec<ResultNode>,
}

/// Maximum rendered size of one complete parent replacement in the structured
/// result boundary. This is deliberately larger than one wire frame: PR 3
/// materializes locally while the existing v3 delivery carrier remains in
/// place.
pub const MAX_RESULT_TREE_PARENT_BYTES: usize = 8 * crate::protocol_limits::MAX_WIRE_FRAME_BYTES;

/// One rendered output occurrence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultNode {
    /// Stable output-occurrence address used for whole-parent replacement.
    pub occurrence: OutputOccurrenceId,
    /// Projected row value.
    pub row: CurrentRow,
    /// Named relation outputs. Their values retain null/hole/empty distinction.
    pub relations: BTreeMap<String, ResultRelation>,
}

/// State of one named relation output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultRelation {
    /// A resolvable relation rendered as an ordered child array (possibly empty).
    Array(Vec<ResultNode>),
    /// An optional relation was explicitly null.
    Null,
    /// A requested relation target is not locally resolvable.
    Hole,
}

impl ResultRelation {
    /// Empty rendered array state.
    pub fn empty() -> Self {
        Self::Array(Vec::new())
    }
}

/// A complete replacement for exactly one output parent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultTreeReplacement {
    /// Address of the replaced parent occurrence.
    pub occurrence: OutputOccurrenceId,
    /// The complete replacement value.
    pub parent: ResultNode,
}

/// Error emitted when a complete parent cannot fit the configured frame budget.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParentTooLargeError {
    /// Source parent row.
    pub parent: RowUuid,
    /// Named relation path which made the rendered parent too large.
    pub relation_path: String,
    /// Rendered byte count.
    pub rendered_bytes: usize,
    /// Configured byte limit.
    pub limit: usize,
}

impl std::fmt::Display for ParentTooLargeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "parent-too-large parent={} relation={} rendered_bytes={} limit={}",
            self.parent.0, self.relation_path, self.rendered_bytes, self.limit
        )
    }
}

impl std::error::Error for ParentTooLargeError {}

impl ResultTree {
    /// Reduce a reset snapshot, replacing all roots.
    pub fn apply_reset(&mut self, reset: ResultTree) {
        *self = reset;
    }

    /// Reduce a complete whole-parent replacement.
    pub fn apply_replacement(&mut self, replacement: ResultTreeReplacement) {
        if let Some(node) = self
            .roots
            .iter_mut()
            .find(|node| node.occurrence == replacement.occurrence)
        {
            *node = replacement.parent;
        } else {
            self.roots.push(replacement.parent);
        }
    }
}
