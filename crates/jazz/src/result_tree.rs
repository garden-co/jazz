//! Canonical structured query results at the Jazz output boundary.

use std::collections::BTreeMap;

use crate::node::CurrentRow;
use crate::tools::OutputOccurrenceId;

/// Ordered recursive result of a query with relation arrays.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResultTree {
    /// Root output occurrences in query order.
    pub roots: Vec<ResultNode>,
}

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
