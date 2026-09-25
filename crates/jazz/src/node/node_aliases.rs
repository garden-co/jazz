//! Bidirectional mapping between stable node UUIDs and compact on-disk aliases.

use std::collections::BTreeMap;
use std::collections::btree_map;

use rustc_hash::FxHashMap;

use crate::ids::{NodeAlias, NodeUuid};

/// The in-memory `jazz_nodes` catalogue.
///
/// Every stored version names its writer by [`NodeAlias`], so resolving a
/// version's transaction id is an alias → UUID lookup on hot read, publish and
/// ingest paths. The reverse index keeps that lookup O(1) instead of scanning
/// every node the process has ever seen. The two directions are a bijection:
/// recovery rejects a durable catalogue that maps one alias to two UUIDs (or
/// one UUID to two aliases), and `insert` keeps both indexes in lockstep.
#[derive(Clone, Debug, Default)]
pub(crate) struct NodeAliases {
    by_node: BTreeMap<NodeUuid, NodeAlias>,
    by_alias: FxHashMap<NodeAlias, NodeUuid>,
    /// Highest alias ever installed. Monotonic: `remove` does not lower it,
    /// which is conservative for allocating the next alias.
    max_alias: u64,
    /// Counts dropped `node ↔ alias` pairs (a replacement of either side, or
    /// a removal). Inserting a fresh pair never lowers it: a consumer that
    /// observed an unchanged count knows every alias it resolved still
    /// resolves to the same node.
    retargets: u64,
}

impl NodeAliases {
    pub(crate) fn get(&self, node: &NodeUuid) -> Option<&NodeAlias> {
        self.by_node.get(node)
    }

    pub(crate) fn contains_key(&self, node: &NodeUuid) -> bool {
        self.by_node.contains_key(node)
    }

    /// The node that owns `alias`, if the mapping is resident.
    pub(crate) fn node_for_alias(&self, alias: NodeAlias) -> Option<NodeUuid> {
        self.by_alias.get(&alias).copied()
    }

    /// Install `node ↔ alias`, replacing any previous mapping of either side
    /// so the two indexes stay a bijection.
    pub(crate) fn insert(&mut self, node: NodeUuid, alias: NodeAlias) -> Option<NodeAlias> {
        self.max_alias = self.max_alias.max(alias.0);
        if let Some(previous_node) = self.by_alias.insert(alias, node)
            && previous_node != node
        {
            self.by_node.remove(&previous_node);
            self.retargets += 1;
        }
        let previous_alias = self.by_node.insert(node, alias);
        if let Some(previous_alias) = previous_alias
            && previous_alias != alias
        {
            self.by_alias.remove(&previous_alias);
            self.retargets += 1;
        }
        previous_alias
    }

    /// See the `retargets` field.
    pub(crate) fn retarget_count(&self) -> u64 {
        self.retargets
    }

    /// Highest alias ever installed, or 0 when none was.
    pub(crate) fn max_alias(&self) -> u64 {
        self.max_alias
    }

    #[cfg(test)]
    pub(crate) fn remove(&mut self, node: &NodeUuid) -> Option<NodeAlias> {
        let alias = self.by_node.remove(node)?;
        self.by_alias.remove(&alias);
        self.retargets += 1;
        Some(alias)
    }

    pub(crate) fn iter(&self) -> btree_map::Iter<'_, NodeUuid, NodeAlias> {
        self.by_node.iter()
    }

    pub(crate) fn len(&self) -> usize {
        self.by_node.len()
    }
}

impl PartialEq for NodeAliases {
    fn eq(&self, other: &Self) -> bool {
        // `by_alias` is derived from `by_node`.
        self.by_node == other.by_node
    }
}

impl Eq for NodeAliases {}

impl std::ops::Index<&NodeUuid> for NodeAliases {
    type Output = NodeAlias;

    fn index(&self, node: &NodeUuid) -> &NodeAlias {
        &self.by_node[node]
    }
}

impl FromIterator<(NodeUuid, NodeAlias)> for NodeAliases {
    fn from_iter<I: IntoIterator<Item = (NodeUuid, NodeAlias)>>(iter: I) -> Self {
        let mut aliases = Self::default();
        for (node, alias) in iter {
            aliases.insert(node, alias);
        }
        aliases
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(byte: u8) -> NodeUuid {
        NodeUuid(uuid::Uuid::from_bytes([byte; 16]))
    }

    // Internal: the reverse index is private state. Public APIs only ever
    // install fresh aliases, so replacement and removal cannot be driven
    // through them; this pins that both directions stay a bijection.
    #[test]
    fn reverse_lookup_tracks_inserts_replacements_and_removals() {
        let mut aliases = NodeAliases::default();
        assert_eq!(aliases.insert(node(1), NodeAlias(1)), None);
        assert_eq!(aliases.insert(node(2), NodeAlias(2)), None);
        assert_eq!(aliases.node_for_alias(NodeAlias(1)), Some(node(1)));
        assert_eq!(aliases.node_for_alias(NodeAlias(2)), Some(node(2)));
        assert_eq!(aliases.node_for_alias(NodeAlias(3)), None);

        // Re-aliasing a node retires its old alias.
        assert_eq!(aliases.insert(node(1), NodeAlias(3)), Some(NodeAlias(1)));
        assert_eq!(aliases.node_for_alias(NodeAlias(1)), None);
        assert_eq!(aliases.node_for_alias(NodeAlias(3)), Some(node(1)));

        // Claiming an alias retires the node that held it.
        aliases.insert(node(4), NodeAlias(2));
        assert_eq!(aliases.get(&node(2)), None);
        assert_eq!(aliases.node_for_alias(NodeAlias(2)), Some(node(4)));
        assert_eq!(aliases.len(), 2);

        assert_eq!(aliases.remove(&node(4)), Some(NodeAlias(2)));
        assert_eq!(aliases.node_for_alias(NodeAlias(2)), None);
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases.max_alias(), 3);
    }
}
