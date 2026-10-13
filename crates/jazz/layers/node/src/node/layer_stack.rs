//! The layer-stack overlay engine (SPEC 4 §4.7, SPEC 11 §11.3).
//!
//! A read is a stack of layers, top first, and the top layer holding a row
//! supplies it, whatever its stamp: a pending image is already the settled
//! image with every pending patch folded on top, and a head image already
//! replaces its base row.
//!
//! - a plain current read is `[local, origin]` (`[origin]` at `Global`);
//! - a branch view is `[head@local, head@origin, base@local, base@origin]`,
//!   built as a stack of two branch layers that are each a ref stack;
//! - a frozen-base branch view puts the snapshot at the bottom.
//!
//! Content and the deletion register are stacked separately, each by this
//! rule (`INV-BVIEW-9`, `INV-BVIEW-10`). Every overlay composition goes
//! through [`compose`] (or [`exposed`] for one term of it, and [`top_wins`]
//! for already materialized layers), so the rule lives in one place.
//!
//! A layer's rows may be narrowed by the read's access path; its [`Mask`]
//! says how it hides rows in the layers below it, and is what keeps the
//! composition exact under that narrowing.

use super::*;
use crate::node::query_engine::BranchViewSourceBase;
use crate::protocol::BranchViewBase;

/// How a layer hides the rows of the layers below it. Every mask is a
/// relation keyed by `row_uuid`; the variants say why it is exact.
pub(in crate::node) enum Mask {
    /// The `row_uuid`s of the layer's own rows. Exact when the layer's rows
    /// are every row of the layer that could hide a candidate below: an
    /// unbounded layer, or one bounded by the same row-closed access path
    /// (a primary-key prefix) as the layers below it.
    OwnRows,
    /// The layer's `row_uuid`s, read without the access path that bounds its
    /// rows, so a layer edit that left the predicate still hides the row
    /// below (SPEC 11 "Masking precedes predicates"). Only ids the layers
    /// below hold can hide anything, so the presence is read just for those
    /// ids: a keyed lookup of the layer's stored rows by the lower
    /// candidates' row ids. The local delta masks origin this way under an
    /// index (a pending edit that moved a row out of the prefix still hides
    /// its origin image), and a branch view masks its base by the head's
    /// presence.
    Presence(GraphBuilder),
}

/// One layer of a stack: the row images it supplies and how it masks the
/// layers below it.
pub(in crate::node) struct Layer {
    rows: GraphBuilder,
    mask: Mask,
}

impl Layer {
    /// A layer masking the layers below it by its own rows.
    pub(in crate::node) fn new(rows: GraphBuilder) -> Self {
        Self {
            rows,
            mask: Mask::OwnRows,
        }
    }

    /// A layer masking the layers below it by `mask`.
    pub(in crate::node) fn masked_by(rows: GraphBuilder, mask: Mask) -> Self {
        Self { rows, mask }
    }

    /// The `row_uuid`s this layer hides in every layer below it.
    fn hidden_below(&self) -> GraphBuilder {
        match &self.mask {
            Mask::OwnRows => self.rows.clone(),
            Mask::Presence(rows) => rows.clone(),
        }
        .project(["row_uuid"])
    }
}

/// Compose a stack, top first: the union over every layer of its rows
/// anti-joined against the masks of every layer above it. A one-layer
/// stack is that layer's rows unchanged.
///
/// # Panics
///
/// On an empty stack: every read has at least one layer.
pub(in crate::node) fn compose(layers: impl IntoIterator<Item = Layer>) -> GraphBuilder {
    let mut layers = layers.into_iter().collect::<Vec<_>>();
    let bottom = layers
        .pop()
        .expect("a layer stack has at least one layer")
        .rows;
    layers.into_iter().rev().fold(bottom, |below, layer| {
        let hidden = layer.hidden_below();
        GraphBuilder::union([
            layer.rows,
            GraphBuilder::anti_join(below, hidden, ["row_uuid"], ["row_uuid"]),
        ])
    })
}

/// One term of [`compose`]: the rows of a layer that the layers above it
/// leave exposed.
pub(in crate::node) fn exposed<'a>(
    rows: GraphBuilder,
    above: impl IntoIterator<Item = &'a Layer>,
) -> GraphBuilder {
    above.into_iter().fold(rows, |rows, layer| {
        GraphBuilder::anti_join(rows, layer.hidden_below(), ["row_uuid"], ["row_uuid"])
    })
}

/// The layers of one coordinate's ref stack at `tier`, top first:
/// `[local, origin]`, or `[origin]` at `Global` ([`Ref::stack`]).
pub(in crate::node) fn ref_layers(
    tier: DurabilityTier,
    layer: impl FnMut(Ref) -> Layer,
) -> Vec<Layer> {
    Ref::stack(tier).iter().copied().map(layer).collect()
}

/// [`ref_layers`] for layers whose reads can fail.
pub(in crate::node) fn try_ref_layers<E>(
    tier: DurabilityTier,
    layer: impl FnMut(Ref) -> Result<Layer, E>,
) -> Result<Vec<Layer>, E> {
    Ref::stack(tier).iter().copied().map(layer).collect()
}

/// [`compose`] over layers a node has already materialized, keyed by row:
/// the value of the top layer holding `key`.
pub(in crate::node) fn top_wins<'a, K: Ord + 'a, V>(
    layers: impl IntoIterator<Item = &'a BTreeMap<K, V>>,
    key: &K,
) -> Option<&'a V> {
    layers.into_iter().find_map(|layer| layer.get(key))
}

/// The branch layers of a branch view over one table, top first: the head's
/// key and the base below it, each projected onto the table's branch
/// columns. A base whose key equals the head's collapses into it
/// (`INV-BVIEW-8`), leaving a one-branch stack.
pub(in crate::node) fn branch_view_stack(
    schema: &JazzSchema,
    table: &TableSchema,
    head: &BranchSelector,
    base: Option<&BranchViewBase>,
) -> Result<(BranchKey, Option<BranchViewSourceBase>), String> {
    let (head, _) = schema.project_branch_view_selector(table, head)?;
    let base = base
        .map(|base| match base {
            BranchViewBase::Current(selector) => schema
                .project_branch_view_selector(table, selector)
                .map(|(key, _)| BranchViewSourceBase::Current(key)),
            BranchViewBase::Snapshot { branch, snapshot } => schema
                .project_branch_view_selector(table, branch)
                .map(|(key, _)| BranchViewSourceBase::Snapshot(key, snapshot.clone())),
        })
        .transpose()?
        .filter(|base| match base {
            BranchViewSourceBase::Current(key) | BranchViewSourceBase::Snapshot(key, _) => {
                key != &head
            }
        });
    Ok((head, base))
}
