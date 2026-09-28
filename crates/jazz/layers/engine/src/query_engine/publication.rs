//! Publication roles of current-row fields: which fields are application
//! cells, public provenance or engine metadata, and under which name each is
//! published.

use crate::ids::PhysicalColumnId;

/// Constructor-time source or logical role before publication is finalized.
///
/// This role is never serialized. The single publication metadata owner below
/// carries authoritative catalogue IDs, names and application-cell visibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CurrentRowBindingRole {
    /// A persisted CurrentRow field using Jazz's private physical name.
    PhysicalColumn,
    /// A query, relation, or collector field using its public logical name.
    LogicalField,
}

/// Application cells, public provenance, and private engine metadata have
/// different publication roles even when their names happen to be identical.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CurrentRowResultVisibility {
    /// A query-visible application cell, included in subscription cell comparison.
    ApplicationCell,
    /// Public magic provenance, available to explicit projections and row metadata.
    PublicProvenance,
    /// Engine bookkeeping carried only for decoding/internal identity.
    HiddenMetadata,
}

impl CurrentRowResultVisibility {
    /// Classify metadata constructed by the CurrentRow producer, not a wire
    /// field guessed by a consumer. Explicit application outputs bypass this.
    pub fn current_row_metadata(name: &str) -> Self {
        match name {
            "$createdBy" | "$createdAt" | "$updatedBy" | "$updatedAt" => Self::PublicProvenance,
            _ => Self::HiddenMetadata,
        }
    }
}

/// One producer-owned publication binding. Runtime query slots are separate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CurrentRowPublicationField {
    /// A source application cell with its authoritative catalogue identity.
    StoredColumn {
        /// Exact catalogue column identity.
        id: PhysicalColumnId,
        /// Application output name, including explicit aliases.
        output_name: String,
    },
    /// A derived or metadata field with an explicit name and visibility.
    ResultField {
        /// Exact name sent to the host.
        name: String,
        /// Explicit application/provenance/internal role assigned by the producer.
        visibility: CurrentRowResultVisibility,
    },
    /// Construction-only source cell, resolved before publication.
    UnresolvedSourceCell {
        /// Source application name in the selected read schema.
        output_name: String,
    },
}

impl CurrentRowPublicationField {
    #[doc(hidden)]
    pub fn public_name(&self) -> Option<&str> {
        match self {
            Self::StoredColumn { output_name, .. } | Self::UnresolvedSourceCell { output_name } => {
                Some(output_name)
            }
            Self::ResultField {
                name,
                visibility:
                    CurrentRowResultVisibility::ApplicationCell
                    | CurrentRowResultVisibility::PublicProvenance,
            } => Some(name),
            Self::ResultField {
                visibility: CurrentRowResultVisibility::HiddenMetadata,
                ..
            } => None,
        }
    }

    #[doc(hidden)]
    pub fn application_name(&self) -> Option<&str> {
        match self {
            Self::StoredColumn { output_name, .. } | Self::UnresolvedSourceCell { output_name } => {
                Some(output_name)
            }
            Self::ResultField {
                name,
                visibility: CurrentRowResultVisibility::ApplicationCell,
            } => Some(name),
            Self::ResultField { .. } => None,
        }
    }

    #[doc(hidden)]
    pub fn role(&self) -> CurrentRowBindingRole {
        match self {
            Self::StoredColumn { .. } | Self::UnresolvedSourceCell { .. } => {
                CurrentRowBindingRole::PhysicalColumn
            }
            Self::ResultField { .. } => CurrentRowBindingRole::LogicalField,
        }
    }
}
