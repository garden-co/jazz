//! Public computed columns requested from a query source. This vocabulary is
//! internal to query preparation and is not a storage or wire encoding.

pub use crate::ids::AuthorField;
use crate::ids::AuthorSubject;
use jazz_types::records::ValueType;

/// The row event whose provenance a column exposes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProvenanceEvent {
    /// Original creation of the row.
    Creation,
    /// Authorship of the selected content version.
    Update,
    /// Current winning deletion of the logical row.
    Deletion,
}

/// A timestamp or author field from one row event.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProvenanceValue {
    /// Authored time, exposed as physical milliseconds.
    Timestamp,
    /// Author or author subfield.
    Author(AuthorField),
}

/// Public magic columns, independent of their physical source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MagicColumn {
    /// Creation, update, or deletion provenance.
    Provenance {
        /// Event supplying the value.
        event: ProvenanceEvent,
        /// Value within the event.
        value: ProvenanceValue,
    },
    /// Full acceptance HLC of a historical content version.
    AcceptedAt,
    /// Portable transaction identity of a historical content version.
    TransactionId,
}

impl MagicColumn {
    /// Resolve a public name, rejecting unknown fields and paths.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "$acceptedAt" => return Some(Self::AcceptedAt),
            "$transactionId" => return Some(Self::TransactionId),
            _ => {}
        }
        let (event, suffix) = if let Some(suffix) = name.strip_prefix("$created") {
            (ProvenanceEvent::Creation, suffix)
        } else if let Some(suffix) = name.strip_prefix("$updated") {
            (ProvenanceEvent::Update, suffix)
        } else {
            (ProvenanceEvent::Deletion, name.strip_prefix("$deleted")?)
        };
        let value = match suffix {
            "At" => ProvenanceValue::Timestamp,
            "By" => ProvenanceValue::Author(AuthorField::Whole),
            _ => ProvenanceValue::Author(AuthorField::parse_path(suffix.strip_prefix("By.")?)?),
        };
        Some(Self::Provenance { event, value })
    }

    /// Public column name.
    pub fn name(self) -> &'static str {
        use AuthorField::*;
        use ProvenanceEvent::*;
        use ProvenanceValue::*;
        match self {
            Self::AcceptedAt => "$acceptedAt",
            Self::TransactionId => "$transactionId",
            Self::Provenance {
                event: Creation,
                value: Timestamp,
            } => "$createdAt",
            Self::Provenance {
                event: Creation,
                value: Author(Whole),
            } => "$createdBy",
            Self::Provenance {
                event: Creation,
                value: Author(Account),
            } => "$createdBy.account",
            Self::Provenance {
                event: Creation,
                value: Author(Identity),
            } => "$createdBy.identity",
            Self::Provenance {
                event: Creation,
                value: Author(IdentityIssuer),
            } => "$createdBy.identity.issuer",
            Self::Provenance {
                event: Creation,
                value: Author(IdentitySubject),
            } => "$createdBy.identity.subject",
            Self::Provenance {
                event: Update,
                value: Timestamp,
            } => "$updatedAt",
            Self::Provenance {
                event: Update,
                value: Author(Whole),
            } => "$updatedBy",
            Self::Provenance {
                event: Update,
                value: Author(Account),
            } => "$updatedBy.account",
            Self::Provenance {
                event: Update,
                value: Author(Identity),
            } => "$updatedBy.identity",
            Self::Provenance {
                event: Update,
                value: Author(IdentityIssuer),
            } => "$updatedBy.identity.issuer",
            Self::Provenance {
                event: Update,
                value: Author(IdentitySubject),
            } => "$updatedBy.identity.subject",
            Self::Provenance {
                event: Deletion,
                value: Timestamp,
            } => "$deletedAt",
            Self::Provenance {
                event: Deletion,
                value: Author(Whole),
            } => "$deletedBy",
            Self::Provenance {
                event: Deletion,
                value: Author(Account),
            } => "$deletedBy.account",
            Self::Provenance {
                event: Deletion,
                value: Author(Identity),
            } => "$deletedBy.identity",
            Self::Provenance {
                event: Deletion,
                value: Author(IdentityIssuer),
            } => "$deletedBy.identity.issuer",
            Self::Provenance {
                event: Deletion,
                value: Author(IdentitySubject),
            } => "$deletedBy.identity.subject",
        }
    }

    /// Whether this column is row provenance rather than history-only metadata.
    pub fn is_provenance(self) -> bool {
        matches!(self, Self::Provenance { .. })
    }

    /// Whether this column describes the current deletion winner.
    pub fn is_deletion(self) -> bool {
        matches!(
            self,
            Self::Provenance {
                event: ProvenanceEvent::Deletion,
                ..
            }
        )
    }

    /// Whether this column needs content-version provenance.
    pub fn is_content_provenance(self) -> bool {
        matches!(
            self,
            Self::Provenance {
                event: ProvenanceEvent::Creation | ProvenanceEvent::Update,
                ..
            }
        )
    }

    /// Whole timestamp or author needed to supply this column.
    pub fn root(self) -> Self {
        match self {
            Self::Provenance {
                event,
                value: ProvenanceValue::Author(_),
            } => Self::Provenance {
                event,
                value: ProvenanceValue::Author(AuthorField::Whole),
            },
            _ => self,
        }
    }

    /// Nonempty author path, if this column selects a nested author field.
    pub fn author_path(self) -> Option<&'static [&'static str]> {
        match self {
            Self::Provenance {
                value: ProvenanceValue::Author(field),
                ..
            } if field != AuthorField::Whole => Some(field.path()),
            _ => None,
        }
    }

    /// Executable value type, including deletion nullability.
    pub fn value_type(self) -> &'static ValueType {
        match self {
            Self::Provenance {
                value: ProvenanceValue::Author(_),
                ..
            } => AuthorSubject::metadata_type(self.name()).expect("known author column"),
            Self::Provenance {
                event: ProvenanceEvent::Deletion,
                value: ProvenanceValue::Timestamp,
            } => {
                static TYPE: std::sync::LazyLock<ValueType> =
                    std::sync::LazyLock::new(|| ValueType::Nullable(Box::new(ValueType::U64)));
                &TYPE
            }
            Self::TransactionId => {
                static TYPE: std::sync::LazyLock<ValueType> =
                    std::sync::LazyLock::new(crate::tx::TxId::value_type);
                &TYPE
            }
            _ => &ValueType::U64,
        }
    }
}
