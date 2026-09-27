//! Decoding contract for structured terminal roots: descriptor slots,
//! publication fields and root output occurrence identity. Re-exported from
//! `crate::db`.

use groove::records::RecordDescriptor;

use super::api_error::{Error, ErrorCode};
#[cfg(any(test, feature = "testing"))]
use super::query_engine::CurrentRowBindingRole;
use crate::object::{ObjectId, OutputOccurrenceId};

/// Immutable producer-owned decoding contract for a structured terminal root.
///
/// The maintained query compiler creates this alongside its app-row terminal.
/// Consumers install it before applying operations which name `id`; the
/// descriptor remains the source of truth for encoded types while these slots
/// map public fields to their physical record positions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalRootLayout {
    /// Stable hash of the descriptor, slots, identities and carrier.
    pub id: String,
    /// Exact physical root descriptor used to decode packed bytes.
    pub root_descriptor: RecordDescriptor,
    /// Descriptor slot containing the stable root UUID.
    pub root_key_slot: usize,
    /// Exact descriptor identity of the root UUID slot.
    pub root_key_field_name: String,
    /// Public field-to-descriptor slot mappings, in public output order.
    pub public_fields: Vec<TerminalRootPublicField>,
    /// Physical representation used for public cells.
    pub carrier: TerminalRootCarrier,
    /// The collector key contains an arm discriminator immediately after its
    /// physical root UUID, which denotes source position zero.
    pub root_union_arm: bool,
}

/// One public root field's immutable physical slot identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalRootPublicField {
    /// Authoritative publication binding supplied by the compiler.
    pub publication: crate::node::CurrentRowPublicationField,
    /// Public column name.
    pub name: String,
    /// Physical descriptor field name at `slot`.
    pub descriptor_field_name: String,
    /// Physical descriptor slot.
    pub slot: usize,
    /// Encoded representation of this individual slot.
    pub carrier: TerminalRootCarrier,
}

/// The producer representation applied around declared public column types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalRootCarrier {
    /// A physical `CurrentRow`: each application cell has one extra nullable
    /// carrier around its declared storage type.
    CurrentRow,
    /// A logical collector/projection record with declared storage types.
    Logical,
}

/// Derive the explicit producer provenance for every terminal descriptor slot.
///
/// Both terminal-delta decoding and local maintained-view reset snapshots use
/// this exact mapping; treating a hybrid collector record as wholly logical
/// loses the distinction between a physical `user_{column}` and a logical
/// field with that same name.
pub fn terminal_root_publication_fields(
    layout: &TerminalRootLayout,
) -> Vec<crate::node::CurrentRowPublicationField> {
    use crate::node::CurrentRowPublicationField;
    let mut fields = layout
        .root_descriptor
        .fields()
        .iter()
        .map(|field| CurrentRowPublicationField::ResultField {
            name: field.name.clone().expect("terminal fields are named"),
            visibility: crate::node::CurrentRowResultVisibility::HiddenMetadata,
        })
        .collect::<Vec<_>>();
    for field in &layout.public_fields {
        assert_eq!(
            field.publication.public_name(),
            Some(field.name.as_str()),
            "terminal publication name must match its public slot mapping"
        );
        fields[field.slot] = field.publication.clone();
    }
    fields
}

#[cfg(any(test, feature = "testing"))]
#[doc(hidden)]
pub fn terminal_root_binding_fields(layout: &TerminalRootLayout) -> Vec<CurrentRowBindingRole> {
    let binding_for_carrier = |carrier| match carrier {
        TerminalRootCarrier::CurrentRow => CurrentRowBindingRole::PhysicalColumn,
        TerminalRootCarrier::Logical => CurrentRowBindingRole::LogicalField,
    };
    let mut fields =
        vec![binding_for_carrier(layout.carrier); layout.root_descriptor.fields().len()];
    for field in &layout.public_fields {
        fields[field.slot] = binding_for_carrier(field.carrier);
    }
    fields
}

/// Public logical names for the same terminal descriptor slots.  A terminal
/// projection can retain its source's `user_{column}` carrier name while its
/// public output is simply `{column}`.  Native hosts must receive the latter
/// without guessing from a prefix, while truly logical `user_*` fields remain
/// untouched.
#[cfg(test)]
pub fn terminal_root_binding_field_names(layout: &TerminalRootLayout) -> Vec<Option<String>> {
    let mut names = vec![None; layout.root_descriptor.fields().len()];
    for field in &layout.public_fields {
        names[field.slot] = Some(field.name.clone());
    }
    names
}

/// Decode the Groove ordered key used to address one root output occurrence.
/// Plain joins are UUID sequences; joined-source discriminators precede their
/// UUIDs at source positions one and above. Root-union collector keys retain
/// their physical UUID first and need the prepared layout to identify the
/// following `(label, actual-root-row)` pair as source position zero.
pub fn terminal_root_occurrence_id_with_root_union(
    encoded: &[u8],
    root_union_arm: bool,
) -> Result<OutputOccurrenceId, Error> {
    fn uuid_at(encoded: &[u8], cursor: &mut usize) -> Option<ObjectId> {
        if encoded.get(*cursor).copied() != Some(10) {
            return None;
        }
        let start = *cursor + 1;
        let end = start + 16;
        let uuid = uuid::Uuid::from_slice(encoded.get(start..end)?).ok()?;
        *cursor = end;
        Some(ObjectId::from_uuid(uuid))
    }

    fn ordered_string_at(encoded: &[u8], cursor: &mut usize) -> Option<String> {
        if encoded.get(*cursor).copied() != Some(6) {
            return None;
        }
        *cursor += 1;
        let mut decoded = Vec::new();
        loop {
            let byte = *encoded.get(*cursor)?;
            *cursor += 1;
            if byte != 0 {
                decoded.push(byte);
                continue;
            }
            match encoded.get(*cursor).copied()? {
                0 => {
                    *cursor += 1;
                    break;
                }
                0xff => {
                    *cursor += 1;
                    decoded.push(0);
                }
                _ => return None,
            }
        }
        String::from_utf8(decoded).ok()
    }

    let mut cursor = 0;
    let root = uuid_at(encoded, &mut cursor).ok_or_else(|| {
        Error::new(
            ErrorCode::Protocol,
            "terminal root key must begin with a UUID",
        )
    })?;
    let mut joined = Vec::new();
    let mut union_arms = Vec::new();
    if root_union_arm {
        let label = ordered_string_at(encoded, &mut cursor).ok_or_else(|| {
            Error::new(
                ErrorCode::Protocol,
                "terminal root key contains an invalid root union discriminator",
            )
        })?;
        let actual_root = uuid_at(encoded, &mut cursor).ok_or_else(|| {
            Error::new(
                ErrorCode::Protocol,
                "terminal root key contains no root union contributor",
            )
        })?;
        if actual_root != root {
            return Err(Error::new(
                ErrorCode::Protocol,
                "terminal root key root union contributor disagrees with root UUID",
            ));
        }
        union_arms.push((0, label));
    }
    while cursor < encoded.len() {
        let discriminator = if encoded[cursor] == 6 {
            Some(ordered_string_at(encoded, &mut cursor).ok_or_else(|| {
                Error::new(
                    ErrorCode::Protocol,
                    "terminal root key contains an invalid union discriminator",
                )
            })?)
        } else {
            None
        };
        let joined_id = uuid_at(encoded, &mut cursor).ok_or_else(|| {
            Error::new(
                ErrorCode::Protocol,
                "terminal root key contains an unsupported component",
            )
        })?;
        if let Some(discriminator) = discriminator {
            union_arms.push((joined.len() + 1, discriminator));
        }
        joined.push(joined_id);
    }

    if union_arms.is_empty() {
        Ok(OutputOccurrenceId::new(root, joined))
    } else {
        OutputOccurrenceId::with_union_arms(root, joined, union_arms).ok_or_else(|| {
            Error::new(
                ErrorCode::Protocol,
                "terminal root key contains invalid union discriminators",
            )
        })
    }
}
