//! Node-local compact aliases for durable row authors.
//!
//! Physical row tables (history, global current, ahead current and its
//! shadow) store an [`AuthorAlias`] in their `created_by` / `updated_by`
//! columns instead of the full [`crate::ids::RowAuthor`] record. The durable
//! `jazz_authors` table maps each alias to the exact author record bytes.
//! This module owns the in-memory side of that table: the byte → alias index
//! used by writers and the shared append-only [`ValueDictionary`] that
//! physical-to-logical read projections expand aliases through, so a read
//! never pays a storage lookup per row.
//!
//! Aliases are 4-byte (`u32`) node-local numbers allocated densely from 1;
//! `0` is reserved. Allocation fails closed with
//! [`AuthorAliasSpaceExhausted`] once `u32::MAX` is taken: it never wraps and
//! never reuses an alias. The in-memory dictionary stays indexed by alias.
//!
//! Aliases are storage shorthand only. Logical rows, query graphs, policies
//! and wire records always carry the full author record.

use std::collections::BTreeSet;
use std::sync::Arc;

use groove::ivm::ValueDictionary;
use groove::records::RecordDescriptor;
use rustc_hash::FxHashMap;

use crate::ids::{AuthorAlias, RowAuthor};

/// Stable name of the author dictionary. Hashing covers only this name and
/// the value type, keeping graph node ids deterministic across runs.
const AUTHOR_DICTIONARY_NAME: &str = "jazz_authors";

/// In-memory `jazz_authors` catalogue.
///
/// An alias is *provisional* from allocation until a durable (resident)
/// `jazz_authors` row for it has been observed. Every batch that stores a
/// provisional alias also upserts its author row, so a batch that is dropped
/// after allocation can never leave a stored alias without its mapping.
#[derive(Clone, Debug)]
pub(crate) struct AuthorAliases {
    by_record: FxHashMap<Box<[u8]>, AuthorAlias>,
    dictionary: ValueDictionary,
    max_alias: u32,
    provisional: BTreeSet<AuthorAlias>,
    expanded_descriptors: FxHashMap<RecordDescriptor, RecordDescriptor>,
}

impl Default for AuthorAliases {
    fn default() -> Self {
        Self {
            by_record: FxHashMap::default(),
            dictionary: ValueDictionary::new(AUTHOR_DICTIONARY_NAME, RowAuthor::value_type()),
            max_alias: 0,
            provisional: BTreeSet::new(),
            expanded_descriptors: FxHashMap::default(),
        }
    }
}

/// A durable mapping disagrees with the resident catalogue.
#[derive(Debug)]
pub(crate) struct AuthorAliasConflict;

/// Every `u32` alias is allocated; a new author cannot be aliased.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct AuthorAliasSpaceExhausted;

impl AuthorAliases {
    /// The shared dictionary registered read projections expand through.
    pub(crate) fn dictionary(&self) -> &ValueDictionary {
        &self.dictionary
    }

    /// Alias of one exact encoded author record, if resident.
    pub(crate) fn alias_for(&self, author_record: &[u8]) -> Option<AuthorAlias> {
        self.by_record.get(author_record).copied()
    }

    /// Exact encoded author record bound to `alias`, if resident.
    pub(crate) fn author_record(&self, alias: AuthorAlias) -> Option<Arc<[u8]>> {
        self.dictionary.get(u64::from(alias.0))
    }

    /// Install one durable `jazz_authors` row. Rejects a row that maps an
    /// alias or record already bound to something else.
    pub(crate) fn install_durable(
        &mut self,
        alias: AuthorAlias,
        author_record: &[u8],
    ) -> Result<(), AuthorAliasConflict> {
        if alias.0 == 0 {
            return Err(AuthorAliasConflict);
        }
        if let Some(existing) = self.by_record.get(author_record)
            && *existing != alias
        {
            return Err(AuthorAliasConflict);
        }
        self.dictionary
            .install(u64::from(alias.0), author_record)
            .map_err(|_| AuthorAliasConflict)?;
        self.by_record.insert(author_record.into(), alias);
        self.max_alias = self.max_alias.max(alias.0);
        self.provisional.remove(&alias);
        Ok(())
    }

    /// Alias for storing `author_record`, allocating a provisional one when
    /// the author is new to this node. Returns whether the caller's batch
    /// must also carry the author row (true while the alias is provisional).
    ///
    /// A new author after `u32::MAX` has been allocated fails closed with
    /// [`AuthorAliasSpaceExhausted`]; known authors keep resolving.
    pub(crate) fn stage(
        &mut self,
        author_record: &[u8],
    ) -> Result<(AuthorAlias, bool), AuthorAliasSpaceExhausted> {
        if let Some(alias) = self.by_record.get(author_record).copied() {
            return Ok((alias, self.provisional.contains(&alias)));
        }
        let alias = AuthorAlias(
            self.max_alias
                .checked_add(1)
                .ok_or(AuthorAliasSpaceExhausted)?,
        );
        // Every alias above `max_alias` is unbound, so this cannot conflict.
        self.dictionary
            .install(u64::from(alias.0), author_record)
            .expect("fresh author alias is unbound");
        self.by_record.insert(author_record.into(), alias);
        self.max_alias = alias.0;
        self.provisional.insert(alias);
        Ok((alias, true))
    }

    /// Aliases whose author row has not yet been observed in storage.
    pub(crate) fn provisional(&self) -> impl Iterator<Item = AuthorAlias> + '_ {
        self.provisional.iter().copied()
    }

    pub(crate) fn has_provisional(&self) -> bool {
        !self.provisional.is_empty()
    }

    /// Mark `alias` durable after its row was observed in resident storage.
    pub(crate) fn confirm(&mut self, alias: AuthorAlias) {
        self.provisional.remove(&alias);
    }

    /// Descriptor of `physical` with its alias author fields widened back to
    /// the logical author record type. Cached per physical descriptor.
    pub(crate) fn expanded_descriptor(
        &mut self,
        physical: RecordDescriptor,
        author_fields: &[usize],
    ) -> RecordDescriptor {
        *self
            .expanded_descriptors
            .entry(physical)
            .or_insert_with(|| {
                let author = RowAuthor::value_type();
                RecordDescriptor::new_with_fields(
                    physical
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(index, field)| {
                            let mut field = field.clone();
                            if author_fields.contains(&index) {
                                field.value_type = author.clone();
                            }
                            field
                        })
                        .collect::<Vec<_>>(),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Internal: allocation, provisional state and the dictionary are private
    // storage bookkeeping with no public-API observable besides unchanged
    // provenance, which the black-box provenance tests already cover.
    #[test]
    fn staging_allocates_once_and_durable_rows_confirm() {
        let mut aliases = AuthorAliases::default();
        let (first, needs_row) = aliases.stage(b"author-a").unwrap();
        assert_eq!(first, AuthorAlias(1));
        assert!(needs_row);
        assert_eq!(aliases.stage(b"author-a").unwrap(), (first, true));
        let (second, _) = aliases.stage(b"author-b").unwrap();
        assert_eq!(second, AuthorAlias(2));
        assert_eq!(
            aliases.author_record(first).as_deref(),
            Some(&b"author-a"[..])
        );

        aliases.confirm(first);
        assert_eq!(aliases.stage(b"author-a").unwrap(), (first, false));
        assert!(aliases.install_durable(second, b"author-b").is_ok());
        assert!(!aliases.has_provisional());

        // A durable row may not rebind an alias or a record.
        assert!(aliases.install_durable(second, b"author-c").is_err());
        assert!(
            aliases
                .install_durable(AuthorAlias(3), b"author-a")
                .is_err()
        );
    }

    // Internal: exhausting four billion aliases is not reachable through the
    // public API in a test (nor affordable through the dense dictionary), so
    // the allocator boundary is pinned directly by raising the high-water mark.
    #[test]
    fn staging_fails_closed_when_u32_aliases_are_exhausted() {
        let mut aliases = AuthorAliases::default();
        let (known, _) = aliases.stage(b"author-a").unwrap();
        aliases.confirm(known);
        aliases.max_alias = u32::MAX;

        // No wrap to 0 or 1, no reuse: a new author is refused.
        assert_eq!(aliases.stage(b"author-b"), Err(AuthorAliasSpaceExhausted));
        assert_eq!(aliases.alias_for(b"author-b"), None);
        assert!(!aliases.has_provisional());
        // Already-aliased authors keep resolving after exhaustion.
        assert_eq!(aliases.stage(b"author-a").unwrap(), (known, false));
    }
}
