//! Trusted-host catalogue captures, independent of account rows and read coverage.
//! Uses the current authority namespace, structural versions and stored-name
//! digests. Neither a capture nor its encoding authenticates arbitrary input.
use super::*;
use serde::{Deserialize, Serialize};

const HEADER: &[u8] = b"JAZZ-SQLITE-CATALOGUE-CAPTURE\0\x02";
pub(super) const MODE_KEY: &str = "trusted-catalogue-cache-v2";

#[derive(Clone, Serialize, Deserialize)]
struct Capture {
    catalogue: sync::CatalogueUpdate,
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::Protocol, message)
}

impl Capture {
    fn validate(&self) -> Result<(), Error> {
        let c = &self.catalogue;
        if c.namespace.is_none_or(|n| n == [0; 16]) || c.versions.is_empty() {
            return Err(invalid(
                "catalogue capture has no authenticated namespace or versions",
            ));
        }
        if c.versions.len() != c.confirmed.len() {
            return Err(invalid(
                "catalogue capture lacks complete confirmed descriptors",
            ));
        }
        let mut seen = BTreeSet::new();
        for (v, (id, digest)) in c.versions.iter().zip(&c.confirmed) {
            let version = *v.schema.id.as_bytes();
            if !seen.insert(version)
                || version != *id
                || v.schema.schema.version_id() != v.schema.id
                || v.schema.schema.without_permissions() != v.schema.schema
            {
                return Err(invalid(
                    "catalogue capture versions must be unique, structural and confirmed",
                ));
            }
            let names = crate::values::StoredNames {
                tables: v
                    .names
                    .iter()
                    .map(|(table, stored, columns)| {
                        (
                            table.clone(),
                            (stored.clone(), columns.iter().cloned().collect()),
                        )
                    })
                    .collect(),
            };
            let shape = v.schema.schema.tables();
            if names.tables.len() != shape.len()
                || shape.iter().any(|shape| {
                    names
                        .tables
                        .get(&shape.name)
                        .is_none_or(|(stored, columns)| {
                            stored.is_empty()
                                || columns.len() != shape.columns.len()
                                || shape
                                    .columns
                                    .iter()
                                    .any(|c| columns.get(&c.name).is_none_or(String::is_empty))
                        })
                })
            {
                return Err(invalid(
                    "catalogue capture stored-name shape does not match its schema",
                ));
            }
            if names.tables.len() != v.names.len()
                || v.names.iter().any(|(_, _, columns)| {
                    columns
                        .iter()
                        .map(|(name, _)| name)
                        .collect::<BTreeSet<_>>()
                        .len()
                        != columns.len()
                })
                || crate::names::names_digest(&names) != *digest
            {
                return Err(invalid(
                    "catalogue capture has duplicate or mismatched stored names",
                ));
            }
        }
        let mut cells = BTreeSet::new();
        for cell in &c.cells {
            if !c.confirmed.iter().any(|(id, _)| *id == cell.since)
                || !cells.insert((&cell.table, &cell.column, cell.since))
            {
                return Err(invalid(
                    "catalogue capture has an unconfirmed or duplicate lens cell",
                ));
            }
        }
        Ok(())
    }

    fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = HEADER.to_vec();
        bytes.extend(postcard::to_allocvec(self).map_err(|e| invalid(e.to_string()))?);
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let bytes_body = bytes
            .strip_prefix(HEADER)
            .ok_or_else(|| invalid("unsupported trusted catalogue capture encoding"))?;
        let (capture, trailing): (Self, _) = postcard::take_from_bytes(bytes_body)
            .map_err(|e| invalid(format!("invalid catalogue capture: {e}")))?;
        if !trailing.is_empty() {
            return Err(invalid("trailing catalogue capture bytes"));
        }
        capture.validate()?;
        if capture.encode()? != bytes {
            return Err(invalid("noncanonical catalogue capture bytes"));
        }
        Ok(capture)
    }
}

/// Validate an atomic host-cache replacement in one exact registry/app/environment.
/// The host owns that scope. This checks immutable namespace, schema and names;
/// it does not assert a current permissions revision or row/authority cut.
pub fn validate_catalogue_capture_replacement(previous: &[u8], next: &[u8]) -> Result<(), Error> {
    let old = Capture::decode(previous)?;
    let new = Capture::decode(next)?;
    if old.catalogue.namespace != new.catalogue.namespace {
        return Err(invalid("catalogue cache authority namespace changed"));
    }
    for version in &old.catalogue.versions {
        if !new.catalogue.versions.contains(version) {
            return Err(invalid(
                "catalogue cache replacement omits or changes an accepted descriptor",
            ));
        }
    }
    for cell in &old.catalogue.cells {
        if !new.catalogue.cells.contains(cell) {
            return Err(invalid(
                "catalogue cache replacement omits or changes a lens cell",
            ));
        }
    }
    Ok(())
}

/// One owner observation. Readiness qualifies this handle's requested schema;
/// a host must durably publish moved bytes before advertising cached readiness.
pub struct AuthenticatedCatalogueState {
    pub capture: Option<Vec<u8>>,
    pub ready: bool,
}

impl Inner {
    pub(super) fn cached_catalogue_ready(&self, schema: &Schema) -> Result<bool, Error> {
        Ok(self.namespace()?.is_some() && self.names_confirmed(&schema.jazz.version_id()))
    }

    pub(super) fn require_cached_catalogue_schema(&self, schema: &Schema) -> Result<(), Error> {
        if self.catalogue_cache_required && !self.cached_catalogue_ready(schema)? {
            return Err(Error::new(
                ErrorCode::Schema,
                "requested schema is not admitted by the authenticated catalogue",
            ));
        }
        Ok(())
    }

    fn capture_catalogue(&self) -> Result<Option<Capture>, Error> {
        let Some(namespace) = self.namespace()? else {
            return Ok(None);
        };
        let confirmed = if self.publishes_catalogue() {
            self.names
                .in_order()
                .into_iter()
                .map(|(version, names)| (*version.as_bytes(), crate::names::names_digest(names)))
                .collect::<Vec<_>>()
        } else {
            self.names.confirmations()
        };
        // Keep actual admission order: lens dependencies can precede their targets.
        if confirmed.is_empty() {
            return Ok(None);
        }
        let mut versions = Vec::with_capacity(confirmed.len());
        for (id, _) in &confirmed {
            let version = crate::ids::SchemaVersionId::from_bytes(*id);
            let schema = if version == self.owner_version {
                &self.owner.jazz
            } else {
                &self
                    .admitted
                    .iter()
                    .find(|(known, _)| *known == version)
                    .ok_or_else(|| {
                        invalid("confirmed catalogue descriptor is unavailable in this owner")
                    })?
                    .1
            };
            let names = self
                .names
                .get(&version)
                .ok_or_else(|| invalid("confirmed catalogue names are unavailable"))?;
            versions.push(sync::CatalogueVersion {
                schema: crate::protocol::SchemaVersion {
                    id: version,
                    schema: schema.without_permissions(),
                },
                names: names
                    .tables
                    .iter()
                    .map(|(table, (stored, columns))| {
                        (
                            table.clone(),
                            stored.clone(),
                            columns
                                .iter()
                                .map(|(a, b)| (a.clone(), b.clone()))
                                .collect(),
                        )
                    })
                    .collect(),
            });
        }
        let mut cells = self
            .names
            .cells()
            .iter()
            .filter(|cell| {
                confirmed
                    .iter()
                    .any(|(id, _)| *id == *cell.since.as_bytes())
            })
            .map(|cell| {
                let (source, default) = match &cell.rule {
                    crate::names::LensCellRule::Copy(source) => {
                        (Some(source.clone()), sync::Cell::Null)
                    }
                    crate::names::LensCellRule::Dropped(default) => {
                        (None, sync::Cell::from(default.clone()))
                    }
                };
                sync::CatalogueCell {
                    table: cell.table.clone(),
                    column: cell.column.clone(),
                    since: *cell.since.as_bytes(),
                    source,
                    default,
                }
            })
            .collect::<Vec<_>>();
        cells.sort_by(|a, b| (&a.table, &a.column, a.since).cmp(&(&b.table, &b.column, b.since)));
        let capture = Capture {
            catalogue: sync::CatalogueUpdate {
                namespace: Some(namespace),
                versions,
                cells,
                confirmed,
            },
        };
        capture.validate()?;
        Ok(Some(capture))
    }
}

impl Db {
    pub fn has_authenticated_catalogue(&self) -> Result<bool, Error> {
        self.ensure_storage_owner_active()?;
        let inner = self.inner();
        inner.cached_catalogue_ready(&inner.schema)
    }

    pub fn take_authenticated_catalogue_state(&self) -> Result<AuthenticatedCatalogueState, Error> {
        self.ensure_storage_owner_active()?;
        let inner = self.inner();
        let ready = inner.cached_catalogue_ready(&inner.schema)?;
        let capture = if inner.catalogue_capture_pending.get() {
            inner.capture_catalogue()?.map(|c| c.encode()).transpose()?
        } else {
            None
        };
        inner.catalogue_capture_pending.set(false);
        Ok(AuthenticatedCatalogueState { capture, ready })
    }

    /// Restore prior authenticated metadata during owner open, before sharing it.
    /// No fresh coverage, row cut, permissions or authority role is installed.
    ///
    /// # Safety
    /// The host must authenticate provenance in the exact registry/app/environment
    /// and supply only bytes from that scope's prior capture. This is not a
    /// signature on arbitrary caller input. Call before admitting other handles.
    pub unsafe fn install_cached_catalogue(&self, bytes: &[u8]) -> Result<(), Error> {
        self.ensure_storage_owner_active()?;
        let capture = Capture::decode(bytes)?;
        if self.view.is_some() {
            return Err(invalid("catalogue cache requires its physical owner"));
        }
        let mut inner = self.inner();
        if inner.mutation_admission_closed {
            return Err(Error::new(
                ErrorCode::Storage,
                "database is closing or closed",
            ));
        }
        if inner.authority
            || inner.client.is_some()
            || !inner.batches.is_empty()
            || inner.tx.is_some()
            || !inner.sessions.is_empty()
        {
            return Err(invalid(
                "catalogue cache must be installed before owner publication",
            ));
        }
        if inner
            .namespace()?
            .is_some_and(|n| Some(n) != capture.catalogue.namespace)
        {
            return Err(invalid(
                "catalogue cache belongs to another authority namespace",
            ));
        }
        // Current durable versions win over a stale host cache; matching versions
        // still pass the ordinary immutable-name/type checks, never replacement.
        let had_catalogue = inner.namespace()?.is_some();
        let saved = (
            inner.physical.clone(),
            inner.names.clone(),
            inner.admitted.clone(),
            inner.owner.clone(),
            inner.schema.clone(),
        );
        let tags = inner.names.tags().snapshot();
        begin_savepoint(&inner.conn, "jazz_cached_catalogue")?;
        let c = capture.catalogue;
        let result = (|| {
            inner.mirror_catalogue(c.namespace, c.versions, c.cells, &c.confirmed)?;
            for (id, _) in &c.confirmed {
                if !inner
                    .names
                    .confirmed(&crate::ids::SchemaVersionId::from_bytes(*id))
                {
                    return Err(invalid(
                        "catalogue cache names were not confirmed after installation",
                    ));
                }
            }
            inner
                .conn
                .execute(
                    "INSERT OR REPLACE INTO _jazz_meta(k,v) VALUES(?1,?2)",
                    rusqlite::params![MODE_KEY, vec![1u8]],
                )
                .map_err(storage)?;
            Ok(())
        })();
        let ended = end_savepoint(&inner.conn, "jazz_cached_catalogue", result.is_ok());
        if let Err(error) = result.and(ended) {
            (
                inner.physical,
                inner.names,
                inner.admitted,
                inner.owner,
                inner.schema,
            ) = saved;
            inner.names.tags().restore(tags);
            *inner.stored_view.borrow_mut() = None;
            inner.reset_everything();
            return Err(error);
        }
        inner.catalogue_cache_required = true;
        inner.catalogue_capture_pending.set(had_catalogue);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A private codec unit is necessary to pin the complete durable envelope,
    /// independently of randomized authority namespaces in public runtime tests.
    /// The fixture was assembled from explicit fields with independent UUID/SHA
    /// calculations, not captured from this encoder or a synthetic sync link.
    #[test]
    fn catalogue_capture_v2_independent_empty_fixture() {
        let bytes = include_bytes!("fixtures/catalogue_capture_v2_empty.bin");
        let actual = Capture::decode(bytes).unwrap();
        assert_eq!(actual.catalogue.namespace, Some([17; 16]));
        assert_eq!(actual.catalogue.versions.len(), 1);
        let schema = JazzSchema::empty();
        assert_eq!(actual.catalogue.versions[0].schema.id, schema.version_id());
        assert_eq!(actual.catalogue.versions[0].schema.schema, schema);
        assert!(actual.catalogue.versions[0].names.is_empty());
        assert!(actual.catalogue.cells.is_empty());
        assert_eq!(actual.catalogue.confirmed.len(), 1);
        assert_eq!(actual.encode().unwrap().as_slice(), bytes);
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(Capture::decode(&trailing).is_err());
        let mut legacy = bytes.to_vec();
        legacy[HEADER.len() - 1] = 1;
        assert!(Capture::decode(&legacy).is_err());
    }
}
