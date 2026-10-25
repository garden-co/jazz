//! Authority-issued catalogue identities. Logical and stored names locate a
//! lineage; only the authority's persisted UUID manifest identifies it.
use super::*;
use crate::ids::{GlobalPhysicalColumnId, GlobalPhysicalTableId, SchemaVersionId};
use crate::protocol::{CatalogueSnapshot, PhysicalIdentityManifest, PhysicalTableIdentity};
use serde::{Deserialize, Serialize};

/// `jazz.sqlite.catalogue-head.v1`: one catalogue namespace and its monotone
/// publication and active-schema positions. Neither position is a row sequence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogueHeadV1 {
    pub root: [u8; 32],
    pub through: u64,
    pub revision: u64,
    pub current: SchemaVersionId,
}

/// `jazz.sqlite.catalogue-schema.v1`: a published descriptor and its exact
/// authority-issued manifest. Receivers never allocate or derive these UUIDs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CatalogueSchemaV1 {
    pub schema: SchemaVersion,
    pub identities: PhysicalIdentityManifest,
}
impl CatalogueSchemaV1 {
    pub(super) fn validate(&self) -> Result<(), Error> {
        if self.schema.id != self.schema.schema.version_id() {
            return Err(invalid("catalogue schema identity mismatch"));
        }
        self.identities
            .validate_for_schema(&self.schema.schema)
            .map_err(invalid)
    }
}

pub(super) const DDL: &str = "\
 CREATE TABLE IF NOT EXISTS _jazz_catalogue_head_v1 (slot INTEGER PRIMARY KEY CHECK(slot=1), value BLOB NOT NULL);\
 CREATE TABLE IF NOT EXISTS _jazz_catalogue_schema_v1 (version BLOB PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID;\
 CREATE TABLE IF NOT EXISTS _jazz_catalogue_table_v1 (version BLOB NOT NULL, name TEXT NOT NULL, value BLOB NOT NULL, PRIMARY KEY(version,name)) WITHOUT ROWID;";
fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::Protocol, message)
}
fn absent(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::NotObserved, message)
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
    postcard::to_allocvec(value).map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    let (value, rest) = postcard::take_from_bytes(bytes)
        .map_err(|e| Error::new(ErrorCode::Storage, format!("invalid catalogue-v1: {e}")))?;
    if !rest.is_empty() {
        return Err(Error::new(
            ErrorCode::Storage,
            "trailing catalogue-v1 bytes",
        ));
    }
    Ok(value)
}

impl Inner {
    pub(super) fn catalogue_head(&self) -> Result<Option<CatalogueHeadV1>, Error> {
        let bytes: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT value FROM _jazz_catalogue_head_v1 WHERE slot=1",
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)?;
        let head: Option<CatalogueHeadV1> = bytes.as_deref().map(decode).transpose()?;
        if head
            .as_ref()
            .is_some_and(|h| h.root == [0; 32] || h.current.0.is_nil())
        {
            return Err(Error::new(ErrorCode::Storage, "invalid catalogue-v1 head"));
        }
        Ok(head)
    }
    pub(super) fn catalogue_schema(
        &self,
        version: SchemaVersionId,
    ) -> Result<Option<CatalogueSchemaV1>, Error> {
        let bytes: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT value FROM _jazz_catalogue_schema_v1 WHERE version=?1",
                [version.as_bytes().as_slice()],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)?;
        let record: Option<CatalogueSchemaV1> = bytes.as_deref().map(decode).transpose()?;
        if let Some(record) = &record {
            record
                .validate()
                .map_err(|e| Error::new(ErrorCode::Storage, e.message))?;
            if record.schema.id != version {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "catalogue-v1 schema key mismatch",
                ));
            }
        }
        Ok(record)
    }
    fn catalogue_table(
        &self,
        version: SchemaVersionId,
        name: &str,
    ) -> Result<Option<PhysicalTableIdentity>, Error> {
        let bytes: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT value FROM _jazz_catalogue_table_v1 WHERE version=?1 AND name=?2",
                rusqlite::params![version.as_bytes().as_slice(), name],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)?;
        let table: Option<PhysicalTableIdentity> = bytes.as_deref().map(decode).transpose()?;
        if let Some(t) = &table {
            let mut seen = BTreeSet::new();
            if t.id.0.is_nil()
                || !seen.insert(t.id.0)
                || t.columns
                    .values()
                    .any(|c| c.id.0.is_nil() || !seen.insert(c.id.0))
            {
                return Err(Error::new(
                    ErrorCode::Storage,
                    "invalid catalogue-v1 table identities",
                ));
            }
        }
        if table.is_none()
            && self
                .catalogue_schema(version)?
                .is_some_and(|record| record.identities.tables.contains_key(name))
        {
            return Err(Error::new(
                ErrorCode::Storage,
                "catalogue-v1 table record missing",
            ));
        }
        Ok(table)
    }

    /// Called inside the caller's savepoint. Metadata and the selected head
    /// become visible atomically; an immutable descriptor cannot be rebound.
    pub(super) fn record_catalogue(
        &self,
        head: &CatalogueHeadV1,
        records: &[CatalogueSchemaV1],
    ) -> Result<(), Error> {
        if head.root == [0; 32] || head.current.0.is_nil() {
            return Err(invalid("invalid catalogue head"));
        }
        if let Some(previous) = self.catalogue_head()? {
            if previous.root != head.root {
                return Err(invalid("catalogue authority namespace changed"));
            }
            if head.revision < previous.revision || head.through < previous.through {
                return Err(absent("catalogue response is older than the accepted head"));
            }
            if head.revision == previous.revision && head.current != previous.current {
                return Err(invalid("conflicting catalogue revision"));
            }
        }
        let mut seen = BTreeSet::new();
        for record in records {
            record.validate()?;
            if !seen.insert(record.schema.id) {
                return Err(invalid("duplicate catalogue schema"));
            }
            if let Some(previous) = self.catalogue_schema(record.schema.id)? {
                if previous.identities != record.identities
                    || !previous
                        .schema
                        .schema
                        .has_same_lineage_structure(&record.schema.schema)
                {
                    return Err(invalid("published catalogue identities are immutable"));
                }
                continue;
            }
            let version = record.schema.id.as_bytes().as_slice();
            self.conn
                .execute(
                    "INSERT INTO _jazz_catalogue_schema_v1(version,value) VALUES(?1,?2)",
                    rusqlite::params![version, encode(record)?],
                )
                .map_err(storage)?;
            for (name, table) in &record.identities.tables {
                self.conn
                    .execute(
                        "INSERT INTO _jazz_catalogue_table_v1(version,name,value) VALUES(?1,?2,?3)",
                        rusqlite::params![version, name, encode(table)?],
                    )
                    .map_err(storage)?;
            }
        }
        if self.catalogue_schema(head.current)?.is_none() {
            return Err(invalid("catalogue current schema has no accepted manifest"));
        }
        self.conn.execute("INSERT INTO _jazz_catalogue_head_v1(slot,value) VALUES(1,?1) ON CONFLICT(slot) DO UPDATE SET value=excluded.value",[encode(head)?]).map_err(storage)?;
        self.catalogue_capture_pending.set(true);
        Ok(())
    }
    /// At most the current descriptor and the requested fixed descriptor;
    /// no history or unrelated catalogue versions are scanned per read.
    pub(super) fn catalogue_records(
        &self,
        opened: SchemaVersionId,
    ) -> Result<(CatalogueHeadV1, Vec<CatalogueSchemaV1>), Error> {
        let head = self
            .catalogue_head()?
            .ok_or_else(|| absent("accepted catalogue is unavailable"))?;
        let mut records = Vec::new();
        for version in BTreeSet::from([head.current, opened]) {
            if let Some(record) = self.catalogue_schema(version)? {
                records.push(record);
            }
        }
        Ok((head, records))
    }
}

impl Db {
    /// Initialise a fresh standalone authority's genesis catalogue exactly
    /// once. Server deployments install their existing authored snapshot instead.
    pub fn initialize_authority_catalogue(&self) -> Result<(), Error> {
        let inner = self.inner();
        if !inner.authority || inner.client.is_some() {
            return Err(absent("catalogue publication requires the authority"));
        }
        writer_order::require_ready(&inner.conn)?;
        if inner.catalogue_head()?.is_some() {
            return Ok(());
        }
        let schema = inner.owner.jazz.clone();
        drop(inner);
        self.install_authority_catalogue(&CatalogueSnapshot {
            genesis_physical_identities: PhysicalIdentityManifest::allocate(&schema),
            schemas: vec![SchemaVersion::new(schema.clone())],
            lineages: vec![],
            current_write_schema: CurrentWriteSchema {
                revision: 0,
                schema: schema.version_id(),
            },
        })
    }

    /// Install the authority's already-authored catalogue. UUID allocation and
    /// evolution belong to its publication path; schema admission alone does
    /// not grant accepted identities to a local proposal or typed view.
    pub fn validate_catalogue_snapshot(snapshot: &CatalogueSnapshot) -> Result<(), Error> {
        validate_snapshot(snapshot).map(drop)
    }

    /// Admit a validated authority snapshot into this store.
    pub fn install_authority_catalogue(&self, snapshot: &CatalogueSnapshot) -> Result<(), Error> {
        let (head, records) = validate_snapshot(snapshot)?;
        let inner = self.inner();
        if !inner.authority || inner.client.is_some() {
            return Err(absent("catalogue publication requires the authority"));
        }
        // A complete authority publication cannot forget retired descriptors.
        let mut statement = inner
            .conn
            .prepare("SELECT version FROM _jazz_catalogue_schema_v1")
            .map_err(storage)?;
        let versions = statement
            .query_map([], |r| r.get::<_, Vec<u8>>(0))
            .map_err(storage)?;
        for version in versions {
            let version = version.map_err(storage)?;
            if !records
                .iter()
                .any(|r| r.schema.id.as_bytes().as_slice() == version)
            {
                return Err(invalid("catalogue snapshot omits accepted history"));
            }
        }
        drop(statement);
        inner
            .conn
            .execute_batch("SAVEPOINT catalogue_install")
            .map_err(storage)?;
        let result = inner.record_catalogue(&head, &records).and_then(|()| {
            let schema = snapshot
                .schemas
                .iter()
                .find(|s| s.id == head.current)
                .ok_or_else(|| invalid("current catalogue schema is missing"))?;
            inner.record_policy_descriptor(&PolicyDescriptorV1 {
                head: head.clone(),
                schema: schema.clone(),
            })
        });
        if result.is_ok() {
            inner
                .conn
                .execute_batch("RELEASE catalogue_install")
                .map_err(storage)?;
        } else {
            inner
                .conn
                .execute_batch("ROLLBACK TO catalogue_install; RELEASE catalogue_install")
                .map_err(storage)?;
        }
        drop(inner);
        result?;
        self.broadcast_catalogue_metadata()
    }

    /// Read an accepted identity without fetching, granting access or minting
    /// UUIDs. Fixed views retain their descriptor; owners follow the catalogue's
    /// accepted current write schema, independently of their opened query schema.
    pub fn table_identity(&self, table: &str) -> Result<Option<GlobalPhysicalTableId>, Error> {
        let inner = self.inner();
        let head = inner
            .catalogue_head()?
            .ok_or_else(|| absent("accepted catalogue is unavailable"))?;
        let version = self
            .view
            .as_ref()
            .map_or(head.current, |v| v.jazz.version_id());
        Ok(inner.catalogue_table(version, table)?.map(|t| t.id))
    }
    /// Read the accepted column epoch in this handle's catalogue view.
    pub fn column_identity(
        &self,
        table: &str,
        column: &str,
    ) -> Result<Option<GlobalPhysicalColumnId>, Error> {
        let inner = self.inner();
        let head = inner
            .catalogue_head()?
            .ok_or_else(|| absent("accepted catalogue is unavailable"))?;
        let version = self
            .view
            .as_ref()
            .map_or(head.current, |v| v.jazz.version_id());
        Ok(inner
            .catalogue_table(version, table)?
            .and_then(|t| t.columns.get(column).map(|c| c.id)))
    }
    /// Fresh authority catalogue coverage through the normal query/policy path.
    /// Offline metadata lookup is deliberately a separate API.
    pub async fn cover_catalogue(
        &self,
        table: &str,
        scope: Option<(AuthorSubject, BTreeMap<String, Value>)>,
    ) -> Result<(), Error> {
        let opened = self.inner().schema.jazz.version_id();
        if self.is_client() {
            self.fetch_catalogue(table, opened, scope).await
        } else {
            self.authority_catalogue_coverage(table, opened, scope)
                .map(drop)
        }
    }

    pub(super) fn authority_catalogue_coverage(
        &self,
        table: &str,
        opened: SchemaVersionId,
        scope: Option<(AuthorSubject, BTreeMap<String, Value>)>,
    ) -> Result<(CatalogueHeadV1, Vec<CatalogueSchemaV1>), Error> {
        let inner = self.inner();
        if !inner.authority || inner.client.is_some() {
            return Err(absent("fresh catalogue coverage requires the authority"));
        }
        if inner.catalogue_schema(opened)?.is_none() {
            return Err(absent("opened schema has no accepted catalogue identity"));
        }
        let view = inner
            .governed(opened)
            .ok_or_else(|| absent("opened catalogue schema is not admitted"))?;
        let (head, records) = inner.catalogue_records(opened)?;
        if head.current != inner.owner.jazz.version_id() {
            return Err(absent(
                "catalogue head and installed policies are not aligned",
            ));
        }
        drop(inner);
        let db = Db {
            inner: self.inner.clone(),
            view: Some(view),
        };
        let prepared = db.prepare_query(&Query::from(table).limit(0))?;
        let (prepared, author) = match scope {
            Some((author, claims)) => (prepared.with_identity_claims(author, claims), Some(author)),
            None => (prepared, None),
        };
        // LIMIT 0 avoids row bodies, while the ordinary compiler still checks
        // sources and policy semantics. Coverage is not permission to read rows.
        db.inner().run(&prepared, author, &ReadSpec::default())?;
        Ok((head, records))
    }
}

fn validate_snapshot(
    snapshot: &CatalogueSnapshot,
) -> Result<(CatalogueHeadV1, Vec<CatalogueSchemaV1>), Error> {
    let mut schemas = BTreeMap::new();
    for version in &snapshot.schemas {
        if version.id != version.schema.version_id()
            || schemas.insert(version.id, version).is_some()
        {
            return Err(invalid("invalid or duplicate catalogue schema"));
        }
    }
    let targets: BTreeSet<_> = snapshot.lineages.iter().map(|(_, p)| p.schema.id).collect();
    if targets.len() != snapshot.lineages.len() {
        return Err(invalid("duplicate catalogue lineage target"));
    }
    let genesis = schemas
        .values()
        .filter(|s| !targets.contains(&s.id))
        .collect::<Vec<_>>();
    if genesis.len() != 1 {
        return Err(invalid("catalogue requires one genesis descriptor"));
    }
    let genesis = genesis[0];
    let first = CatalogueSchemaV1 {
        schema: SchemaVersion::new(genesis.schema.without_permissions()),
        identities: snapshot.genesis_physical_identities.clone(),
    };
    first.validate()?;
    let root = *blake3::Hasher::new_derive_key("jazz.sqlite.catalogue-namespace.v1")
        .update(&encode(&(genesis.id, &first.identities))?)
        .finalize()
        .as_bytes();
    let mut sources = BTreeMap::from([(
        genesis.id,
        (genesis.schema.clone(), first.identities.clone()),
    )]);
    let mut records = vec![first];
    let mut lineages = snapshot.lineages.iter().collect::<Vec<_>>();
    lineages.sort_by_key(|(seq, _)| *seq);
    let mut through = 0;
    for (seq, publication) in lineages {
        if *seq <= through
            || publication.id != publication.content_id()
            || publication.predecessors.is_empty()
        {
            return Err(invalid("invalid catalogue lineage ordering or identity"));
        }
        through = *seq;
        let schema = schemas
            .get(&publication.schema.id)
            .ok_or_else(|| invalid("catalogue publication descriptor missing"))?;
        if !schema
            .schema
            .has_same_lineage_structure(&publication.schema.schema)
        {
            return Err(invalid("catalogue publication descriptor mismatch"));
        }
        let mut predecessors = BTreeSet::new();
        for predecessor in &publication.predecessors {
            if !predecessors.insert(predecessor.lens.source) {
                return Err(invalid("duplicate catalogue predecessor"));
            }
            let source = &sources
                .get(&predecessor.lens.source)
                .ok_or_else(|| invalid("catalogue predecessor is not published"))?
                .0;
            let canonical = crate::protocol::MigrationLens::new(
                predecessor.lens.source,
                predecessor.lens.target,
                predecessor.lens.table_lenses.clone(),
            )
            .map_err(invalid)?;
            if canonical != predecessor.lens {
                return Err(invalid("invalid catalogue lens identity"));
            }
            let mut source_tables = BTreeSet::new();
            let mut target_tables = BTreeSet::new();
            for table in &predecessor.lens.table_lenses {
                source_tables.insert(table.source_table.as_str());
                target_tables.insert(table.target_table.as_str());
            }
            for table in &predecessor.dropped_tables {
                if !source_tables.insert(table.as_str()) {
                    return Err(invalid("duplicate or mapped dropped catalogue table"));
                }
            }
            for table in &predecessor.new_tables {
                if !target_tables.insert(table.as_str()) {
                    return Err(invalid("duplicate or mapped new catalogue table"));
                }
            }
            if source_tables != source.tables().iter().map(|t| t.name.as_str()).collect()
                || target_tables
                    != schema
                        .schema
                        .tables()
                        .iter()
                        .map(|t| t.name.as_str())
                        .collect()
            {
                return Err(invalid("catalogue table declarations are incomplete"));
            }
            if predecessor.lens.target != schema.id {
                return Err(invalid("catalogue lens targets another schema"));
            }
            LensEffects::of(&predecessor.lens)?;
        }
        publication
            .validate_predecessor_identities(&sources, records.iter().map(|r| r.identities.clone()))
            .map_err(invalid)?;
        let record = CatalogueSchemaV1 {
            schema: SchemaVersion::new(schema.schema.without_permissions()),
            identities: publication.physical_identities.clone(),
        };
        record.validate()?;
        sources.insert(
            schema.id,
            (schema.schema.clone(), record.identities.clone()),
        );
        records.push(record);
    }
    if !sources.contains_key(&snapshot.current_write_schema.schema) {
        return Err(invalid("catalogue active schema is not published"));
    }
    Ok((
        CatalogueHeadV1 {
            root,
            through,
            revision: snapshot.current_write_schema.revision,
            current: snapshot.current_write_schema.schema,
        },
        records,
    ))
}

// The SQL compiler receives this capability rather than a caller-populated map.
// Each lookup touches only the selected schema/table's accepted durable record.
impl crate::compile::CatalogueIdentityLookup for Inner {
    fn table_identity(&self, schema: SchemaVersionId, table: &str) -> Result<uuid::Uuid, Error> {
        if self.catalogue_head()?.is_none() {
            return Err(unsupported(
                "CatalogueTableIdentityV1 without accepted catalogue metadata",
            ));
        }
        self.catalogue_table(schema, table)?
            .map(|table| table.id.0)
            .ok_or_else(|| unsupported(format!(
                "CatalogueTableIdentityV1 has no accepted manifest for schema {schema:?} table `{table}`"
            )))
    }
}
