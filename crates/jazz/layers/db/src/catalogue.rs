//! The deploy catalogue: every schema, lens and permissions revision a
//! store knows, as rows beside the stored names they produced.
//!
//! Definitions are immutable and kept whole, as canonical JSON text (object
//! keys sorted, no whitespace), so SQL reads them with `json_each` and a
//! definition is never stored twice. What the store decided about them is
//! kept in real columns: which admitted version a schema is (`n`, see
//! [`crate::names`]), when it was published, and which permissions revision
//! was activated at which seq.
//!
//! - `_jazz_schemas`: one row per structural schema (policies cleared), by
//!   its public content hash. `n` is the admitted version it compiles to;
//!   `NULL` until it is admitted (a schema published without a lens).
//!   Several hashes can compile to one version (defaults and indexes do not
//!   change it); the store records the one it admitted. `published_at` is
//!   set on the store that published it (an authority), `NULL` elsewhere.
//! - `_jazz_lenses`: the published migrations, by (source, target) hash,
//!   with their forward transform. A replica keeps none: its upstream sends
//!   the lens cells they made instead.
//! - `_jazz_permissions`: each permissions revision, with the write schema
//!   it selects and its per-table policies. The current one is the highest
//!   revision; its parent is the revision before it. `activated_seq` is the
//!   seq at which the read policies last changed when it was activated, and
//!   `read_digest` a digest of them (both `NULL` until activated).
//!
//! An authority's server writes these in the same transaction as the
//! admission they describe, so a deploy is never half written. A store
//! with no server writes schemas and permissions as it admits and
//! activates them.

use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::ids::SchemaVersionId;
use crate::options::{Error, ErrorCode};
use crate::schema::JazzSchema;
use crate::values::{CachedRow, storage};
use jazz_model::model::public_schema::{
    Schema as PublicSchema, SchemaHash, TableName, TablePolicies,
};
use jazz_model::model::schema_lens::LensTransform;

pub(crate) const DDL: &str = "\
    CREATE TABLE IF NOT EXISTS _jazz_schemas (hash BLOB PRIMARY KEY, n INTEGER, \
     published_at INTEGER, def TEXT NOT NULL) WITHOUT ROWID;\
    CREATE TABLE IF NOT EXISTS _jazz_lenses (source BLOB NOT NULL, target BLOB NOT NULL, \
     published_at INTEGER NOT NULL, def TEXT NOT NULL, PRIMARY KEY (source, target)) WITHOUT ROWID;\
    CREATE TABLE IF NOT EXISTS _jazz_permissions (revision INTEGER PRIMARY KEY, hash BLOB NOT NULL, \
     published_at INTEGER, activated_seq INTEGER, read_digest INTEGER, def TEXT NOT NULL);";

/// A table's policies, by table name.
pub type Permissions = BTreeMap<TableName, TablePolicies>;

/// A structural schema the store knows.
#[derive(Clone, Debug, PartialEq)]
pub struct CatalogueSchema {
    pub hash: SchemaHash,
    /// Its place in admission order: the first admitted (the store's
    /// genesis) is lowest. `None` while it is not admitted.
    pub admitted: Option<u64>,
    /// The admitted version it compiles to, once admitted.
    pub version: Option<SchemaVersionId>,
    /// Unix milliseconds, on the store that published it.
    pub published_at: Option<u64>,
    pub schema: PublicSchema,
}

/// A published migration.
#[derive(Clone, Debug, PartialEq)]
pub struct CatalogueLens {
    pub source: SchemaHash,
    pub target: SchemaHash,
    /// Unix milliseconds.
    pub published_at: u64,
    pub forward: LensTransform,
}

/// One permissions revision.
#[derive(Clone, Debug, PartialEq)]
pub struct PermissionsRevision {
    pub revision: u64,
    /// The write schema it selects (structural hash).
    pub schema: SchemaHash,
    /// Unix milliseconds, on the store that published it.
    pub published_at: Option<u64>,
    /// The seq its read policies took effect at, once activated.
    pub activated_seq: Option<u64>,
    pub permissions: Permissions,
}

/// Everything in the catalogue, each list in key order (permissions by
/// revision).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CatalogueRows {
    pub schemas: Vec<CatalogueSchema>,
    pub lenses: Vec<CatalogueLens>,
    pub permissions: Vec<PermissionsRevision>,
}

impl CatalogueRows {
    /// The current permissions revision.
    pub fn head(&self) -> Option<&PermissionsRevision> {
        self.permissions.last()
    }

    /// The first version the store admitted.
    pub fn genesis(&self) -> Option<&CatalogueSchema> {
        self.schemas
            .iter()
            .filter(|schema| schema.admitted.is_some())
            .min_by_key(|schema| schema.admitted)
    }

    pub fn schema(&self, hash: &SchemaHash) -> Option<&CatalogueSchema> {
        self.schemas.iter().find(|schema| schema.hash == *hash)
    }
}

/// What a host publishes in one write: all or none.
#[derive(Clone, Debug, Default)]
pub struct CataloguePut {
    /// Structural schemas (policies are ignored), with their publication
    /// time. A schema published again keeps the later time.
    pub schemas: Vec<(PublicSchema, u64)>,
    pub lenses: Vec<CatalogueLens>,
    /// Revisions to record; one already recorded is replaced, keeping its
    /// activation.
    pub permissions: Vec<(u64, SchemaHash, Permissions, u64)>,
}

impl CataloguePut {
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty() && self.lenses.is_empty() && self.permissions.is_empty()
    }
}

/// A catalogue change a host applies in one transaction (see
/// [`crate::db::Db::apply_catalogue`]).
#[derive(Clone, Debug, Default)]
pub struct CatalogueChange {
    pub put: CataloguePut,
    /// Versions to admit, in order, each with the lenses into it.
    pub admit: Vec<(JazzSchema, Vec<crate::protocol::MigrationLens>)>,
    /// The schema (with its policies) to make current, at a permissions
    /// revision, with the lenses into it.
    pub activate: Option<(u64, JazzSchema, Vec<crate::protocol::MigrationLens>)>,
}

/// `value` as canonical JSON: object keys sorted at every level, no
/// whitespace. Equal values are equal text, so a definition can be compared
/// and content-addressed as stored.
pub fn canonical_json(value: &impl Serialize) -> Result<String, Error> {
    fn sorted(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let entries: BTreeMap<String, serde_json::Value> =
                    map.into_iter().map(|(k, v)| (k, sorted(v))).collect();
                serde_json::Value::Object(entries.into_iter().collect())
            }
            serde_json::Value::Array(items) => {
                serde_json::Value::Array(items.into_iter().map(sorted).collect())
            }
            other => other,
        }
    }
    let value = serde_json::to_value(value).map_err(|error| {
        Error::new(
            ErrorCode::Storage,
            format!("encoding a catalogue definition: {error}"),
        )
    })?;
    serde_json::to_string(&sorted(value)).map_err(|error| {
        Error::new(
            ErrorCode::Storage,
            format!("encoding a catalogue definition: {error}"),
        )
    })
}

fn decode<T: DeserializeOwned>(def: &str, what: impl std::fmt::Display) -> Result<T, Error> {
    serde_json::from_str(def)
        .map_err(|error| Error::new(ErrorCode::Storage, format!("bad {what}: {error}")))
}

fn hash_of(bytes: Vec<u8>, what: &str) -> Result<SchemaHash, Error> {
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| Error::new(ErrorCode::Storage, format!("bad hash in {what}")))?;
    Ok(SchemaHash::from_bytes(bytes))
}

/// `schema` without its policies, which permissions revisions carry.
fn structural(schema: &PublicSchema) -> PublicSchema {
    let mut schema = schema.clone();
    for table in schema.values_mut() {
        table.policies = TablePolicies::default();
    }
    schema
}

/// The structural hash of `schema` and its definition.
fn structural_def(schema: &PublicSchema) -> Result<(SchemaHash, String), Error> {
    let schema = structural(schema);
    Ok((SchemaHash::compute(&schema), canonical_json(&schema)?))
}

/// A schema's per-table policies. A table left at the default policies
/// (deny everything) has no entry, so equal permissions are equal text.
pub fn permissions_of(schema: &PublicSchema) -> Permissions {
    schema
        .iter()
        .map(|(name, table)| (*name, table.policies.clone()))
        .filter(|(_, policies)| *policies != TablePolicies::default())
        .collect()
}

fn now_millis() -> u64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or_default()
}

/// Write `put` (the caller holds a savepoint or transaction).
pub(crate) fn put(conn: &Connection, put: &CataloguePut) -> Result<(), Error> {
    conn.execute_batch(DDL).map_err(storage)?;
    for (schema, published_at) in &put.schemas {
        let (hash, def) = structural_def(schema)?;
        conn.prepare_cached(
            "INSERT INTO _jazz_schemas (hash, n, published_at, def) VALUES (?1, NULL, ?2, ?3) \
             ON CONFLICT (hash) DO UPDATE SET \
             published_at = max(coalesce(published_at, 0), excluded.published_at)",
        )
        .and_then(|mut statement| {
            statement.execute(params![hash.as_bytes().to_vec(), *published_at as i64, def])
        })
        .map_err(storage)?;
    }
    for lens in &put.lenses {
        let def = canonical_json(&lens.forward)?;
        conn.prepare_cached(
            "INSERT OR REPLACE INTO _jazz_lenses (source, target, published_at, def) \
             VALUES (?1, ?2, ?3, ?4)",
        )
        .and_then(|mut statement| {
            statement.execute(params![
                lens.source.as_bytes().to_vec(),
                lens.target.as_bytes().to_vec(),
                lens.published_at as i64,
                def
            ])
        })
        .map_err(storage)?;
    }
    for (revision, schema, permissions, published_at) in &put.permissions {
        let permissions: Permissions = permissions
            .iter()
            .filter(|(_, policies)| **policies != TablePolicies::default())
            .map(|(name, policies)| (*name, policies.clone()))
            .collect();
        let def = canonical_json(&permissions)?;
        conn.prepare_cached(
            "INSERT INTO _jazz_permissions (revision, hash, published_at, def) \
             VALUES (?1, ?2, ?3, ?4) ON CONFLICT (revision) DO UPDATE SET \
             hash = excluded.hash, published_at = excluded.published_at, def = excluded.def",
        )
        .and_then(|mut statement| {
            statement.execute(params![
                *revision as i64,
                schema.as_bytes().to_vec(),
                *published_at as i64,
                def
            ])
        })
        .map_err(storage)?;
    }
    Ok(())
}

fn exists(conn: &Connection) -> Result<bool, Error> {
    conn.cached_row(
        "SELECT count(*) = 3 FROM sqlite_master WHERE type = 'table' \
         AND name IN ('_jazz_schemas', '_jazz_lenses', '_jazz_permissions')",
        [],
        |r| r.get(0),
    )
    .map_err(storage)
}

/// The whole catalogue. A file with none (a store no version was admitted
/// to yet) reads empty.
pub(crate) fn read(conn: &Connection) -> Result<CatalogueRows, Error> {
    let mut rows = CatalogueRows::default();
    if !exists(conn)? {
        return Ok(rows);
    }
    let has_versions: bool = conn
        .cached_row(
            "SELECT count(*) > 0 FROM sqlite_master WHERE type = 'table' AND name = '_jazz_versions'",
            [],
            |r| r.get(0),
        )
        .map_err(storage)?;
    let sql = if has_versions {
        "SELECT s.hash, v.version, s.published_at, s.def, v.n FROM _jazz_schemas s \
         LEFT JOIN _jazz_versions v ON v.n = s.n ORDER BY s.hash"
    } else {
        "SELECT hash, NULL, published_at, def, NULL FROM _jazz_schemas ORDER BY hash"
    };
    let mut statement = conn.prepare(sql).map_err(storage)?;
    let found = statement
        .query_map([], |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, Option<Vec<u8>>>(1)?,
                r.get::<_, Option<i64>>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, Option<i64>>(4)?,
            ))
        })
        .map_err(storage)?;
    for row in found {
        let (hash, version, published_at, def, admitted) = row.map_err(storage)?;
        let hash = hash_of(hash, "_jazz_schemas")?;
        let version = version
            .map(|version| {
                version
                    .try_into()
                    .map(SchemaVersionId::from_bytes)
                    .map_err(|_| {
                        Error::new(ErrorCode::Storage, "bad schema version in _jazz_versions")
                    })
            })
            .transpose()?;
        let schema: PublicSchema = decode(&def, format_args!("schema {hash} in _jazz_schemas"))?;
        if SchemaHash::compute(&schema) != hash {
            return Err(Error::new(
                ErrorCode::Storage,
                format!("schema {hash} in _jazz_schemas does not hash to its key"),
            ));
        }
        rows.schemas.push(CatalogueSchema {
            hash,
            admitted: admitted.map(|n| n as u64),
            version,
            published_at: published_at.map(|at| at as u64),
            schema,
        });
    }
    drop(statement);
    let mut statement = conn
        .prepare(
            "SELECT source, target, published_at, def FROM _jazz_lenses ORDER BY source, target",
        )
        .map_err(storage)?;
    let found = statement
        .query_map([], |r| {
            Ok((
                r.get::<_, Vec<u8>>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, String>(3)?,
            ))
        })
        .map_err(storage)?;
    for row in found {
        let (source, target, published_at, def) = row.map_err(storage)?;
        let (source, target) = (
            hash_of(source, "_jazz_lenses")?,
            hash_of(target, "_jazz_lenses")?,
        );
        rows.lenses.push(CatalogueLens {
            source,
            target,
            published_at: published_at as u64,
            forward: decode(
                &def,
                format_args!("lens {source} -> {target} in _jazz_lenses"),
            )?,
        });
    }
    drop(statement);
    let mut statement = conn
        .prepare(
            "SELECT revision, hash, published_at, activated_seq, def FROM _jazz_permissions \
             ORDER BY revision",
        )
        .map_err(storage)?;
    let found = statement
        .query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, Option<i64>>(2)?,
                r.get::<_, Option<i64>>(3)?,
                r.get::<_, String>(4)?,
            ))
        })
        .map_err(storage)?;
    for row in found {
        let (revision, hash, published_at, activated_seq, def) = row.map_err(storage)?;
        rows.permissions.push(PermissionsRevision {
            revision: revision as u64,
            schema: hash_of(hash, "_jazz_permissions")?,
            published_at: published_at.map(|at| at as u64),
            activated_seq: activated_seq.map(|seq| seq as u64),
            permissions: decode(
                &def,
                format_args!("permissions revision {revision} in _jazz_permissions"),
            )?,
        });
    }
    Ok(rows)
}

/// Record that admitted version `n` is `schema` (structural). A schema
/// with no public source (an engine test's, built from compiled tables)
/// has no definition to keep, so it is not recorded.
pub(crate) fn record_admitted(conn: &Connection, n: i64, schema: &JazzSchema) -> Result<(), Error> {
    if schema.public_schema().is_empty() && !schema.tables().is_empty() {
        return Ok(());
    }
    let (hash, def) = structural_def(schema.public_schema())?;
    conn.prepare_cached(
        "INSERT INTO _jazz_schemas (hash, n, published_at, def) VALUES (?1, ?2, NULL, ?3) \
         ON CONFLICT (hash) DO UPDATE SET n = excluded.n WHERE n IS NULL",
    )
    .and_then(|mut statement| statement.execute(params![hash.as_bytes().to_vec(), n, def]))
    .map_err(storage)?;
    Ok(())
}

/// The admitted versions' schemas, in admission order, each checked to
/// compile to the version it was admitted as.
pub(crate) fn admitted(conn: &Connection) -> Result<Vec<(SchemaVersionId, JazzSchema)>, Error> {
    let mut statement = conn
        .prepare(
            "SELECT v.version, s.def FROM _jazz_schemas s JOIN _jazz_versions v ON v.n = s.n \
             ORDER BY s.n",
        )
        .map_err(storage)?;
    let found = statement
        .query_map([], |r| {
            Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?))
        })
        .map_err(storage)?;
    let mut admitted = Vec::new();
    for row in found {
        let (version, def) = row.map_err(storage)?;
        let version =
            SchemaVersionId::from_bytes(version.try_into().map_err(|_| {
                Error::new(ErrorCode::Storage, "bad schema version in _jazz_versions")
            })?);
        let public: PublicSchema = decode(&def, format_args!("schema of version {version:?}"))?;
        let schema = JazzSchema::new(&public).map_err(|error| {
            Error::new(
                ErrorCode::Storage,
                format!("schema of version {version:?} no longer compiles: {error}"),
            )
        })?;
        if schema.version_id() != version {
            return Err(Error::new(
                ErrorCode::Storage,
                format!("schema of version {version:?} compiles to another version"),
            ));
        }
        admitted.push((version, schema));
    }
    Ok(admitted)
}

/// The seq the current read policies took effect at.
pub(crate) fn activated_seq(conn: &Connection) -> Result<Option<u64>, Error> {
    conn.cached_row(
        "SELECT activated_seq FROM _jazz_permissions WHERE activated_seq IS NOT NULL \
         ORDER BY revision DESC LIMIT 1",
        [],
        |r| r.get::<_, i64>(0),
    )
    .optional()
    .map(|seq| seq.map(|seq| seq as u64))
    .map_err(storage)
}

/// Record that `schema` (with its policies) was made current at `seq`,
/// with a digest of its read policies: at `revision`, or, without one, at
/// the current revision if it selects the same schema and policies, else
/// at the next. The read policies take effect at `seq` only if their digest
/// changed since the last activation.
pub(crate) fn record_activation(
    conn: &Connection,
    revision: Option<u64>,
    schema: &JazzSchema,
    read_digest: u64,
    seq: u64,
) -> Result<(), Error> {
    let (hash, _) = structural_def(schema.public_schema())?;
    let def = canonical_json(&permissions_of(schema.public_schema()))?;
    let head: Option<(i64, Vec<u8>, String)> = conn
        .cached_row(
            "SELECT revision, hash, def FROM _jazz_permissions ORDER BY revision DESC LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(storage)?;
    let revision = match (revision, &head) {
        (Some(revision), _) => revision as i64,
        (None, Some((revision, head_hash, head_def)))
            if *head_hash == hash.as_bytes() && *head_def == def =>
        {
            *revision
        }
        (None, Some((revision, ..))) => revision + 1,
        (None, None) => 1,
    };
    let last: Option<(i64, i64)> = conn
        .cached_row(
            "SELECT activated_seq, read_digest FROM _jazz_permissions \
             WHERE activated_seq IS NOT NULL ORDER BY revision DESC LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(storage)?;
    let activated_seq = match last {
        Some((at, digest)) if digest as u64 == read_digest => at,
        _ => seq as i64,
    };
    conn.prepare_cached(
        "INSERT INTO _jazz_permissions (revision, hash, published_at, activated_seq, read_digest, def) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT (revision) DO UPDATE SET \
         hash = excluded.hash, def = excluded.def, activated_seq = excluded.activated_seq, \
         read_digest = excluded.read_digest",
    )
    .and_then(|mut statement| {
        statement.execute(params![
            revision,
            hash.as_bytes().to_vec(),
            now_millis() as i64,
            activated_seq,
            read_digest as i64,
            def
        ])
    })
    .map_err(storage)?;
    Ok(())
}
