//! Host-owned, app-scoped captures of accepted SQLite catalogue metadata.
//! The envelope is not a signature: only an authenticated upstream or an
//! explicitly trusted host cache may supply the metadata it contains.
use super::*;
use serde::{Deserialize, Serialize};

const HEADER: &[u8] = b"JAZZ-SQLITE-CATALOGUE\0\x01";
pub(super) const DDL: &str = "CREATE TABLE IF NOT EXISTS _jazz_catalogue_cache_mode_v1 (slot INTEGER PRIMARY KEY CHECK(slot=1))";

/// `jazz.sqlite.catalogue-capture.v1`: head followed by all accepted descriptor
/// records in ascending schema UUID order. No row data or policy claims.
#[derive(Serialize, Deserialize)]
struct CaptureV1 {
    head: CatalogueHeadV1,
    schemas: Vec<CatalogueSchemaV1>,
}
fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::Protocol, message)
}
impl CaptureV1 {
    fn validate(&self) -> Result<(), Error> {
        if self.head.root == [0; 32]
            || self.head.current.0.is_nil()
            || !self
                .schemas
                .iter()
                .any(|s| s.schema.id == self.head.current)
        {
            return Err(invalid(
                "invalid authenticated SQLite catalogue capture head",
            ));
        }
        let mut previous = None;
        for record in &self.schemas {
            record.validate()?;
            if previous.is_some_and(|id| id >= record.schema.id) {
                return Err(invalid(
                    "catalogue capture descriptors must be unique and sorted",
                ));
            }
            previous = Some(record.schema.id);
        }
        Ok(())
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let body = bytes
            .strip_prefix(HEADER)
            .ok_or_else(|| invalid("unsupported authenticated catalogue capture encoding"))?;
        let (capture, trailing): (Self, _) = postcard::take_from_bytes(body)
            .map_err(|e| invalid(format!("invalid catalogue capture: {e}")))?;
        if !trailing.is_empty() {
            return Err(invalid("trailing catalogue capture bytes"));
        }
        capture.validate()?;
        // Reject alternate/noncanonical encodings at the host trust boundary.
        if capture.encode()? != bytes {
            return Err(invalid("noncanonical catalogue capture bytes"));
        }
        Ok(capture)
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = HEADER.to_vec();
        bytes.extend(postcard::to_allocvec(self).map_err(|e| invalid(e.to_string()))?);
        Ok(bytes)
    }
}

fn follows(previous: &CatalogueHeadV1, next: &CatalogueHeadV1) -> Result<(), Error> {
    if previous.root != next.root {
        return Err(invalid("catalogue cache authority namespace changed"));
    }
    if next.through < previous.through || next.revision < previous.revision {
        return Err(invalid("catalogue cache replacement moves backwards"));
    }
    if next.revision == previous.revision && next.current != previous.current {
        return Err(invalid(
            "catalogue cache has conflicting active schema revision",
        ));
    }
    Ok(())
}

/// Validate replacement within one exact registry/application/environment scope.
/// The host checks that scope; this function checks metadata and monotonicity.
pub fn validate_catalogue_capture_replacement(previous: &[u8], next: &[u8]) -> Result<(), Error> {
    let previous = CaptureV1::decode(previous)?;
    let next = CaptureV1::decode(next)?;
    follows(&previous.head, &next.head)?;
    for old in previous.schemas {
        let index = next
            .schemas
            .binary_search_by_key(&old.schema.id, |s| s.schema.id)
            .map_err(|_| invalid("catalogue cache replacement omits accepted descriptor"))?;
        let new = &next.schemas[index];
        compatible_descriptor(&old, new)?;
    }
    Ok(())
}

fn compatible_descriptor(old: &CatalogueSchemaV1, new: &CatalogueSchemaV1) -> Result<(), Error> {
    if old.identities != new.identities
        || !old
            .schema
            .schema
            .has_same_lineage_structure(&new.schema.schema)
    {
        return Err(invalid(
            "catalogue cache replacement rebinds accepted identities",
        ));
    }
    Ok(())
}

/// Combine two already-authenticated captures in the same exact host scope.
/// Owners may have received different descriptor subsets or publish out of
/// order. Keep their union and the newer comparable head; shared identities
/// must agree. This neither authenticates input nor grants fresh coverage.
/// The host must run this against the current stored capture inside its atomic
/// publication operation, never against a separately loaded stale copy.
/// The result uses the existing canonical catalogue-capture.v1 encoding.
pub fn merge_catalogue_captures(previous: &[u8], next: &[u8]) -> Result<Vec<u8>, Error> {
    let previous = CaptureV1::decode(previous)?;
    let next = CaptureV1::decode(next)?;
    let (older, newer) = if follows(&previous.head, &next.head).is_ok() {
        (previous, next)
    } else {
        follows(&next.head, &previous.head)?;
        (next, previous)
    };
    let mut records = older
        .schemas
        .into_iter()
        .map(|record| (record.schema.id, record))
        .collect::<BTreeMap<_, _>>();
    for record in newer.schemas {
        if let Some(previous) = records.get(&record.schema.id) {
            compatible_descriptor(previous, &record)?;
        }
        records.insert(record.schema.id, record);
    }
    let merged = CaptureV1 {
        head: newer.head,
        schemas: records.into_values().collect(),
    };
    merged.validate()?;
    merged.encode()
}

/// One atomic observation. Readiness qualifies this handle's opened schema;
/// capture ownership is shared across all handles. Hosts must durably publish
/// moved bytes before exposing readiness, retaining them if publication fails.
pub struct AuthenticatedCatalogueState {
    pub capture: Option<Vec<u8>>,
    pub ready: bool,
}

impl Inner {
    fn catalogue_ready(&self, schema: &Schema) -> Result<bool, Error> {
        if self.catalogue_head()?.is_none() {
            return Ok(false);
        }
        // Descriptors are validated before atomic installation and immutable
        // afterwards. Admission needs only the indexed receipt, not a decode
        // of every unrelated table in the requested schema on each query.
        self.conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM _jazz_catalogue_schema_v1 WHERE version=?1)",
                [schema.jazz.version_id().as_bytes().as_slice()],
                |row| row.get(0),
            )
            .map_err(storage)
    }
    pub(super) fn needs_catalogue_schema(&self, schema: &Schema) -> Result<bool, Error> {
        Ok(self.catalogue_cache_required && !self.catalogue_ready(schema)?)
    }
    pub(super) fn require_catalogue_schema(&self, schema: &Schema) -> Result<(), Error> {
        if self.needs_catalogue_schema(schema)? {
            return Err(Error::new(
                ErrorCode::Schema,
                "requested schema is not admitted by the authenticated catalogue",
            ));
        }
        Ok(())
    }
    fn capture_catalogue(&self, head: CatalogueHeadV1) -> Result<Vec<u8>, Error> {
        // Export explicitly requests the complete locally accepted catalogue.
        // Cost is proportional to the capture, never to application rows or
        // authority history that this replica has not received.
        let mut statement = self
            .conn
            .prepare("SELECT version FROM _jazz_catalogue_schema_v1 ORDER BY version")
            .map_err(storage)?;
        let keys = statement
            .query_map([], |row| row.get::<_, Vec<u8>>(0))
            .map_err(storage)?;
        let mut schemas = Vec::new();
        for key in keys {
            let key = key.map_err(storage)?;
            let version = crate::ids::SchemaVersionId(
                uuid::Uuid::from_slice(&key)
                    .map_err(|e| Error::new(ErrorCode::Storage, e.to_string()))?,
            );
            schemas.push(
                self.catalogue_schema(version)?.ok_or_else(|| {
                    Error::new(ErrorCode::Storage, "missing catalogue descriptor")
                })?,
            );
        }
        let capture = CaptureV1 { head, schemas };
        capture.validate()?;
        capture.encode()
    }
}

impl Db {
    /// Qualify this view without consuming its owner's pending capture.
    pub fn has_authenticated_catalogue(&self) -> Result<bool, Error> {
        let inner = self.inner();
        inner.catalogue_ready(&inner.schema)
    }
    /// Move the owner's current capture once, even when this requested schema
    /// is unpublished. Identical upstream metadata may produce another capture;
    /// merely installing cached bytes does not echo them back to their host.
    pub fn take_authenticated_catalogue_state(&self) -> Result<AuthenticatedCatalogueState, Error> {
        let inner = self.inner();
        let ready = inner.catalogue_ready(&inner.schema)?;
        let capture = if inner.catalogue_capture_pending.get() {
            inner
                .catalogue_head()?
                .map(|head| inner.capture_catalogue(head))
                .transpose()?
        } else {
            None
        };
        inner.catalogue_capture_pending.set(false);
        Ok(AuthenticatedCatalogueState { capture, ready })
    }

    /// Restore trusted host metadata before exposing this runtime. This neither
    /// installs policies, grants fresh coverage nor makes the store an authority.
    /// A newer durable head wins over a stale cache; immutable IDs must agree.
    ///
    /// # Safety
    /// Bytes must come from a prior authenticated capture in the same exact
    /// registry/application/environment scope. This format does not authenticate
    /// arbitrary caller input. Call only during owner open, before sharing it.
    pub unsafe fn install_cached_catalogue(&self, bytes: &[u8]) -> Result<(), Error> {
        let capture = CaptureV1::decode(bytes)?;
        if self.view.is_some() {
            return Err(invalid(
                "catalogue cache must be installed by the runtime owner",
            ));
        }
        let mut inner = self.inner();
        let previous = inner.catalogue_head()?;
        let head = if let Some(previous) = &previous {
            if follows(previous, &capture.head).is_ok() {
                capture.head.clone()
            } else {
                follows(&capture.head, previous)?;
                previous.clone()
            }
        } else {
            capture.head.clone()
        };
        let pending = inner.catalogue_capture_pending.get();
        inner
            .conn
            .execute_batch("SAVEPOINT catalogue_cache_install")
            .map_err(storage)?;
        let result = (|| {
            inner.record_catalogue(&head, &capture.schemas)?;
            inner
                .conn
                .execute(
                    "INSERT OR IGNORE INTO _jazz_catalogue_cache_mode_v1(slot) VALUES(1)",
                    [],
                )
                .map_err(storage)?;
            Ok(())
        })();
        if result.is_ok() {
            inner
                .conn
                .execute_batch("RELEASE catalogue_cache_install")
                .map_err(storage)?;
            inner.catalogue_cache_required = true;
            // A reopened durable root may be ahead of the independent host
            // cache. Export its union again; fresh cache-only opens do not echo.
            inner.catalogue_capture_pending.set(previous.is_some());
        } else {
            inner
                .rollback_savepoint("catalogue_cache_install")
                .map_err(storage)?;
            inner.catalogue_capture_pending.set(pending);
        }
        result
    }
}
