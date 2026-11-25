//! jazz-napi — Native Node.js bindings for the Jazz database (`jazz-db`).
//!
//! Provides Node.js bindings for the Jazz database, server helpers, and
//! device-account identity utilities.
//!
//! # Architecture
//!
//! - `NapiDb` exposes the database over the encoded-row boundary the
//!   TypeScript client packages share with the WASM binding. Both delegate to
//!   [`jazz::binding_host`]; this crate only adapts it to Node: options
//!   objects, pollable pending reads, write promises and the tick callback.
//!   Unsupported features fail with an explicit "not supported yet" error.
//! - `JazzServer` exposes the Rust server process used by integration tests
//!   and Node deployments.
//! - device-account JWT helpers stay here as package-level native utilities.
//!
//! # Thread affinity
//!
//! A database is `Rc`/thread-affine and NAPI's worker-pool promises need
//! `Send`, so nothing here runs off the JavaScript thread. `jazz-db` runs
//! every operation to completion synchronously; only reads and write fates
//! that need the server wait, and they complete on a later tick. Reads hand
//! JavaScript a pollable object the host drives after transport progress;
//! write waits are promises this crate settles after every call that can make
//! progress (see [`settle_promises`]).
//!
//! # Allocator
//!
//! This crate uses `mimalloc-safe` (napi-rs–maintained mimalloc fork) as Rust's
//! `#[global_allocator]`. It does NOT override the host process's `malloc`/`free` —
//! Node.js / V8 keep their own allocator. The two coexist safely as long as
//! memory crosses the FFI boundary **by copy**, which is what napi-rs does today
//! for Vec/String/Buffer returns.
//!
//! Footgun: never `Vec::leak` / `Box::into_raw` an allocation across FFI and let
//! the host call `free()` on it — that mixes allocators and corrupts the heap.
//! If a future zero-copy shim is added, hand the host a Rust-defined finalizer
//! callback that frees through mimalloc instead.

mod schema_checks;
mod serve_core;
mod text;

#[cfg(feature = "rn-test-bridge")]
mod rn_test_bridge;

#[global_allocator]
static GLOBAL: mimalloc_safe::MiMalloc = mimalloc_safe::MiMalloc;

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures::FutureExt;
use jazz::binding_host::{self, Binding, EventChunk, HostError, HostFuture, HostResult, OpenAs};
use jazz::db::{Error as CoreError, FileOptions, LargeValueUpdate, SubscriptionEvent as CoreEvent};
use jazz::db::{SubscriptionStream, block_on};
use jazz::identity;
use jazz::tools::AppId;
use jazz::tx::DurabilityTier;
use jazz_server::{
    AuthConfig, JazzServer as CoreJazzServer, ServerBuilder, ServerDataDir, StorageBackend,
};
mod embedded;
use jazz_e2ee_client::host::runtime as encryption;

use napi::bindgen_prelude::*;
use napi::sys;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use serde::Deserialize;

/// Any JSON-compatible value crossing the native JavaScript boundary.
///
/// Keep this alias exported through napi-rs rather than relying on its Rust
/// import name: exported methods use `JsonValue` throughout their generated
/// declarations, so the package must define that name for TypeScript
/// consumers.
#[napi]
pub type JsonValue = serde_json::Value;

/// Validate a schema and its embedded permissions without opening a database.
#[napi(js_name = "validateSchema")]
pub fn validate_schema(schema: Uint8Array) -> js::Result<()> {
    binding_host::validate_deployable_schema(&schema).map_err(host_error)?;
    Ok(())
}

/// Check an app's shipped migrations (the `ClientMigrations` JSON) leading
/// to the schema hash `target` as a deployment to a new authority would,
/// without opening a database. `jazz-tools validate` runs it.
#[napi(js_name = "checkClientMigrations")]
pub fn check_client_migrations(migrations: String, target: String) -> napi::Result<()> {
    jazz::tools::deployment::published::check_client_migrations(migrations.as_bytes(), &target)
        .map_err(napi::Error::from_reason)
}

/// An app's two files, as [`write_app_artifact`] renders them.
#[napi(object)]
pub struct WrittenAppFiles {
    /// `jazz.app.json`, which the app and its clients ship.
    pub app: String,
    /// `jazz.permissions.json`, which only its backends read.
    pub permissions: String,
}

/// The `$generated` marker to write: `{ by }`, what wrote the files.
#[napi(object)]
pub struct GeneratedBy {
    /// E.g. "jazz-tools generate from schema.ts — edit the TypeScript, not
    /// this file".
    pub by: String,
}

/// How [`write_app_artifact`] and [`write_app_files`] write the files.
#[napi(object)]
pub struct WriteAppFilesOptions {
    /// Mark each file `"$generated": { "by", "sha256" }`, first after
    /// `$schema`.
    pub generated: Option<GeneratedBy>,
}

/// Build the `jazz.app.json` and `jazz.permissions.json` texts from what
/// `jazz-tools validate` compiled (an
/// [`ArtifactInput`](jazz::tools::deployment::artifact::ArtifactInput) as
/// JSON): the files' only writer, which refuses what a reader would. With
/// `options.generated`, each file carries a `$generated` marker.
#[napi(js_name = "writeAppArtifact")]
pub fn write_app_artifact(
    input: String,
    options: Option<WriteAppFilesOptions>,
) -> js::Result<WrittenAppFiles> {
    use jazz::tools::deployment::artifact;
    let input: artifact::ArtifactInput = serde_json::from_str(&input)
        .map_err(|error| napi::Error::from_reason(format!("jazz.app.json input: {error}")))?;
    let written = match options.and_then(|o| o.generated) {
        Some(generated) => artifact::write_generated(input, &generated.by),
        None => artifact::write(input),
    }
    .map_err(artifact_error)?;
    Ok(WrittenAppFiles {
        app: written.app,
        permissions: written.permissions,
    })
}

/// Check an app's readable `jazz.app.json` (`app`) and
/// `jazz.permissions.json` (`permissions`) as a backend does and render
/// them canonically through the writer, which reads its output back to
/// exactly the same app. With `options.generated`, each file carries a
/// `$generated` marker. Idempotent.
#[napi(js_name = "writeAppFiles")]
pub fn write_app_files(
    app: String,
    permissions: String,
    options: Option<WriteAppFilesOptions>,
) -> js::Result<WrittenAppFiles> {
    let by = options.and_then(|o| o.generated).map(|g| g.by);
    let written = jazz::tools::deployment::artifact::rewrite(
        app.as_bytes(),
        permissions.as_bytes(),
        by.as_deref(),
    )
    .map_err(artifact_error)?;
    Ok(WrittenAppFiles {
        app: written.app,
        permissions: written.permissions,
    })
}

/// Whether a `jazz.app.json` or `jazz.permissions.json` text is as the tool
/// its `$generated` marker names wrote it: `"none"` (no marker), `"match"`
/// or `"mismatch"` (edited since; formatting alone doesn't count). Throws
/// when the text isn't a JSON object or the marker is malformed.
#[napi(
    js_name = "verifyGenerated",
    ts_return_type = "\"none\" | \"match\" | \"mismatch\""
)]
pub fn verify_generated(text: String) -> napi::Result<String> {
    use jazz::tools::deployment::artifact::{self, GeneratedCheck};
    Ok(
        match artifact::verify_generated(text.as_bytes()).map_err(napi::Error::from_reason)? {
            GeneratedCheck::Unmarked => "none",
            GeneratedCheck::Matches(_) => "match",
            GeneratedCheck::Mismatch { .. } => "mismatch",
        }
        .to_owned(),
    )
}

/// Read and check an app's `jazz.app.json` (`app`) and
/// `jazz.permissions.json` (`permissions`) as a backend does. Returns the
/// current schema hash and the bundle id the two files make.
#[napi(js_name = "loadAppArtifact")]
pub fn load_app_artifact(app: String, permissions: String) -> js::Result<JsonValue> {
    let loaded = jazz::tools::deployment::artifact::load(app.as_bytes(), permissions.as_bytes())
        .map_err(artifact_error)?;
    Ok(loaded_backend(&loaded))
}

/// What a backend needs of its loaded files: the current schema hash, the
/// bundle id, the engine permissions (`{ [table]: TablePolicies }`), the
/// body it publishes (`deployment`, the server's `DeployRequest` with
/// `allowDroppingHistory: false`) and what loaded but is ignored.
fn loaded_backend(loaded: &jazz::tools::deployment::artifact::LoadedApp) -> JsonValue {
    serde_json::json!({
        "current": loaded.current_hash().to_string(),
        "bundle": loaded.bundle().to_hex(),
        "permissions": loaded.permissions(),
        "deployment": loaded.deployment_body(false),
        "warnings": loaded.warnings(),
    })
}

/// Read and check a backend's `jazz.permissions.json` (`permissions`)
/// against the compiled app its generated bindings embed (`schema`: JSON of
/// `{ schema, relations }`, the options they pass to `bindApp`), for a
/// deployed backend that has the bindings but not `jazz.app.json`. Same
/// result and checks as `loadAppArtifact`.
#[napi(js_name = "loadBackendApp")]
pub fn load_backend_app(schema: String, permissions: String) -> js::Result<JsonValue> {
    let loaded =
        jazz::tools::deployment::artifact::load_backend(schema.as_bytes(), permissions.as_bytes())
            .map_err(artifact_error)?;
    Ok(loaded_backend(&loaded))
}

/// The bindings for `appJson` (a `jazz.app.json` text) in `lang` (`"ts"`):
/// the whole `jazz.generated.ts`.
#[napi(js_name = "generate")]
pub fn generate_bindings(lang: String, app_json: String) -> js::Result<String> {
    let lang = jazz_codegen::Lang::parse(&lang).map_err(codegen_error)?;
    jazz_codegen::generate(lang, app_json.as_bytes()).map_err(codegen_error)
}

/// The options the generated TS bindings pass to `bindApp` (`{ source,
/// schema, relations }`, without the computed and transform callbacks), as
/// JSON text, for tests that define an app inline.
#[napi(js_name = "compileApp")]
pub fn compile_app(app_json: String) -> js::Result<String> {
    jazz_codegen::compile_app(app_json.as_bytes()).map_err(codegen_error)
}

/// `"sha256:<hex>"` of a `jazz.app.json` text, CRLF read as LF: the
/// `source` generated bindings record.
#[napi(js_name = "schemaSourceHash")]
pub fn schema_source_hash(app_json: String) -> String {
    jazz::tools::deployment::artifact::source_hash(app_json.as_bytes())
}

/// The engine's hash of a schema in its own JSON form (`{ "tables": … }`),
/// policies dropped as a deployment drops them, as 64 hex characters. Not
/// [`schema_source_hash`], which hashes the `jazz.app.json` file.
#[napi(js_name = "schemaHash")]
pub fn schema_hash(engine_schema_json: String) -> js::Result<String> {
    jazz::tools::deployment::artifact::schema_hash(engine_schema_json.as_bytes())
        .map(|hash| hash.to_string())
        .map_err(|error| napi::Error::from_reason(error.message().to_owned()).into())
}

/// Whether rows pass unchanged between two schemas in the engine's JSON form
/// (`{ "tables": … }`), so a deployment connects them without a migration:
/// the rule `migrations create` follows to decide whether to write one.
/// Indexes, history retention, merge strategies, column order and defaults
/// of columns outside `branchBy` don't count; everything else does.
#[napi(js_name = "schemasAreCompatible")]
pub fn schemas_are_compatible(left_json: String, right_json: String) -> js::Result<bool> {
    jazz::tools::deployment::artifact::schemas_are_compatible(
        left_json.as_bytes(),
        right_json.as_bytes(),
    )
    .map_err(|error| napi::Error::from_reason(error.message().to_owned()).into())
}

/// What the migration between two schemas in the engine's JSON form
/// (`{ "tables": … }`) does, as `migrations create` decides it, as JSON:
/// `{ createTables, dropTables, renameTables: [{ from, to }], migrate:
/// [{ table, steps, addedVariants, unchangedRows }] }`, each step
/// `{ step: "add" | "drop" | "maybeRenamed" | "maybeRenamedAway", column, … }`.
/// Throws, with the reason, for a change no migration step does.
#[napi(js_name = "migrationDiff")]
pub fn migration_diff(from_json: String, to_json: String) -> js::Result<String> {
    jazz_codegen::migrations::diff_steps_json(from_json.as_bytes(), to_json.as_bytes())
        .map_err(codegen_error)
}

/// A schema in the engine's JSON form (`{ "tables": … }`) as the readable
/// `tables` of a `jazz.app.json` `history` version (no relations, computed
/// fields or transforms), as JSON text.
#[napi(js_name = "versionTables")]
pub fn version_tables(engine_schema_json: String) -> js::Result<String> {
    jazz::tools::deployment::artifact::version_tables_json(engine_schema_json.as_bytes())
        .map_err(|error| napi::Error::from_reason(error.message().to_owned()).into())
}

/// What `migrationsCreate` wrote.
#[napi(object)]
pub struct CreatedMigration {
    /// The new `jazz.app.json` text.
    pub app: String,
    /// Whether a version and migration were added.
    pub changed: bool,
    /// JSON paths of the `"TODO: …"` strings left for the author.
    pub todos: Vec<String>,
    /// Why nothing was written, when it isn't simply that nothing changed
    /// (an index-only edit).
    pub note: Option<String>,
}

/// Write the migration from the version the committed bindings
/// (`bindings`, `jazz.generated.ts`'s text, or null when there are none) were
/// generated from to `appJson`'s edited `tables`, named `name`.
#[napi(js_name = "migrationsCreate")]
pub fn migrations_create(
    app_json: String,
    bindings: Option<String>,
    name: String,
) -> js::Result<CreatedMigration> {
    let created = jazz_codegen::migrations::create(
        jazz_codegen::Lang::TypeScript,
        app_json.as_bytes(),
        bindings.as_deref(),
        &name,
    )
    .map_err(codegen_error)?;
    Ok(CreatedMigration {
        app: created.app,
        changed: created.changed,
        todos: created.todos,
        note: created.note,
    })
}

/// Check committed bindings (`bindings`, or null) against `appJson`. Throws
/// with `error.code` `JAZZ_SCHEMA_CHANGED` when `tables` changed since the
/// bindings were generated and no migration leads from that version (run
/// `migrations create` before regenerating: the bindings hold the only
/// copy of that version), or `JAZZ_BINDINGS_STALE` when regenerating is
/// enough (including when there are none).
#[napi(js_name = "validateBindings")]
pub fn validate_bindings(app_json: String, bindings: Option<String>) -> js::Result<()> {
    use jazz_codegen::bindings::Staleness;
    let app = jazz_codegen::App::load(app_json.as_bytes()).map_err(artifact_error)?;
    match jazz_codegen::bindings::staleness(
        &app,
        jazz_codegen::Lang::TypeScript,
        bindings.as_deref(),
    )
    .map_err(codegen_error)?
    {
        None => Ok(()),
        Some(staleness) => {
            let code = match staleness {
                Staleness::SchemaChanged => "JAZZ_SCHEMA_CHANGED",
                Staleness::Missing | Staleness::OutOfDate | Staleness::Unreadable(_) => {
                    "JAZZ_BINDINGS_STALE"
                }
            };
            Err(BindingError(napi::Error::new(
                JsErrorCode(code.to_owned()),
                staleness.message(jazz_codegen::Lang::TypeScript),
            )))
        }
    }
}

/// What `generateForDir` / `migrationsCreateForDir` did in an app's directory.
#[napi(object)]
pub struct DirOutcome {
    /// Lines to print.
    pub notes: Vec<String>,
    /// Warnings to print.
    pub warnings: Vec<String>,
    /// Bindings files written, relative to the directory.
    pub written: Vec<String>,
    /// Bindings files already up to date, relative to the directory.
    pub current: Vec<String>,
    /// `migrationsCreateForDir`: whether `jazz.app.json` changed.
    pub changed: bool,
    /// `migrationsCreateForDir`: JSON paths of the TODOs left to resolve.
    pub todos: Vec<String>,
}

/// Options of [`generate_for_dir`] and [`migrations_create_for_dir`].
#[napi(object)]
pub struct DirOptions {
    /// Fail when the files or bindings are stale, and write nothing.
    pub check: Option<bool>,
    /// The bindings' languages (`"ts"`, `"rust"`, …); by default each the
    /// directory's files say the app is written in.
    pub lang: Option<Vec<String>>,
    /// The language assumed when nothing in the directory names one and
    /// no other language's manifest is there (the npm CLI passes `"ts"`).
    pub fallback: Option<String>,
}

fn dir_languages(
    dir: &std::path::Path,
    options: &Option<DirOptions>,
) -> js::Result<Vec<jazz_codegen::project::Selected>> {
    let given = options
        .as_ref()
        .and_then(|o| o.lang.clone())
        .unwrap_or_default()
        .iter()
        .map(|name| jazz_codegen::Lang::parse(name))
        .collect::<std::result::Result<Vec<_>, jazz_codegen::CodegenError>>()
        .map_err(codegen_error)?;
    let fallback = options
        .as_ref()
        .and_then(|o| o.fallback.as_deref())
        .map(jazz_codegen::Lang::parse)
        .transpose()
        .map_err(codegen_error)?;
    jazz_codegen::project::languages_or(dir, &given, fallback)
        .map_err(|error| napi::Error::from_reason(error).into())
}

fn dir_outcome(outcome: jazz_codegen::project::Outcome) -> DirOutcome {
    DirOutcome {
        notes: outcome.notes,
        warnings: outcome.warnings,
        written: outcome.written,
        current: outcome.current,
        changed: outcome.changed,
        todos: outcome.todos,
    }
}

/// `jazz-tools generate [--check]` in `dir`, the directory of the app's
/// `jazz.app.json`: the same languages, checks and bytes as the
/// `jazz-tools` binary, which runs the same code.
#[napi(js_name = "generateForDir")]
pub fn generate_for_dir(dir: String, options: Option<DirOptions>) -> js::Result<DirOutcome> {
    let dir = std::path::Path::new(&dir);
    let langs = dir_languages(dir, &options)?;
    let check = options.as_ref().and_then(|o| o.check).unwrap_or(false);
    jazz_codegen::project::generate(dir, &langs, check)
        .map(dir_outcome)
        .map_err(|error| napi::Error::from_reason(error).into())
}

/// `jazz-tools migrations create --name <name>` in `dir`, as the
/// `jazz-tools` binary runs it.
#[napi(js_name = "migrationsCreateForDir")]
pub fn migrations_create_for_dir(
    dir: String,
    name: String,
    options: Option<DirOptions>,
) -> js::Result<DirOutcome> {
    let dir = std::path::Path::new(&dir);
    let langs = dir_languages(dir, &options)?;
    jazz_codegen::project::migrations_create(dir, &langs, &name)
        .map(dir_outcome)
        .map_err(|error| napi::Error::from_reason(error).into())
}

/// A codegen refusal, coded as [`artifact_error`] codes the loaders'.
fn codegen_error(error: jazz_codegen::CodegenError) -> BindingError {
    use jazz::tools::deployment::artifact;
    if error.is_unsupported() {
        BindingError(napi::Error::new(
            JsErrorCode(artifact::UNSUPPORTED_CODE.to_owned()),
            error.message().to_owned(),
        ))
    } else {
        napi::Error::from_reason(error.message().to_owned()).into()
    }
}

/// An app-file refusal: what the files can't express (yet) throws with
/// `error.code` [`UNSUPPORTED_CODE`](jazz::tools::deployment::artifact::UNSUPPORTED_CODE)
/// (`"JAZZ_APP_FILES_UNSUPPORTED"`); every other refusal keeps the default
/// `GenericFailure`.
fn artifact_error(error: jazz::tools::deployment::artifact::ArtifactError) -> BindingError {
    use jazz::tools::deployment::artifact;
    if error.is_unsupported() {
        BindingError(napi::Error::new(
            JsErrorCode(artifact::UNSUPPORTED_CODE.to_owned()),
            error.to_string(),
        ))
    } else {
        napi::Error::from_reason(error.to_string()).into()
    }
}

/// Exact build/ABI fingerprint for the generated native artifact.
#[napi]
pub fn native_artifact_fingerprint() -> String {
    option_env!("JAZZ_NATIVE_ARTIFACT_FINGERPRINT")
        .unwrap_or("missing-build-fingerprint")
        .to_owned()
}

/// Test-only bridge for executing the Rust-owned v1 binding corpus through the
/// generated NAPI artifact. It intentionally returns the frozen corpus rather
/// than a TypeScript reimplementation of its byte layouts.
#[napi(js_name = "__testBindingCodecGoldenFixture", skip_typescript)]
pub fn test_binding_codec_golden_fixture() -> String {
    jazz::binding_codec::BINDING_CODEC_GOLDEN_FIXTURE.to_owned()
}

/// Test-only: read one pinned wire corpus batch through the generated NAPI
/// artifact, as `reader` (`"server-opening"`, `"server"` or `"client"`)
/// would, with the production frame and sync message decoders. Not a host
/// wire API.
#[napi(js_name = "__testValidateWireFrameBatchCorpus", skip_typescript)]
pub fn test_validate_wire_frame_batch_corpus(
    batch: Buffer,
    reader: String,
    negotiated_features: String,
) -> Result<()> {
    let negotiated_features = negotiated_features
        .parse()
        .map_err(|_| napi::Error::from_reason("test wire corpus features must be a u64 decimal"))?;
    let reader = corpus_reader(&reader).map_err(napi::Error::from_reason)?;
    jazz::db::validate_frame_batch_for_corpus(&batch, reader, negotiated_features)
        .map_err(napi::Error::from_reason)
}

fn corpus_reader(name: &str) -> std::result::Result<jazz::db::CorpusReader, String> {
    match name {
        "server-opening" => Ok(jazz::db::CorpusReader::ServerOpening),
        "server" => Ok(jazz::db::CorpusReader::Server),
        "client" => Ok(jazz::db::CorpusReader::Client),
        other => Err(format!("unknown wire corpus reader {other:?}")),
    }
}

/// Test-only: every core `Reason` name, so the TypeScript reason list is
/// checked against the engine's rather than copied from it.
#[napi(js_name = "__testCoreErrorReasonNames", skip_typescript)]
pub fn test_core_error_reason_names() -> Vec<String> {
    jazz::db::Reason::ALL
        .into_iter()
        .map(|reason| reason.as_str().to_owned())
        .collect()
}

/// Test-only feature inventory paired with [`test_validate_wire_frame_batch_corpus`].
#[napi(js_name = "__testWireFrameCorpusFeatures", skip_typescript)]
pub fn test_wire_frame_corpus_features() -> String {
    jazz::wire::current_wire_features().to_string()
}

/// One row of the Inspector's `explain`: whether each read-policy clause holds.
#[napi(object)]
pub struct ExplainedReadRow {
    pub id: String,
    pub holds: Vec<bool>,
}

/// The Inspector's `explain`: how many clauses the read policy has, and the rows found.
#[napi(object)]
pub struct ExplainedReadClauses {
    pub clauses: u32,
    pub rows: Vec<ExplainedReadRow>,
}

// ---- options -------------------------------------------------------------------------

#[napi(object)]
pub struct InsertOptions {
    pub row_id: Option<Uint8Array>,
    pub author: Option<Uint8Array>,
    pub attribution: Option<Uint8Array>,
    pub branch: Option<JsonValue>,
    pub updated_at_ms: Option<f64>,
    pub add_creator_as_member: Option<bool>,
}

#[napi(object)]
pub struct UpdateOptions {
    pub author: Option<Uint8Array>,
    pub attribution: Option<Uint8Array>,
    pub head: Option<JsonValue>,
    pub base: Option<JsonValue>,
    pub updated_at_ms: Option<f64>,
}

#[napi(object)]
pub struct UpsertOptions {
    pub author: Option<Uint8Array>,
    pub attribution: Option<Uint8Array>,
    pub head: Option<JsonValue>,
    pub base: Option<JsonValue>,
    pub updated_at_ms: Option<f64>,
}

#[napi(object)]
pub struct DeleteOptions {
    pub author: Option<Uint8Array>,
    pub attribution: Option<Uint8Array>,
    pub head: Option<JsonValue>,
    pub base: Option<JsonValue>,
    pub updated_at_ms: Option<f64>,
}

#[napi(object)]
pub struct RestoreOptions {
    pub author: Option<Uint8Array>,
    pub attribution: Option<Uint8Array>,
    pub branch: Option<JsonValue>,
    pub updated_at_ms: Option<f64>,
}

/// Write options as the JSON object [`binding_host`] reads: byte arrays
/// become arrays of numbers, absent fields stay absent.
#[derive(Default)]
struct JsonOptions(serde_json::Map<String, serde_json::Value>);

impl JsonOptions {
    fn bytes(mut self, name: &str, value: Option<Uint8Array>) -> Self {
        if let Some(value) = value {
            self.0.insert(name.to_owned(), value.to_vec().into());
        }
        self
    }

    fn json(mut self, name: &str, value: Option<JsonValue>) -> Self {
        if let Some(value) = value {
            self.0.insert(name.to_owned(), value);
        }
        self
    }

    fn number(mut self, name: &str, value: Option<f64>) -> Self {
        if let Some(value) = value {
            self.0.insert(name.to_owned(), value.into());
        }
        self
    }

    fn done(self) -> serde_json::Value {
        serde_json::Value::Object(self.0)
    }
}

fn insert_options(options: Option<InsertOptions>) -> serde_json::Value {
    let Some(options) = options else {
        return serde_json::Value::Null;
    };
    JsonOptions::default()
        .bytes("rowId", options.row_id)
        .bytes("author", options.author)
        .bytes("attribution", options.attribution)
        .json("branch", options.branch)
        .number("updatedAtMs", options.updated_at_ms)
        .json(
            "addCreatorAsMember",
            options.add_creator_as_member.map(serde_json::Value::Bool),
        )
        .done()
}

fn update_options(options: Option<UpdateOptions>) -> serde_json::Value {
    let Some(options) = options else {
        return serde_json::Value::Null;
    };
    JsonOptions::default()
        .bytes("author", options.author)
        .bytes("attribution", options.attribution)
        .json("head", options.head)
        .json("base", options.base)
        .number("updatedAtMs", options.updated_at_ms)
        .done()
}

fn delete_options(options: Option<DeleteOptions>) -> serde_json::Value {
    update_options(options.map(|options| UpdateOptions {
        author: options.author,
        attribution: options.attribution,
        head: options.head,
        base: options.base,
        updated_at_ms: options.updated_at_ms,
    }))
}

fn restore_options(options: Option<RestoreOptions>) -> serde_json::Value {
    insert_options(options.map(|options| InsertOptions {
        row_id: None,
        author: options.author,
        attribution: options.attribution,
        branch: options.branch,
        updated_at_ms: options.updated_at_ms,
        add_creator_as_member: None,
    }))
}

/// Upsert options from the raw JavaScript object. `#[napi(object)]` reads an
/// absent, `undefined` and `null` field alike, but the removed `branch` field
/// must be rejected by its presence, so an untyped caller cannot silently
/// write to the root. It is checked with `has`, never read.
fn upsert_options(options: Option<Unknown<'_>>) -> napi::Result<serde_json::Value> {
    let Some(options) = options else {
        return Ok(serde_json::Value::Null);
    };
    match options.get_type()? {
        ValueType::Undefined | ValueType::Null => return Ok(serde_json::Value::Null),
        ValueType::Object => {}
        _ => return Err(napi::Error::from_reason("upsert options must be an object")),
    }
    let object = Object::from_raw(options.value().env, options.value().value);
    let mut json = JsonOptions::default()
        .bytes("author", object.get_named_property_unchecked("author")?)
        .bytes(
            "attribution",
            object.get_named_property_unchecked("attribution")?,
        )
        .json("head", object.get_named_property_unchecked("head")?)
        .json("base", object.get_named_property_unchecked("base")?)
        .number(
            "updatedAtMs",
            object.get_named_property_unchecked("updatedAtMs")?,
        );
    if object.has_named_property("branch")? {
        json.0.insert("branch".to_owned(), serde_json::Value::Null);
    }
    Ok(json.done())
}

fn large_value_updates(mutations: JsonValue) -> napi::Result<Vec<LargeValueUpdate>> {
    serde_json::from_value(mutations).map_err(|error| {
        napi::Error::from_reason(format!("invalid partial-value update descriptor: {error}"))
    })
}

/// A structured option (a branch selector or base), `None` when absent.
fn structured<T: serde::de::DeserializeOwned>(
    value: Option<JsonValue>,
    name: &str,
) -> napi::Result<Option<T>> {
    value
        .filter(|value| !value.is_null())
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| napi::Error::from_reason(format!("invalid {name}: {error}")))
}

fn json_or_null(value: Option<JsonValue>) -> serde_json::Value {
    value.unwrap_or(serde_json::Value::Null)
}

// ---- errors --------------------------------------------------------------------------

/// Carries the core code through napi-rs until the exported method throws it.
const CORE_ERROR_CODE_CARRIER: &str = "\u{0}jazz-core-error-code:";

fn napi_core_error(error: &CoreError) -> napi::Error {
    let mut napi_error = napi::Error::from_reason(error.to_string());
    napi_error.set_cause(napi::Error::from_reason(format!(
        "{CORE_ERROR_CODE_CARRIER}{}",
        error.code.as_str()
    )));
    napi_error
}

fn carried_code(error: &napi::Error) -> Option<&str> {
    error
        .cause
        .as_ref()?
        .reason
        .strip_prefix(CORE_ERROR_CODE_CARRIER)
}

/// The error a shared-layer failure throws: an engine error keeps its stable
/// code, an argument error is a plain `GenericFailure`.
fn host_error(error: HostError) -> napi::Error {
    match error {
        HostError::Engine(error) => napi_core_error(&error),
        HostError::Argument(message) => napi::Error::from_reason(message),
    }
}

/// napi-rs normally constructs a synchronous exception from just its code and
/// message. Throw the completed JS object when a write needs its space field.
fn sync_host_error(env: &Env, error: HostError) -> napi::Error {
    if matches!(&error, HostError::Engine(core) if core.space.is_some()) {
        let value = js_error_value(env.raw(), error);
        if unsafe { sys::napi_throw(env.raw(), value) } == sys::Status::napi_ok {
            return napi::Error::from_status(napi::Status::PendingException);
        }
        return napi::Error::from_reason("could not throw the encryption refusal");
    }
    host_error(error)
}

fn not_supported(what: &str) -> napi::Error {
    host_error(binding_host::not_supported(what))
}

/// The JavaScript `error.code` of an error thrown by an exported method.
///
/// napi-rs throws an `Error<S>` with `code` set to `S::as_ref()`. A core error
/// uses its stable core code (for example `not_observed`); every other error
/// keeps the napi status name napi-rs has always used (`GenericFailure`).
pub struct JsErrorCode(String);

impl AsRef<str> for JsErrorCode {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// The error type of exported methods: a `napi::Error` whose JavaScript
/// `code` is the core error code when there is one.
pub struct BindingError(napi::Error<JsErrorCode>);

impl From<napi::Error> for BindingError {
    fn from(mut error: napi::Error) -> Self {
        // A thrown error carries only the code: napi-rs creates it from a
        // code and a message, with no way to add properties.
        let carried = carried_code(&error).map(str::to_owned);
        let (code, cause) = match carried {
            Some(code) => (code, None),
            None => (error.status.as_ref().to_owned(), error.cause.take()),
        };
        let mut coded = napi::Error::new(JsErrorCode(code), std::mem::take(&mut error.reason));
        coded.cause = cause;
        Self(coded)
    }
}

impl From<BindingError> for JsError<JsErrorCode> {
    fn from(error: BindingError) -> Self {
        JsError::from(error.0)
    }
}

impl std::ops::Deref for BindingError {
    type Target = napi::Error<JsErrorCode>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Debug for BindingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// `js::Result` is the return type of exported methods that can throw a core
/// error. napi-derive recognises a return type whose last path segment is
/// `Result`, so the alias keeps that name.
mod js {
    pub type Result<T> = std::result::Result<T, super::BindingError>;
}

fn closed() -> napi::Error {
    napi::Error::from_reason("database is closed")
}

// ---- promises --------------------------------------------------------------------------

thread_local! {
    static PENDING_PROMISES: jazz_e2ee_client::host::PromiseQueue = Default::default();
}

fn promise_for<T, R: 'static>(
    env: &Env,
    future: impl std::future::Future<Output = R> + 'static,
    settle: impl FnOnce(sys::napi_env, sys::napi_deferred, R) + 'static,
    failure: &str,
) -> napi::Result<PromiseRaw<'static, T>> {
    let env = env.raw();
    let mut deferred = std::ptr::null_mut();
    let mut promise = std::ptr::null_mut();
    if unsafe { sys::napi_create_promise(env, &mut deferred, &mut promise) } != sys::Status::napi_ok
    {
        return Err(napi::Error::from_reason(failure.to_owned()));
    }
    PENDING_PROMISES.with(|queue| {
        queue.submit(async move {
            settle(env, deferred, future.await);
        })
    });
    Ok(PromiseRaw::new(env, promise))
}

fn host_promise(env: &Env, future: HostFuture<()>) -> napi::Result<PromiseRaw<'static, ()>> {
    promise_for(
        env,
        future,
        |env, deferred, result| match result {
            Ok(()) => {
                let mut undefined = std::ptr::null_mut();
                if unsafe { sys::napi_get_undefined(env, &mut undefined) } == sys::Status::napi_ok {
                    unsafe { sys::napi_resolve_deferred(env, deferred, undefined) };
                }
            }
            Err(error) => {
                let rejection = js_error_value(env, error);
                unsafe { sys::napi_reject_deferred(env, deferred, rejection) };
            }
        },
        "failed to create a write wait promise",
    )
}

/// A JavaScript `Error` with `error`'s message, code and, for a core error
/// that has one, its reason name as `error.reason` (see [`BindingError`]).
fn js_error_value(env: sys::napi_env, error: HostError) -> sys::napi_value {
    let (code, reason, space, detail) = match &error {
        HostError::Engine(core) => (
            core.code.as_str(),
            core.reason,
            core.space.as_ref(),
            core.to_string(),
        ),
        HostError::Argument(message) => ("GenericFailure", None, None, message.clone()),
    };
    let string = |text: &str| {
        let mut value = std::ptr::null_mut();
        unsafe {
            sys::napi_create_string_utf8(env, text.as_ptr().cast(), text.len() as isize, &mut value)
        };
        value
    };
    let message = string(&detail);
    let mut js_error = std::ptr::null_mut();
    // `napi_create_error` sets the JavaScript `error.code` from its code.
    let status = unsafe { sys::napi_create_error(env, string(code), message, &mut js_error) };
    if status != sys::Status::napi_ok {
        return message;
    }
    if let Some(reason) = reason {
        // Setting a data property on a fresh error object cannot fail.
        unsafe {
            sys::napi_set_named_property(env, js_error, c"reason".as_ptr(), string(reason.as_str()))
        };
    }
    if let Some(space) = space {
        let mut address = std::ptr::null_mut();
        unsafe {
            if sys::napi_create_object(env, &mut address) == sys::Status::napi_ok {
                sys::napi_set_named_property(
                    env,
                    address,
                    c"scopeId".as_ptr(),
                    string(&space.scope_id),
                );
                sys::napi_set_named_property(
                    env,
                    address,
                    c"identifier".as_ptr(),
                    string(&space.identifier),
                );
                sys::napi_set_named_property(env, js_error, c"space".as_ptr(), address);
            }
        }
    }
    js_error
}

/// Settle every promise whose wait completed. Waits complete only when a
/// tick, a local write or a claims change made progress, so every call that
/// can do so ends here.
fn settle_promises() {
    PENDING_PROMISES.with(|queue| queue.settle());
}

/// The encryption hooks an open installs: a client's, over its runtime, or a
/// backend's keyless ones.
enum Hooks<'a> {
    Client(&'a EncryptionRuntime),
    Keyless(Rc<dyn jazz::e2ee::E2eeHooks>),
}

/// A Jazz self-signed client identity for an encrypted open, as for the
/// plain one.
#[napi(object)]
pub struct EncryptionProof {
    pub token: String,
    pub claimed_author: String,
    /// The session proof names its app. A device open always uses its runtime's app.
    pub app_id: Option<String>,
}

/// The end-to-end encryption runtime of an app, built over the private store
/// the app supplies, before the database that uses it opens. The store is
/// JavaScript: its requests are taken with `takeStoreRequests` and answered
/// with `replyStore` (see `js_store`).
#[napi]
pub struct EncryptionRuntime {
    builder: encryption::RuntimeBuilder,
}

#[napi]
impl EncryptionRuntime {
    /// A runtime for the app whose generated bindings embed the compiled app
    /// `bindings` (JSON of `{ schema, relations }`, the options they pass to
    /// `bindApp`): what a deployed app has in place of `jazz.app.json`.
    #[napi(factory, js_name = "fromBindings")]
    pub fn from_bindings(
        bindings: String,
        app_id: String,
        environment: String,
        account_id: String,
        device_account_secret: Option<String>,
    ) -> napi::Result<Self> {
        Ok(Self {
            builder: encryption::RuntimeBuilder::from_bindings(
                bindings,
                app_id,
                environment,
                account_id,
                device_account_secret,
            )
            .map_err(napi::Error::from_reason)?,
        })
    }

    #[napi(js_name = "setAddCreatorAsMember")]
    pub fn set_add_creator_as_member(&self, enabled: bool) {
        self.builder.add_creator_as_member.set(enabled);
    }

    /// Builds the runtime: settles when its device is loaded or made, which
    /// takes the app's store (answer its requests while waiting).
    #[napi(ts_return_type = "Promise<string>")]
    pub fn ready(&self, env: &Env) -> js::Result<PromiseRaw<'static, String>> {
        let future = self
            .builder
            .start_build()
            .map_err(napi::Error::from_reason)?;
        Ok(json_promise(env, future)?)
    }

    /// Subscribe to new store requests and deadlines while serving the store.
    #[napi(js_name = "setStoreWake")]
    pub fn set_store_wake(&self, callback: Option<Function<'_, String, ()>>) -> napi::Result<()> {
        let callback = callback
            .map(|callback| {
                callback
                    .build_threadsafe_function::<String>()
                    .callee_handled::<false>()
                    .weak::<true>()
                    .build()
            })
            .transpose()?;
        self.builder.set_store_wake(callback.map(|callback| {
            Box::new(move |wake: jazz_e2ee_client::host::bridge::StoreWake| {
                let _ = callback.call(wake.urgency(), ThreadsafeFunctionCallMode::NonBlocking);
            }) as Box<dyn Fn(_)>
        }));
        Ok(())
    }

    /// The store requests queued since the last call, each a JSON text.
    #[napi(js_name = "takeStoreRequests")]
    pub fn take_store_requests(&self) -> Vec<String> {
        let requests = self.builder.take_requests();
        self.builder.drive(settle_promises);
        requests
    }

    /// The app's answer to a store request; `false` when no such request is
    /// outstanding (the answer is dropped).
    #[napi(js_name = "replyStore")]
    pub fn reply_store(&self, id: String, ok: bool, payload: String) -> bool {
        let accepted = self.builder.reply(&id, ok, &payload);
        self.builder.drive(settle_promises);
        accepted
    }
}

fn json_promise(
    env: &Env,
    future: encryption::JsonFuture,
) -> napi::Result<PromiseRaw<'static, String>> {
    promise_for(
        env,
        future,
        |env, deferred, result| {
            let string = |text: &str| {
                let mut value = std::ptr::null_mut();
                unsafe {
                    sys::napi_create_string_utf8(
                        env,
                        text.as_ptr().cast(),
                        text.len() as isize,
                        &mut value,
                    )
                };
                value
            };
            match result {
                Ok(value) => unsafe {
                    sys::napi_resolve_deferred(env, deferred, string(&value.to_string()));
                },
                Err(message) => {
                    let mut error = std::ptr::null_mut();
                    unsafe {
                        sys::napi_create_error(
                            env,
                            std::ptr::null_mut(),
                            string(&message),
                            &mut error,
                        )
                    };
                    unsafe { sys::napi_reject_deferred(env, deferred, error) };
                }
            }
        },
        "failed to create a promise",
    )
}

// ---- host hooks ------------------------------------------------------------------

/// Node's half of the hooks: the tick and mutation-error callbacks.
#[derive(Default)]
struct Host {
    /// Shared by the owner and its schema views until the owner closes.
    encryption: RefCell<Option<Rc<encryption::Encryption>>>,
    tick_scheduler: RefCell<Option<ThreadsafeFunction<String, ()>>>,
    /// A tick is requested and not yet delivered: further requests coalesce.
    tick_requested: Arc<AtomicBool>,
    /// Told of a write rejected at apply time that no wait claimed.
    mutation_errors: RefCell<Option<FunctionRef<JsonValue, ()>>>,
    /// The same callback, told of an upload the authority refused that no
    /// wait claimed (it arrives during a tick, outside any JS call).
    upstream_mutation_errors: RefCell<Option<MutationErrorCallback>>,
}

type MutationErrorCallback =
    ThreadsafeFunction<serde_json::Value, (), serde_json::Value, napi::Status, false, true>;

impl Host {
    fn request_tick(&self) {
        let scheduler = self.tick_scheduler.borrow();
        let Some(scheduler) = scheduler.as_ref() else {
            return;
        };
        if self.tick_requested.swap(true, Ordering::AcqRel) {
            return;
        }
        let status = scheduler.call(
            Ok("immediate".to_owned()),
            ThreadsafeFunctionCallMode::NonBlocking,
        );
        if status != napi::Status::Ok {
            self.tick_requested.store(false, Ordering::Release);
        }
    }
}

/// A bound database (or one of its schema views) and the Node hooks.
#[derive(Clone)]
struct Inner {
    binding: Binding,
    host: Rc<Host>,
    /// The thin session's transactions this database serves, when it is a
    /// backend's client for one (`serveThinTransaction`): taken while a
    /// request is served, so requests run one at a time.
    thin: Rc<RefCell<Option<ThinState>>>,
}

/// A thin session's transactions, made on the first request.
enum ThinState {
    Idle(jazz::thin_transactions::ThinTransactions),
    Serving,
}

/// The session's transactions while a request is served: back in their cell
/// when it ends, or when the caller drops it (a request it stopped waiting
/// for), so the next request finds them.
struct Serving {
    cell: Rc<RefCell<Option<ThinState>>>,
    transactions: Option<jazz::thin_transactions::ThinTransactions>,
}

impl Drop for Serving {
    fn drop(&mut self) {
        if let Some(transactions) = self.transactions.take() {
            *self.cell.borrow_mut() = Some(ThinState::Idle(transactions));
        }
    }
}

impl Inner {
    fn new(binding: Binding) -> Self {
        let host = Rc::new(Host::default());
        let scheduler = Rc::clone(&host);
        *binding.hooks.schedule_tick.borrow_mut() =
            Some(Box::new(move || scheduler.request_tick()));
        let timer = Rc::clone(&host);
        *binding.hooks.schedule_tick_after.borrow_mut() = Some(Box::new(move |ms| {
            if let Some(scheduler) = timer.tick_scheduler.borrow().as_ref() {
                scheduler.call(
                    Ok(format!("after:{ms}")),
                    ThreadsafeFunctionCallMode::NonBlocking,
                );
            }
        }));
        let reporter = Rc::clone(&host);
        *binding.hooks.rejected.borrow_mut() =
            Some(Box::new(move |tx_id, error, serializable, _| {
                let callbacks = reporter.upstream_mutation_errors.borrow();
                let Some(callback) = callbacks.as_ref() else {
                    return false;
                };
                let event = mutation_error_event(&tx_id.to_string(), error, serializable, true);
                // A call the queue dropped did not reach JS: keep the
                // refusal for the next delivery.
                callback.call(event, ThreadsafeFunctionCallMode::NonBlocking) == napi::Status::Ok
            }));
        Self {
            binding,
            host,
            thin: Rc::default(),
        }
    }

    /// A write's receipt. A rejected write is reported to the
    /// mutation-error callback unless a wait claims it first.
    fn write(&self, env: &Env, write: HostResult<binding_host::Write>) -> napi::Result<Write> {
        let write = write.map_err(|error| sync_host_error(env, error))?;
        settle_promises();
        let claimed = Rc::new(Cell::new(false));
        if let Some(error) = &write.rejected {
            self.report_unclaimed(env, &write, error, Rc::clone(&claimed))?;
        }
        self.binding.hooks.track(&write, &claimed);
        Ok(Write {
            inner: Some(self.clone()),
            write,
            claimed,
        })
    }

    /// Tell the mutation-error callback of a rejected write unless a wait
    /// claims it first: the caller gets until the current task ends (its
    /// promise chain included) to call `wait`.
    fn report_unclaimed(
        &self,
        env: &Env,
        write: &binding_host::Write,
        error: &CoreError,
        claimed: Rc<Cell<bool>>,
    ) -> napi::Result<()> {
        let host = Rc::clone(&self.host);
        let event =
            mutation_error_event(&write.tx_id.to_string(), error, write.serializable, false);
        let report = env.create_function_from_closure("reportMutationError", move |context| {
            if claimed.get() {
                return Ok(());
            }
            if let Some(callback) = host.mutation_errors.borrow().as_ref() {
                callback.borrow_back(context.env)?.call(event.clone())?;
            }
            Ok(())
        })?;
        let set_timeout: SetTimeout = env
            .get_global()?
            .get_named_property_unchecked("setTimeout")?;
        set_timeout.call(FnArgs {
            data: (report, 0.0),
        })?;
        Ok(())
    }
}

/// `globalThis.setTimeout(handler, delay)`.
type SetTimeout<'a> = Function<'a, FnArgs<(Function<'a, (), ()>, f64)>, Unknown<'a>>;

/// A `MutationErrorEvent` for transaction `tx` rejected with `error` (by
/// the authority, if `upstream`).
fn mutation_error_event(
    tx: &str,
    error: &CoreError,
    serializable: bool,
    upstream: bool,
) -> serde_json::Value {
    let (code, reason) = binding_host::mutation_error_parts(error, upstream);
    serde_json::json!({
        "code": code,
        "reason": reason,
        "transaction": {
            "transactionId": tx,
            "kind": binding_host::transaction_kind(serializable),
            "sealed": true,
            "latestSettlement": {
                "kind": "rejected",
                "transactionId": tx,
                "code": code,
                "reason": reason,
            },
        },
    })
}

// ---- pending operations ------------------------------------------------------------

type ReadFuture = Pin<Box<dyn Future<Output = HostResult<Vec<u8>>>>>;

/// The shared upstream link machine (`jazz::link`), which decides when the
/// runtime's WebSocket connects, backs off and reports an outage. The runtime
/// owns the socket and the timer and steps the machine with JSON documents.
#[napi(js_name = "UpstreamLink")]
pub struct NapiUpstreamLink {
    link: RefCell<jazz::link::host::HostLink>,
}

#[napi]
impl NapiUpstreamLink {
    /// `seed` drives the retry jitter.
    #[napi(constructor)]
    pub fn new(seed: f64) -> Self {
        Self {
            link: RefCell::new(jazz::link::host::HostLink::new(seed as u64)),
        }
    }

    /// Apply one input observed at `now_ms`; returns the actions, events and
    /// status as JSON.
    #[napi]
    pub fn handle(&self, now_ms: f64, input: String) -> napi::Result<String> {
        self.link
            .borrow_mut()
            .handle_json(now_ms, &input)
            .map_err(napi::Error::from_reason)
    }
}

/// A JavaScript-thread-owned read waiting for the server. NAPI promises
/// execute on a Send worker pool, whereas a Jazz database is deliberately
/// `Rc`/thread-affine. The adapter drives this object after its peer
/// transport makes progress instead of blocking Node.
///
/// A one-shot read that left rows out because the encryption runtime could
/// not open them comes back as one of these even when it is done at once:
/// [`PendingNativeRead::unreadable`] names those rows.
#[napi]
pub struct PendingNativeRead {
    future: RefCell<Option<ReadFuture>>,
    /// The answer of a read that was done when it was returned.
    done: RefCell<Option<Vec<u8>>>,
    /// The rows the read left out, as JSON, once it is done.
    unreadable: Rc<RefCell<Option<String>>>,
}

impl PendingNativeRead {
    fn new(future: ReadFuture) -> Self {
        Self {
            future: RefCell::new(Some(future)),
            done: RefCell::default(),
            unreadable: Rc::default(),
        }
    }

    fn poll_once(&self) -> HostResult<Option<Uint8Array>> {
        if let Some(bytes) = self.done.borrow_mut().take() {
            return Ok(Some(bytes.into()));
        }
        let Some(mut future) = self.future.borrow_mut().take() else {
            return Err(HostError::Argument(
                "native pending read is already complete".to_owned(),
            ));
        };
        let mut context = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result.map(|bytes| Some(bytes.into())),
            Poll::Pending => {
                *self.future.borrow_mut() = Some(future);
                Ok(None)
            }
        }
    }
}

#[napi]
impl PendingNativeRead {
    #[napi]
    pub fn poll(&self, env: &Env) -> js::Result<Option<Uint8Array>> {
        Ok(self.poll_once().map_err(|err| sync_host_error(env, err))?)
    }

    #[napi]
    pub fn cancel(&self) {
        self.future.borrow_mut().take();
        self.done.borrow_mut().take();
    }

    /// Once the read is done: the rows it left out because they could not
    /// be opened, as a JSON array of `{ table, id, column, code }`; `null`
    /// when it left none out.
    #[napi]
    pub fn unreadable(&self) -> Option<String> {
        self.unreadable.borrow().clone()
    }
}

fn read_or_pending(future: ReadFuture) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
    let pending = PendingNativeRead::new(future);
    Ok(match pending.poll_once().map_err(host_error)? {
        Some(bytes) => Either::A(bytes),
        None => Either::B(pending),
    })
}

/// [`read_or_pending`] for a read that reports the rows it left out: such a
/// read is answered as a [`PendingNativeRead`] even when done at once, so the
/// rows reach JS with it.
fn reporting_read_or_pending(
    env: &Env,
    future: HostFuture<(Vec<u8>, Vec<jazz::db::UnreadableRow>)>,
) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
    let unreadable = Rc::<RefCell<Option<String>>>::default();
    let future = binding_host::reporting_read(future, Rc::clone(&unreadable));
    let pending = PendingNativeRead {
        unreadable,
        ..PendingNativeRead::new(future)
    };
    match pending
        .poll_once()
        .map_err(|err| sync_host_error(env, err))?
    {
        Some(bytes) if pending.unreadable.borrow().is_none() => Ok(Either::A(bytes)),
        Some(bytes) => {
            *pending.done.borrow_mut() = Some(bytes.to_vec());
            Ok(Either::B(pending))
        }
        None => Ok(Either::B(pending)),
    }
}

/// A JavaScript-thread-owned permission preflight waiting for the upstream
/// authority's answer. Like pending reads, it stays on the owning JavaScript
/// thread.
#[napi]
pub struct PendingNativePermissionAdvice {
    future: RefCell<Option<HostFuture<&'static str>>>,
    binding: Binding,
    id: u64,
}

impl PendingNativePermissionAdvice {
    fn poll_once(&self) -> napi::Result<Option<String>> {
        let Some(mut future) = self.future.borrow_mut().take() else {
            return Err(napi::Error::from_reason(
                "native pending permission advice is already complete",
            ));
        };
        let mut context = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result
                .map(|advice| Some(advice.to_owned()))
                .map_err(host_error),
            Poll::Pending => {
                *self.future.borrow_mut() = Some(future);
                Ok(None)
            }
        }
    }
}

#[napi]
impl PendingNativePermissionAdvice {
    #[napi]
    pub fn poll(&self) -> js::Result<Option<String>> {
        Ok(self.poll_once()?)
    }

    /// Stop waiting; a late answer is ignored.
    #[napi]
    pub fn cancel(&self) {
        if self.future.borrow_mut().take().is_some() {
            self.binding.cancel_permission_advice(self.id);
        }
    }
}

// ---- writes ------------------------------------------------------------------------

#[napi(js_name = "Write")]
pub struct Write {
    write: binding_host::Write,
    inner: Option<Inner>,
    /// Whether a wait took over reporting the write's rejection.
    claimed: Rc<Cell<bool>>,
}

#[napi]
impl Write {
    #[napi(getter, js_name = "txId")]
    pub fn tx_id(&self) -> String {
        self.write.tx_id.to_string()
    }

    #[napi(getter)]
    pub fn payload(&self) -> Uint8Array {
        self.write.payload.clone().into()
    }

    #[napi(getter, js_name = "rowId")]
    pub fn row_id(&self) -> Uint8Array {
        self.write.row_id.to_bytes().to_vec().into()
    }

    #[napi(js_name = "writeState")]
    pub fn write_state(&self) -> js::Result<JsonValue> {
        Err(not_supported("write state snapshots").into())
    }

    /// Resolve once the write reached `tier`: at once for `none` and
    /// `local` (the commit is in the store when the write call returns), on
    /// the server's acceptance for `global`, rejecting if it refused.
    #[napi]
    pub fn wait(
        &self,
        env: Env,
        tier: String,
        observe_only: Option<bool>,
    ) -> js::Result<PromiseRaw<'static, ()>> {
        let Some(inner) = &self.inner else {
            return Err(napi::Error::from_reason("write state is unavailable").into());
        };
        let fate = inner
            .binding
            .wait_for_write(
                &self.write,
                &tier,
                (!observe_only.unwrap_or(false)).then(|| Rc::clone(&self.claimed)),
            )
            .map_err(host_error)?;

        Ok(host_promise(&env, fate)?)
    }

    #[napi]
    pub fn close(&mut self) -> bool {
        self.inner.take().is_some()
    }
}

/// A large value uploaded in chunks, then published as one row mutation.
#[napi(js_name = "StreamingMutation")]
pub struct StreamingMutation {
    inner: Inner,
    mutation: binding_host::StreamingMutation,
}

#[napi]
impl StreamingMutation {
    #[napi]
    pub fn push(&mut self, chunk: Uint8Array) -> js::Result<()> {
        Ok(self.mutation.push(&chunk).map_err(host_error)?)
    }

    #[napi]
    pub fn finish(&mut self, env: Env) -> js::Result<Write> {
        let write = self.mutation.finish();
        Ok(self.inner.write(&env, write)?)
    }

    /// Stage the value's row mutation in the transaction it began in.
    #[napi(js_name = "finishInTransaction")]
    pub fn finish_in_transaction(&mut self) -> js::Result<()> {
        Ok(self.mutation.finish_in_transaction().map_err(host_error)?)
    }

    #[napi]
    pub fn abort(&mut self) -> js::Result<bool> {
        Ok(self.mutation.abort().map_err(host_error)?)
    }
}

// ---- subscriptions ----------------------------------------------------------------

#[napi(object)]
pub struct SubscriptionDeltaEvent {
    #[napi(js_name = "type", ts_type = "'delta'")]
    pub event_type: String,
    pub reset: bool,
    /// A reset standing for deltas the reader fell too far behind to be
    /// told one by one (its backlog passed its limit).
    pub coalesced: bool,
    pub delta: Uint8Array,
    /// Always empty: SQLite subscriptions deliver whole-row deltas.
    #[napi(js_name = "terminalOperations", ts_type = "Array<never>")]
    pub terminal_operations: Vec<JsonValue>,
    pub settled: bool,
    #[napi(ts_type = "'None' | 'Local' | 'Global'")]
    pub tier: String,
    /// The rows the result leaves out because the encryption runtime could
    /// not open them, as a JSON array of `{ table, id, column, code }`.
    /// Set on an opening, a reset, and a delta that changed the list, which
    /// it replaces; absent when the list is unchanged.
    pub unreadable: Option<String>,
}

#[napi(object)]
pub struct SubscriptionRejectedEvent {
    #[napi(js_name = "type", ts_type = "'rejected'")]
    pub event_type: String,
    pub reason: SubscriptionServerFailureReason,
}

#[napi(object)]
pub struct SubscriptionClosedEvent {
    #[napi(js_name = "type", ts_type = "'closed'")]
    pub event_type: String,
}

/// Why the server refused a subscription.
#[napi(object)]
pub struct SubscriptionServerFailureReason {
    #[napi(js_name = "type", ts_type = "'ServerFailure'")]
    pub reason_type: String,
    /// The server's reason.
    pub code: String,
    /// The rejection's core error code (`"authority_changed"`, …), as a
    /// thrown core error's `code`.
    #[napi(js_name = "errorCode")]
    pub error_code: String,
}

#[napi]
pub type SubscriptionEvent =
    Either3<SubscriptionDeltaEvent, SubscriptionRejectedEvent, SubscriptionClosedEvent>;

#[napi(js_name = "Subscription")]
pub struct Subscription {
    stream: SubscriptionSlot,
}

/// Where a subscription's stream lives: read through this object, or moved
/// into a [`SubscriptionPool`] that drains every pooled stream in one call.
enum SubscriptionSlot {
    Own(Option<Box<SubscriptionStream>>),
    Pooled {
        pool: Rc<RefCell<PoolStreams>>,
        id: u32,
    },
}

fn subscription_event(event: CoreEvent) -> napi::Result<SubscriptionEvent> {
    Ok(
        match binding_host::event_chunk(event).map_err(host_error)? {
            EventChunk::Delta {
                delta,
                reset,
                coalesced,
                settled,
                tier,
                unreadable,
            } => Either3::A(SubscriptionDeltaEvent {
                event_type: "delta".to_owned(),
                reset,
                coalesced,
                delta: delta.into(),
                terminal_operations: Vec::new(),
                settled,
                tier,
                unreadable,
            }),
            EventChunk::Rejected { detail, code } => Either3::B(SubscriptionRejectedEvent {
                event_type: "rejected".to_owned(),
                reason: SubscriptionServerFailureReason {
                    reason_type: "ServerFailure".to_owned(),
                    code: detail,
                    error_code: code.to_owned(),
                },
            }),
            EventChunk::Closed => Either3::C(SubscriptionClosedEvent {
                event_type: "closed".to_owned(),
            }),
        },
    )
}

/// Something events are taken from without waiting: a live stream, or a
/// scripted source in tests.
trait EventSource {
    fn try_next(&mut self) -> Option<CoreEvent>;
}

impl EventSource for SubscriptionStream {
    fn try_next(&mut self) -> Option<CoreEvent> {
        self.try_next_event()
    }
}

/// What one read of one stream produced.
struct StreamRead<T> {
    events: Vec<T>,
    /// The stream delivered `Closed`: it has nothing more to say.
    closed: bool,
    /// Encoding an event failed. The events before it are kept; the stream
    /// is not read further.
    error: Option<napi::Error>,
}

/// Take every event `source` has now, encoding each.
fn read_stream<S: EventSource, T>(
    source: &mut S,
    encode: &mut impl FnMut(CoreEvent) -> napi::Result<T>,
) -> StreamRead<T> {
    let mut read = StreamRead {
        events: Vec::new(),
        closed: false,
        error: None,
    };
    while let Some(event) = source.try_next() {
        let closed = matches!(event, CoreEvent::Closed);
        match encode(event) {
            Ok(event) => read.events.push(event),
            Err(error) => {
                read.error = Some(error);
                return read;
            }
        }
        if closed {
            read.closed = true;
            break;
        }
    }
    read
}

/// Read every stream in `streams` that has something, in id order, and
/// forget the ones that closed or failed. One stream's encode error fails
/// only that stream's entry.
fn drain_streams<S: EventSource, T>(
    streams: &mut BTreeMap<u32, S>,
    mut encode: impl FnMut(CoreEvent) -> napi::Result<T>,
) -> Vec<(u32, StreamRead<T>)> {
    let mut out = Vec::new();
    streams.retain(|id, stream| {
        let read = read_stream(stream, &mut encode);
        let keep = !read.closed && read.error.is_none();
        if !read.events.is_empty() || read.error.is_some() {
            out.push((*id, read));
        }
        keep
    });
    out
}

#[napi]
impl Subscription {
    /// Every event available now; empty until the next tick or local write
    /// has something to deliver.
    #[napi(js_name = "readAll")]
    pub fn read_all(&mut self) -> js::Result<Vec<SubscriptionEvent>> {
        let read = match &mut self.stream {
            SubscriptionSlot::Own(stream) => {
                let Some(source) = stream.as_mut() else {
                    return Err(napi::Error::from_reason("subscription is closed").into());
                };
                let read = read_stream(source.as_mut(), &mut subscription_event);
                if read.closed {
                    *stream = None;
                }
                read
            }
            SubscriptionSlot::Pooled { pool, id } => {
                let mut pool = pool.borrow_mut();
                let Some(source) = pool.streams.get_mut(id) else {
                    return Err(napi::Error::from_reason("subscription is closed").into());
                };
                let read = read_stream(source, &mut subscription_event);
                if read.closed {
                    pool.streams.remove(id);
                }
                read
            }
        };
        match read.error {
            Some(error) => Err(error.into()),
            None => Ok(read.events),
        }
    }

    #[napi]
    pub fn drain(&mut self) -> js::Result<Vec<SubscriptionEvent>> {
        self.read_all()
    }

    /// Move this subscription's stream into `pool`, which then reads it in
    /// [`SubscriptionPool::drain`]. Returns the id its batches carry.
    #[napi(js_name = "joinPool")]
    pub fn join_pool(&mut self, pool: &SubscriptionPool) -> js::Result<u32> {
        let stream = match &mut self.stream {
            SubscriptionSlot::Pooled { pool: joined, id } if Rc::ptr_eq(joined, &pool.streams) => {
                return Ok(*id);
            }
            SubscriptionSlot::Pooled { .. } => {
                return Err(napi::Error::from_reason("subscription is in another pool").into());
            }
            SubscriptionSlot::Own(stream) => stream
                .take()
                .ok_or_else(|| napi::Error::from_reason("subscription is closed"))?,
        };
        let mut streams = pool.streams.borrow_mut();
        let id = streams.next_id;
        streams.next_id = streams.next_id.wrapping_add(1);
        streams.streams.insert(id, *stream);
        drop(streams);
        self.stream = SubscriptionSlot::Pooled {
            pool: Rc::clone(&pool.streams),
            id,
        };
        Ok(id)
    }

    #[napi]
    pub fn close(&mut self) -> bool {
        match &mut self.stream {
            SubscriptionSlot::Own(stream) => stream.take().is_some(),
            SubscriptionSlot::Pooled { pool, id } => pool.borrow_mut().streams.remove(id).is_some(),
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // A collected subscription ends its pooled stream as an unpooled
        // one ends with it.
        self.close();
    }
}

#[derive(Default)]
struct PoolStreams {
    next_id: u32,
    streams: BTreeMap<u32, SubscriptionStream>,
}

/// One subscription's share of a [`SubscriptionPool::drain`].
#[napi(object)]
pub struct SubscriptionBatch {
    /// The id [`Subscription::join_pool`] returned.
    pub id: u32,
    /// The events available now, oldest first.
    pub events: Vec<SubscriptionEvent>,
    /// Set when the stream failed after `events`; the stream has left the
    /// pool. Other subscriptions in the same drain are unaffected.
    pub error: Option<SubscriptionBatchError>,
}

#[napi(object)]
pub struct SubscriptionBatchError {
    pub message: String,
    /// The stable core error code, or the napi status name.
    pub code: String,
}

impl From<napi::Error> for SubscriptionBatchError {
    fn from(error: napi::Error) -> Self {
        let code = carried_code(&error)
            .map(str::to_owned)
            .unwrap_or_else(|| error.status.as_ref().to_owned());
        Self {
            message: error.reason,
            code,
        }
    }
}

/// Subscriptions read together: one [`drain`](Self::drain) call returns the
/// events of every pooled subscription that has any, instead of one
/// `readAll` call per subscription.
#[napi(js_name = "SubscriptionPool")]
pub struct SubscriptionPool {
    streams: Rc<RefCell<PoolStreams>>,
}

impl Default for SubscriptionPool {
    fn default() -> Self {
        Self::new()
    }
}

#[napi]
impl SubscriptionPool {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            streams: Rc::default(),
        }
    }

    /// The events of every pooled subscription that has any now, by id.
    /// Subscriptions with nothing are left out. A closed or failed stream
    /// leaves the pool after its batch.
    #[napi]
    pub fn drain(&self) -> Vec<SubscriptionBatch> {
        let mut streams = self.streams.borrow_mut();
        drain_streams(&mut streams.streams, subscription_event)
            .into_iter()
            .map(|(id, read)| SubscriptionBatch {
                id,
                events: read.events,
                error: read.error.map(SubscriptionBatchError::from),
            })
            .collect()
    }

    /// Pooled subscriptions still open.
    #[napi(getter)]
    pub fn size(&self) -> u32 {
        self.streams.borrow().streams.len() as u32
    }
}

// ---- transports ------------------------------------------------------------------

/// One sync link, carried by the host as byte frames. Frames are applied
/// and produced by the database's tick.
#[napi(js_name = "Transport")]
pub struct Transport {
    link: binding_host::Link,
}

#[napi]
impl Transport {
    /// SQLite's sync has no auxiliary chunk lane: large-value chunks travel
    /// as ordinary sync messages, so every frame belongs to the ordinary lane.
    #[napi(js_name = "routeAuxiliaryWireFrame")]
    pub fn route_auxiliary_wire_frame(&self, frame: Uint8Array) -> Option<Uint8Array> {
        Some(frame)
    }

    #[napi(js_name = "recvAuxiliaryWireFrames")]
    pub fn recv_auxiliary_wire_frames(&self) -> Vec<Uint8Array> {
        Vec::new()
    }

    #[napi(js_name = "auxiliaryReceiveTimeoutMs")]
    pub fn auxiliary_receive_timeout_ms(&self) -> Option<u32> {
        None
    }

    #[napi(js_name = "expireAuxiliaryReceive")]
    pub fn expire_auxiliary_receive(&self) {}

    #[napi(js_name = "auxiliaryOutboundReady")]
    pub fn auxiliary_outbound_ready(&self) -> bool {
        false
    }

    #[napi(js_name = "sendWireFrame")]
    pub fn send_wire_frame(&self, frame: Uint8Array) {
        self.link.receive([frame.to_vec()]);
    }

    #[napi(js_name = "sendWireFrames")]
    pub fn send_wire_frames(&self, frames: Vec<Uint8Array>) {
        self.link.receive(frames.iter().map(|frame| frame.to_vec()));
    }

    #[napi(js_name = "recvWireFrames")]
    pub fn recv_wire_frames(&self) -> Vec<Uint8Array> {
        self.link
            .take_outbound()
            .into_iter()
            .map(Uint8Array::from)
            .collect()
    }

    /// Tick the database this link belongs to: apply received frames, send
    /// what is due. Returns the number of frames waiting to be sent.
    #[napi]
    pub fn tick(&self) -> js::Result<u32> {
        let result = block_on(self.link.binding().tick());
        settle_promises();
        result.map_err(host_error)?;
        Ok(self.link.outbound_len() as u32)
    }

    /// Why the database dropped this upstream link on its own, with the
    /// core's stable `code` (`authority_changed` for a server whose store
    /// was recreated), or `null` while the link stands. The host stops the
    /// link with it rather than retry.
    #[napi(js_name = "upstreamRefusal")]
    pub fn upstream_refusal(&self) -> Option<UpstreamRefusal> {
        self.link.upstream_refusal().map(|error| UpstreamRefusal {
            code: error.code.as_str().to_owned(),
            message: error.to_string(),
        })
    }

    #[napi]
    pub fn close(&mut self) -> bool {
        self.link.close()
    }
}

/// Why the database dropped an upstream link (see `Transport.upstreamRefusal`).
#[napi(object)]
pub struct UpstreamRefusal {
    /// The core's stable error code.
    pub code: String,
    /// What the core said, as an error's message.
    pub message: String,
}

// ---- the database --------------------------------------------------------------

#[napi(js_name = "NapiDb")]
pub struct NapiDb {
    // Taken at close: every later call fails before touching the engine.
    inner: Rc<RefCell<Option<Inner>>>,
    owns_runtime: bool,
}

impl Drop for NapiDb {
    fn drop(&mut self) {
        // Release the tick callback so it no longer keeps Node's event loop
        // alive once JavaScript dropped an unclosed database.
        self.take_inner();
    }
}

impl NapiDb {
    fn take_inner(&self) -> Option<Inner> {
        let inner = self.inner.borrow_mut().take()?;
        if self.owns_runtime {
            inner.host.tick_scheduler.borrow_mut().take();
            if let Some(runtime) = inner.host.encryption.borrow_mut().take() {
                runtime.stop();
            }
        }
        Some(inner)
    }

    /// The runtime of an encrypted app, or why there is none.
    fn encryption(&self) -> napi::Result<Rc<encryption::Encryption>> {
        self.open_inner()?
            .host
            .encryption
            .borrow()
            .clone()
            .ok_or_else(|| napi::Error::from_reason("this database has no encryption runtime"))
    }

    fn keyless_hooks(
        bindings: &str,
        app_id: &str,
        environment: &str,
    ) -> napi::Result<Rc<dyn jazz::e2ee::E2eeHooks>> {
        let declarations = encryption::keyless_declarations(bindings, app_id, environment)
            .map_err(napi::Error::from_reason)?;
        Ok(jazz_e2ee_client::KeylessHooks::hooks(declarations))
    }

    fn open_keyless(
        schema: &[u8],
        config: &[u8],
        path: Option<&str>,
        bindings: &str,
        app_id: &str,
        environment: &str,
        how: OpenAs<'_>,
    ) -> napi::Result<Self> {
        let hooks = Self::keyless_hooks(bindings, app_id, environment)?;
        Self::open_with(schema, config, path, how, Some(Hooks::Keyless(hooks)))
    }

    fn open_encrypted(
        schema: &[u8],
        config: &[u8],
        path: Option<&str>,
        runtime: &EncryptionRuntime,
        proof: Option<&EncryptionProof>,
    ) -> napi::Result<Self> {
        let how = proof.map_or(OpenAs::Untrusted, |proof| OpenAs::SelfSigned {
            token: &proof.token,
            app_id: &runtime.builder.app_id,
            claimed_author: &proof.claimed_author,
        });
        Self::open_with(schema, config, path, how, Some(Hooks::Client(runtime)))
    }

    fn open_inner(&self) -> napi::Result<Inner> {
        self.inner.borrow().clone().ok_or_else(closed)
    }

    fn binding(&self) -> napi::Result<Binding> {
        Ok(self.open_inner()?.binding)
    }

    fn runtime_owner(binding: Binding) -> Self {
        Self {
            inner: Rc::new(RefCell::new(Some(Inner::new(binding)))),
            owns_runtime: true,
        }
    }

    fn open_as(
        schema: &[u8],
        config: &[u8],
        data_path: Option<&str>,
        how: OpenAs<'_>,
    ) -> napi::Result<Self> {
        Self::open_with(schema, config, data_path, how, None)
    }

    fn open_with(
        schema: &[u8],
        config: &[u8],
        data_path: Option<&str>,
        how: OpenAs<'_>,
        encrypted: Option<Hooks<'_>>,
    ) -> napi::Result<Self> {
        let (schema, mut config) = binding_host::decode_open(schema, config).map_err(host_error)?;
        let (identity, trusted) =
            binding_host::open_identity(&mut config, how).map_err(host_error)?;
        let mut db_config = jazz::tools::deployment::published::with_client_migrations(
            binding_host::db_config(schema, &config, identity),
            config.migrations.as_deref(),
        )
        .map_err(|error| host_error(binding_host::HostError::Argument(error)))?;
        // An app that declares encryption opens with the runtime's hooks,
        // built from its own declarations.
        let mut runtime = None;
        if let Some(Hooks::Keyless(hooks)) = &encrypted {
            db_config = db_config.with_e2ee(Rc::clone(hooks));
        }
        if let Some(Hooks::Client(encrypted)) = encrypted {
            db_config = db_config.with_e2ee(
                encrypted
                    .builder
                    .hooks(identity)
                    .map_err(napi::Error::from_reason)?,
            );
            db_config.add_creator_as_member = encrypted.builder.add_creator_as_member.get();
            runtime = Some(encrypted);
        }
        let db = match data_path {
            None => block_on(binding_host::open_memory(db_config)),
            Some(data_path) => block_on(binding_host::open_file(
                db_config,
                &store_file(data_path)?,
                FileOptions::default(),
            )),
        }
        .map_err(|error| napi_core_error(&error))?;
        let binding = block_on(Binding::new(db, identity, trusted));
        let owner = Self::runtime_owner(binding);
        if let Some(runtime) = runtime {
            let inner = owner.inner.borrow();
            let inner = inner.as_ref().expect("just opened");
            let host = Rc::downgrade(&inner.host);
            *inner.host.encryption.borrow_mut() = Some(runtime.builder.attach(
                Rc::clone(&inner.binding.db),
                Box::new(move || {
                    if let Some(host) = host.upgrade() {
                        host.request_tick();
                    }
                }),
            ));
        }
        Ok(owner)
    }

    fn request_permission_advice(
        &self,
        action: HostResult<jazz::protocol::PermissionAdviceAction>,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> js::Result<Either<String, PendingNativePermissionAdvice>> {
        let binding = self.binding()?;
        let action = action.map_err(host_error)?;
        let (id, future) = binding
            .request_permission_advice(action, author.as_deref(), claims)
            .map_err(host_error)?;
        let pending = PendingNativePermissionAdvice {
            future: RefCell::new(Some(future)),
            binding,
            id,
        };
        Ok(match pending.poll_once()? {
            Some(advice) => Either::A(advice),
            None => Either::B(pending),
        })
    }

    fn transport(&self, link: binding_host::Link) -> Transport {
        Transport { link }
    }
}

/// The SQLite file of a persistent store in directory `data_path`.
///
/// A directory that holds a store from before storage format 1 is refused
/// with the `unsupported_storage_codecs` code rather than opened empty next to
/// its data. `jazz.sqlite.native-root.v1` admits the regular SQLite database
/// and sidecar files plus unrelated regular files (such as `.DS_Store` or
/// notes), which are left alone. It refuses symlinks, directories and other
/// non-regular entries of any name, and regular files named like another
/// SQLite database or RocksDB store. Validate before SQLite opens or creates
/// any file.
fn store_file(data_path: &str) -> napi::Result<std::path::PathBuf> {
    let dir = std::path::Path::new(data_path);
    jazz::format::refuse_groove_store(dir).map_err(|error| host_error(HostError::Engine(error)))?;
    std::fs::create_dir_all(dir)
        .map_err(|error| napi::Error::from_reason(format!("create {data_path}: {error}")))?;
    let io_error = |error: std::io::Error| {
        napi::Error::from_reason(format!("inspect native database root {data_path}: {error}"))
    };
    for entry in std::fs::read_dir(dir).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        // DirEntry::file_type does not follow symlinks, so a link of any name
        // (which could redirect the database or a sidecar into another root)
        // is refused here, as is any directory.
        let regular = entry.file_type().map_err(io_error)?.is_file();
        if !regular || names_foreign_store(&entry.file_name().to_string_lossy()) {
            return Err(napi_core_error(&CoreError::new(
                jazz::db::ErrorCode::UnsupportedStorageCodecs,
                "native database root uses an unsupported storage layout; expected jazz.sqlite.native-root.v1",
            )));
        }
    }
    Ok(dir.join("jazz.sqlite"))
}

/// Whether a regular file named `name` in a native database root looks like
/// part of a store other than `jazz.sqlite`: another SQLite database or
/// sidecar, or a RocksDB (Groove) file. The match is by name only, so a copy
/// such as `jazz.sqlite.bak` is a harmless stray.
fn names_foreign_store(name: &str) -> bool {
    if matches!(
        name,
        "jazz.sqlite" | "jazz.sqlite-wal" | "jazz.sqlite-shm" | "jazz.sqlite-journal"
    ) {
        return false;
    }
    // SQLite databases and their sidecars, and RocksDB table files.
    const FOREIGN_SUFFIXES: [&str; 8] = [
        ".sqlite", ".sqlite3", ".db", ".db3", "-wal", "-shm", "-journal", ".sst",
    ];
    const ROCKSDB_NAMES: [&str; 3] = ["CURRENT", "IDENTITY", "LOCK"];
    const ROCKSDB_PREFIXES: [&str; 2] = ["MANIFEST-", "OPTIONS-"];
    let lower = name.to_ascii_lowercase();
    FOREIGN_SUFFIXES
        .iter()
        .any(|suffix| lower.ends_with(suffix))
        || ROCKSDB_NAMES.contains(&name)
        || ROCKSDB_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
}

#[napi]
impl NapiDb {
    /// Exact wire capabilities compiled into this native binding.
    ///
    /// The TypeScript WebSocket carrier uses this for its Hello instead of an
    /// independent feature list, so a package cannot advertise a codec this
    /// native artifact cannot decode.
    #[napi(js_name = "wireFeatures")]
    pub fn wire_features(&self) -> u32 {
        jazz::wire::current_wire_features() as u32
    }

    #[napi(js_name = "requestInsertPermissionAdvice")]
    pub fn request_insert_permission_advice(
        &self,
        table: String,
        cells: Uint8Array,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> js::Result<Either<String, PendingNativePermissionAdvice>> {
        self.request_permission_advice(binding_host::advice::insert(table, &cells), author, claims)
    }

    #[napi(js_name = "requestReadPermissionAdvice")]
    pub fn request_read_permission_advice(
        &self,
        table: String,
        row_id: Uint8Array,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> js::Result<Either<String, PendingNativePermissionAdvice>> {
        self.request_permission_advice(binding_host::advice::read(table, &row_id), author, claims)
    }

    #[napi(js_name = "requestUpdatePermissionAdvice")]
    pub fn request_update_permission_advice(
        &self,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> js::Result<Either<String, PendingNativePermissionAdvice>> {
        self.request_permission_advice(
            binding_host::advice::update(table, &row_id, &patch),
            author,
            claims,
        )
    }

    #[napi(js_name = "requestDeletePermissionAdvice")]
    pub fn request_delete_permission_advice(
        &self,
        table: String,
        row_id: Uint8Array,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> js::Result<Either<String, PendingNativePermissionAdvice>> {
        self.request_permission_advice(binding_host::advice::delete(table, &row_id), author, claims)
    }

    /// Admit a verified device account for this backend runtime only.
    // The JS name is ABI with jazz-tools; it stays until the TS side renames it.
    #[napi(js_name = "admitLocalFirstSession")]
    pub fn admit_device_account_session(
        &self,
        token: String,
        app_id: String,
        claimed_author: String,
    ) -> js::Result<()> {
        Ok(self
            .binding()?
            .admit_device_account_session(&token, &app_id, &claimed_author)
            .map_err(host_error)?)
    }

    #[napi(js_name = "insert")]
    pub fn insert_with_options(
        &self,
        env: Env,
        table: String,
        cells: Uint8Array,
        options: Option<InsertOptions>,
    ) -> js::Result<Write> {
        let inner = self.open_inner()?;
        let write = inner
            .binding
            .insert(&table, &cells, &insert_options(options));
        Ok(inner.write(&env, write)?)
    }

    #[napi(js_name = "insertInTransaction")]
    pub fn insert_in_transaction(
        &self,
        env: &Env,
        open_transaction_id: String,
        table: String,
        cells: Uint8Array,
        options: Option<InsertOptions>,
    ) -> js::Result<Uint8Array> {
        let row = self
            .binding()?
            .insert_in_transaction(
                &open_transaction_id,
                &table,
                &cells,
                &insert_options(options),
            )
            .map_err(|error| sync_host_error(env, error))?;
        Ok(row.to_bytes().to_vec().into())
    }

    #[napi(js_name = "update")]
    pub fn update_with_options(
        &self,
        env: Env,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
        options: Option<UpdateOptions>,
    ) -> js::Result<Write> {
        let inner = self.open_inner()?;
        let write = inner
            .binding
            .update(&table, &row_id, &patch, &update_options(options));
        Ok(inner.write(&env, write)?)
    }

    #[napi(js_name = "updateInTransaction")]
    pub fn update_in_transaction(
        &self,
        env: &Env,
        open_transaction_id: String,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
        options: Option<UpdateOptions>,
    ) -> js::Result<()> {
        Ok(self
            .binding()?
            .update_in_transaction(
                &open_transaction_id,
                &table,
                &row_id,
                &patch,
                &update_options(options),
            )
            .map_err(|error| sync_host_error(env, error))?)
    }

    /// Binding-only entrypoint for typed partial-value updates staged inside
    /// an open transaction. The transaction's bound identity authors them.
    #[napi(js_name = "updateLargeValuesInTransaction")]
    #[allow(clippy::too_many_arguments)] // The JS binding has six arguments plus the NAPI environment.
    pub fn update_large_values_in_transaction(
        &self,
        env: &Env,
        open_transaction_id: String,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
        mutations: JsonValue,
        updated_at_ms: Option<f64>,
    ) -> js::Result<()> {
        let mutations = large_value_updates(mutations)?;
        Ok(self
            .binding()?
            .update_large_values_in_transaction(
                &open_transaction_id,
                &table,
                &row_id,
                &patch,
                mutations,
                updated_at_ms,
            )
            .map_err(|error| sync_host_error(env, error))?)
    }

    /// Binding-only entrypoint for typed partial-value updates. The public
    /// TypeScript API validates column-kind-specific descriptors before they
    /// reach this encoded boundary.
    #[napi(js_name = "updateLargeValues")]
    pub fn update_large_values(
        &self,
        env: Env,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
        mutations: JsonValue,
        updated_at_ms: Option<f64>,
    ) -> js::Result<Write> {
        let mutations = large_value_updates(mutations)?;
        let inner = self.open_inner()?;
        let write =
            inner
                .binding
                .update_large_values(&table, &row_id, &patch, mutations, updated_at_ms);
        Ok(inner.write(&env, write)?)
    }

    #[napi(js_name = "upsert")]
    pub fn upsert_with_options(
        &self,
        env: Env,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        #[napi(ts_arg_type = "UpsertOptions | undefined | null")] options: Option<Unknown<'_>>,
    ) -> js::Result<Write> {
        let options = upsert_options(options)?;
        let inner = self.open_inner()?;
        let write = inner.binding.upsert(&table, &row_id, &cells, &options);
        Ok(inner.write(&env, write)?)
    }

    #[napi(js_name = "upsertInTransaction")]
    pub fn upsert_in_transaction(
        &self,
        env: &Env,
        open_transaction_id: String,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        #[napi(ts_arg_type = "UpsertOptions | undefined | null")] options: Option<Unknown<'_>>,
    ) -> js::Result<()> {
        let options = upsert_options(options)?;
        Ok(self
            .binding()?
            .upsert_in_transaction(&open_transaction_id, &table, &row_id, &cells, &options)
            .map_err(|error| sync_host_error(env, error))?)
    }

    #[napi(js_name = "delete")]
    pub fn delete_with_options(
        &self,
        env: Env,
        table: String,
        row_id: Uint8Array,
        options: Option<DeleteOptions>,
    ) -> js::Result<Write> {
        let inner = self.open_inner()?;
        let write = inner
            .binding
            .delete(&table, &row_id, &delete_options(options));
        Ok(inner.write(&env, write)?)
    }

    #[napi(js_name = "deleteInTransaction")]
    pub fn delete_in_transaction(
        &self,
        env: &Env,
        open_transaction_id: String,
        table: String,
        row_id: Uint8Array,
        options: Option<DeleteOptions>,
    ) -> js::Result<()> {
        Ok(self
            .binding()?
            .delete_in_transaction(
                &open_transaction_id,
                &table,
                &row_id,
                &delete_options(options),
            )
            .map_err(|error| sync_host_error(env, error))?)
    }

    #[napi(js_name = "restore")]
    pub fn restore_with_options(
        &self,
        env: Env,
        table: String,
        row_id: Uint8Array,
        cells: Option<Uint8Array>,
        options: Option<RestoreOptions>,
    ) -> js::Result<Write> {
        let inner = self.open_inner()?;
        let write =
            inner
                .binding
                .restore(&table, &row_id, cells.as_deref(), &restore_options(options));
        Ok(inner.write(&env, write)?)
    }

    #[napi(js_name = "restoreInTransaction")]
    pub fn restore_in_transaction(
        &self,
        env: &Env,
        open_transaction_id: String,
        table: String,
        row_id: Uint8Array,
        cells: Option<Uint8Array>,
        options: Option<RestoreOptions>,
    ) -> js::Result<()> {
        Ok(self
            .binding()?
            .restore_in_transaction(
                &open_transaction_id,
                &table,
                &row_id,
                cells.as_deref(),
                &restore_options(options),
            )
            .map_err(|error| sync_host_error(env, error))?)
    }

    #[napi(js_name = "beginStreamingMutation")]
    #[allow(clippy::too_many_arguments)]
    pub fn begin_streaming_mutation(
        &self,
        env: &Env,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        column: String,
        mutation: Option<String>,
        author: Option<Uint8Array>,
        attribution: Option<Uint8Array>,
        updated_at_ms: Option<f64>,
        head: Option<JsonValue>,
        base: Option<JsonValue>,
        transaction: Option<String>,
    ) -> js::Result<StreamingMutation> {
        let head = structured(head, "head")?;
        let base = structured(base, "base")?;
        let inner = self.open_inner()?;
        let mutation = inner
            .binding
            .begin_streaming_mutation(
                table,
                &row_id,
                &cells,
                column,
                mutation.as_deref(),
                author.as_deref(),
                attribution.as_deref(),
                updated_at_ms,
                head,
                base,
                transaction.as_deref(),
            )
            .map_err(|error| sync_host_error(env, error))?;
        Ok(StreamingMutation { inner, mutation })
    }

    /// Open a backend runtime for an app that declares encryption, with
    /// hooks that hold no keys: it relays the clients' ciphertext and works
    /// on plain columns (`bindings` is the generated bindings' compiled app).
    #[napi(factory, js_name = "openMemoryAsBackendKeyless")]
    pub fn open_memory_as_backend_keyless(
        schema: Uint8Array,
        config: Uint8Array,
        bindings: String,
        app_id: String,
        environment: String,
    ) -> js::Result<Self> {
        Ok(Self::open_keyless(
            &schema,
            &config,
            None,
            &bindings,
            &app_id,
            &environment,
            OpenAs::Backend,
        )?)
    }

    /// Open a session's in-memory database for an app that declares
    /// encryption, with hooks that hold no keys, as the session's own author
    /// (the configured one, or the one `proof` proves), never SYSTEM: the
    /// client a backend serves a thin session's transactions with. Encrypted
    /// writes are refused, encrypted cells unreadable, plain data normal.
    #[napi(factory, js_name = "openMemoryKeyless")]
    pub fn open_memory_keyless(
        schema: Uint8Array,
        config: Uint8Array,
        bindings: String,
        app_id: String,
        environment: String,
        proof: Option<EncryptionProof>,
    ) -> js::Result<Self> {
        let how = match &proof {
            Some(proof) => OpenAs::SelfSigned {
                token: &proof.token,
                app_id: proof
                    .app_id
                    .as_deref()
                    .ok_or_else(|| napi::Error::from_reason("missing proof appId"))?,
                claimed_author: &proof.claimed_author,
            },
            None => OpenAs::Untrusted,
        };
        Ok(Self::open_keyless(
            &schema,
            &config,
            None,
            &bindings,
            &app_id,
            &environment,
            how,
        )?)
    }

    /// [`Self::open_memory_as_backend_keyless`] over a durable store in
    /// directory `data_path`.
    #[napi(factory, js_name = "openPersistentAsBackendKeyless")]
    pub fn open_persistent_as_backend_keyless(
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
        bindings: String,
        app_id: String,
        environment: String,
    ) -> js::Result<Self> {
        Ok(Self::open_keyless(
            &schema,
            &config,
            Some(&data_path),
            &bindings,
            &app_id,
            &environment,
            OpenAs::Backend,
        )?)
    }

    #[napi(factory, js_name = "openMemory")]
    pub fn open_memory(schema: Uint8Array, config: Uint8Array) -> js::Result<Self> {
        Ok(Self::open_as(&schema, &config, None, OpenAs::Untrusted)?)
    }

    /// Open a deliberate backend runtime. Unlike the public raw-open entrypoint,
    /// this explicit ABI derives the canonical system author.
    #[napi(factory, js_name = "openMemoryAsBackend")]
    pub fn open_memory_as_backend(schema: Uint8Array, config: Uint8Array) -> js::Result<Self> {
        Ok(Self::open_as(&schema, &config, None, OpenAs::Backend)?)
    }

    /// Open with a verified Jazz self-signed client identity. This is a
    /// separate ABI entrypoint deliberately: a new client cannot accidentally
    /// hand proof bytes to an old constructor, and an old client cannot enter
    /// the proof-bearing path.
    #[napi(factory, js_name = "openMemoryWithSelfSignedProof")]
    pub fn open_memory_with_self_signed_proof(
        schema: Uint8Array,
        config: Uint8Array,
        token: String,
        app_id: String,
        claimed_author: String,
    ) -> js::Result<Self> {
        let how = OpenAs::SelfSigned {
            token: &token,
            app_id: &app_id,
            claimed_author: &claimed_author,
        };
        Ok(Self::open_as(&schema, &config, None, how)?)
    }

    /// Open the durable store in directory `data_path` (one SQLite file).
    #[napi(factory, js_name = "openPersistent")]
    pub fn open_persistent(
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
    ) -> js::Result<Self> {
        Ok(Self::open_as(
            &schema,
            &config,
            Some(&data_path),
            OpenAs::Untrusted,
        )?)
    }

    /// Open a deliberate persistent backend runtime. This is intentionally a
    /// distinct ABI from the public raw-open entrypoint.
    #[napi(factory, js_name = "openPersistentAsBackend")]
    pub fn open_persistent_as_backend(
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
    ) -> js::Result<Self> {
        Ok(Self::open_as(
            &schema,
            &config,
            Some(&data_path),
            OpenAs::Backend,
        )?)
    }

    #[napi(factory, js_name = "openPersistentWithSelfSignedProof")]
    pub fn open_persistent_with_self_signed_proof(
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
        token: String,
        app_id: String,
        claimed_author: String,
    ) -> js::Result<Self> {
        let how = OpenAs::SelfSigned {
            token: &token,
            app_id: &app_id,
            claimed_author: &claimed_author,
        };
        Ok(Self::open_as(&schema, &config, Some(&data_path), how)?)
    }

    /// Open an encrypted app's in-memory database with its runtime
    /// installed (see [`EncryptionRuntime`]).
    #[napi(factory, js_name = "openMemoryEncrypted")]
    pub fn open_memory_encrypted(
        schema: Uint8Array,
        config: Uint8Array,
        runtime: &EncryptionRuntime,
        proof: Option<EncryptionProof>,
    ) -> js::Result<Self> {
        Ok(Self::open_encrypted(
            &schema,
            &config,
            None,
            runtime,
            proof.as_ref(),
        )?)
    }

    /// Open an encrypted app's durable database in directory `data_path`
    /// with its runtime installed. The private store is the app's.
    #[napi(factory, js_name = "openPersistentEncrypted")]
    pub fn open_persistent_encrypted(
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
        runtime: &EncryptionRuntime,
        proof: Option<EncryptionProof>,
    ) -> js::Result<Self> {
        Ok(Self::open_encrypted(
            &schema,
            &config,
            Some(&data_path),
            runtime,
            proof.as_ref(),
        )?)
    }

    /// Run one end-to-end encryption operation: its name and arguments (JSON),
    /// answered with the JSON text of its result.
    #[napi(js_name = "encryptionCall", ts_return_type = "Promise<string>")]
    pub fn encryption_call(
        &self,
        env: &Env,
        op: String,
        #[napi(ts_arg_type = "Record<string, unknown> | undefined | null")] args: Option<
            Unknown<'_>,
        >,
    ) -> js::Result<PromiseRaw<'static, String>> {
        let encryption = self.encryption()?;
        let args = args.map(JsonValue::from_unknown).transpose()?;
        let future = encryption.call_driven(op, json_or_null(args));
        let promise = json_promise(env, future)?;
        encryption.drive();
        settle_promises();
        Ok(promise)
    }

    /// The runtime's reports since the last call, each a JSON text.
    #[napi(js_name = "takeEncryptionReports")]
    pub fn take_encryption_reports(&self) -> js::Result<Vec<String>> {
        Ok(self.encryption()?.take_reports())
    }

    /// The runtime's account status and last error, as JSON text.
    #[napi(js_name = "encryptionState")]
    pub fn encryption_state(&self) -> js::Result<String> {
        Ok(self.encryption()?.state().to_string())
    }

    /// An app subscription on an encrypted table opened (`open`) or closed:
    /// `table` is the space table's name, `id` the space's row.
    #[napi(js_name = "encryptionUseSpace")]
    pub fn encryption_use_space(&self, table: String, id: String, open: bool) -> js::Result<()> {
        self.encryption()?.use_space(open, table, id);
        Ok(())
    }

    /// Where each encrypted table keeps its space, as JSON text:
    /// `{ table: { scope, column } }` (`column` is `id` when a table's rows
    /// are their own spaces).
    #[napi(js_name = "encryptionSpaces")]
    pub fn encryption_spaces(&self) -> js::Result<String> {
        Ok(self.encryption()?.spaces.to_string())
    }

    /// The connection to the server went away (`false`) or came back (`true`).
    #[napi(js_name = "encryptionSetOnline")]
    pub fn encryption_set_online(&self, online: bool) -> js::Result<()> {
        self.encryption()?.set_online(online);
        Ok(())
    }

    /// Register and return a typed view backed by this same runtime owner,
    /// of an app shipping `migrations` (its bundle as JSON).
    #[napi(js_name = "registerSchema")]
    pub fn register_schema(
        &self,
        schema: Uint8Array,
        migrations: Option<Uint8Array>,
    ) -> js::Result<Self> {
        let inner = self.open_inner()?;
        let migrations =
            jazz::tools::deployment::published::lowered_client_migrations(migrations.as_deref())
                .map_err(|error| host_error(binding_host::HostError::Argument(error)))?;
        let binding = inner
            .binding
            .register_schema_shipping(&schema, migrations)
            .map_err(host_error)?;
        Ok(Self {
            inner: Rc::new(RefCell::new(Some(Inner { binding, ..inner }))),
            owns_runtime: false,
        })
    }

    /// Begin one owner-wide transaction.
    #[napi(js_name = "beginTransaction")]
    pub fn begin_transaction(
        &self,
        open_transaction_id: String,
        kind: String,
        author: Option<Uint8Array>,
        attribution: Option<Uint8Array>,
    ) -> js::Result<()> {
        Ok(self
            .binding()?
            .begin_transaction(
                &open_transaction_id,
                &kind,
                author.as_deref(),
                attribution.as_deref(),
            )
            .map_err(host_error)?)
    }

    /// Commit an owner-wide transaction by id and optional kind.
    #[napi(js_name = "commitTransaction")]
    pub fn commit_transaction(
        &self,
        env: Env,
        open_transaction_id: String,
        kind: Option<String>,
    ) -> js::Result<Write> {
        let inner = self.open_inner()?;
        let write = inner
            .binding
            .commit_transaction(&open_transaction_id, kind.as_deref());
        Ok(inner.write(&env, write)?)
    }

    /// Roll back an owner-wide open transaction by id.
    #[napi(js_name = "rollbackTransaction")]
    pub fn rollback_transaction(&self, open_transaction_id: String) -> js::Result<()> {
        Ok(self
            .binding()?
            .rollback_transaction(&open_transaction_id)
            .map_err(host_error)?)
    }

    /// The host's tick callback: `"immediate"` for ready sync work or
    /// `"after:<milliseconds>"` for pending work with a deadline.
    #[napi(
        js_name = "setTickScheduler",
        ts_args_type = "callback: ((err: Error | null, arg: string) => void)"
    )]
    pub fn set_tick_scheduler(&self, callback: Function<'_, String, ()>) -> js::Result<()> {
        let inner = self.open_inner()?;
        let requested = Arc::clone(&inner.host.tick_requested);
        let callback = callback
            .build_threadsafe_function::<String>()
            .callee_handled::<true>()
            .build_callback(move |context| {
                // Clear before calling JS so reentrant progress can request
                // the next turn without losing its wake.
                if context.value == "immediate" {
                    requested.store(false, Ordering::Release);
                }
                Ok(context.value)
            })?;
        *inner.host.tick_scheduler.borrow_mut() = Some(callback);
        Ok(())
    }

    /// Register a callback for rejected writes that no active wait consumed.
    #[napi(
        js_name = "onMutationError",
        ts_args_type = "callback: (event: any) => void"
    )]
    pub fn on_mutation_error(&self, callback: Function<'_, JsonValue, ()>) -> js::Result<()> {
        let inner = self.open_inner()?;
        *inner.host.mutation_errors.borrow_mut() = Some(callback.create_ref()?);
        let upstream = callback
            .build_threadsafe_function::<serde_json::Value>()
            .callee_handled::<false>()
            .weak::<true>()
            .build()?;
        *inner.host.upstream_mutation_errors.borrow_mut() = Some(upstream);
        // Refusals kept while nobody listened reach this callback now.
        inner.binding.redeliver_rejections();
        Ok(())
    }

    /// Decode, prepare, and execute one read inside Rust. Query-plan ownership
    /// never crosses the language boundary.
    #[napi]
    pub fn all(
        &self,
        env: &Env,
        query: Uint8Array,
        #[napi(
            ts_arg_type = "{ tier?: string; include_deleted?: boolean; sync?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
        open_transaction_id: Option<String>,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        let read = self
            .binding()?
            .all_reporting(
                query.to_vec(),
                &json_or_null(opts),
                open_transaction_id.as_deref(),
                author.as_deref(),
                claims,
            )
            .map_err(|err| sync_host_error(env, err))?;
        reporting_read_or_pending(env, read)
    }

    /// Bind receipt-correlation claims to this client's own admitted identity.
    #[napi(js_name = "setSessionClaims")]
    pub fn set_session_claims(
        &self,
        #[napi(ts_arg_type = "Record<string, unknown> | undefined | null")] claims: Option<
            JsonValue,
        >,
    ) -> js::Result<()> {
        block_on(self.binding()?.set_session_claims(json_or_null(claims))).map_err(host_error)?;
        settle_promises();
        Ok(())
    }

    /// Set the claims reads and writes as `author` are checked with.
    #[napi(js_name = "setIdentityClaims")]
    pub fn set_identity_claims(
        &self,
        author: Uint8Array,
        #[napi(ts_arg_type = "Record<string, unknown> | undefined | null")] claims: Option<
            JsonValue,
        >,
    ) -> js::Result<()> {
        block_on(
            self.binding()?
                .set_identity_claims(&author, json_or_null(claims)),
        )
        .map_err(host_error)?;
        settle_promises();
        Ok(())
    }

    /// The catalogue identity of a table, as UUID text: what a member row's
    /// `scopeId` holds for its space table.
    #[napi(js_name = "tableIdentity")]
    pub fn table_identity(&self, table: String) -> js::Result<String> {
        Ok(self.binding()?.table_identity(&table).map_err(host_error)?)
    }

    /// Exact local state for the write-merge bridge: the row as stored,
    /// deleted or not. Write authorization remains at the mutation boundary;
    /// this is not a query.
    #[napi(js_name = "localCurrentRow")]
    pub fn local_current_row(
        &self,
        env: &Env,
        table: String,
        row_id: Uint8Array,
    ) -> js::Result<Uint8Array> {
        Ok(self
            .binding()?
            .local_current_row(&table, &row_id)
            .map_err(|err| sync_host_error(env, err))?
            .into())
    }

    /// The Inspector's `explain`: for each of `rows` (uuid strings) of
    /// `table`, which clauses of its read policy (the arms of its top-level
    /// `Or`) hold for `author` with `claims`.
    #[napi(js_name = "explainReadClauses")]
    pub fn explain_read_clauses(
        &self,
        table: String,
        rows: Vec<String>,
        author: Uint8Array,
        claims: Option<JsonValue>,
    ) -> js::Result<ExplainedReadClauses> {
        let (clauses, rows) = block_on(
            self.binding()?
                .explain_read_clauses(&table, &rows, &author, claims),
        )
        .map_err(host_error)?;
        Ok(ExplainedReadClauses {
            clauses: clauses as u32,
            rows: rows
                .into_iter()
                .map(|(id, holds)| ExplainedReadRow { id, holds })
                .collect(),
        })
    }

    /// A pool that pooled subscriptions are read through, all in one call.
    #[napi(js_name = "createSubscriptionPool")]
    pub fn create_subscription_pool(&self) -> SubscriptionPool {
        SubscriptionPool::new()
    }

    #[napi]
    pub fn subscribe(
        &self,
        query: Uint8Array,
        #[napi(ts_arg_type = "{ tier?: string; include_deleted?: boolean } | undefined | null")]
        opts: Option<JsonValue>,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> js::Result<Subscription> {
        let stream = self
            .binding()?
            .subscribe(&query, &json_or_null(opts), author.as_deref(), claims)
            .map_err(host_error)?;
        Ok(Subscription {
            stream: SubscriptionSlot::Own(Some(Box::new(stream))),
        })
    }

    /// Apply received frames and send what is due on every link.
    #[napi]
    pub fn tick(&self) -> js::Result<()> {
        let result = block_on(self.binding()?.tick());
        if let Some(encryption) = self.open_inner()?.host.encryption.borrow().clone() {
            encryption.drive();
        }
        settle_promises();
        Ok(result.map_err(host_error)?)
    }

    /// Configure Jazz-owned upload ingress and unpublished-tree expiry limits.
    #[napi(js_name = "setLargeValueStagingPolicy")]
    pub fn set_large_value_staging_policy(
        &self,
        incoming_bytes_per_window: f64,
        window_ms: f64,
        max_age_ms: Option<f64>,
    ) -> js::Result<()> {
        Ok(self
            .binding()?
            .set_large_value_staging_policy(incoming_bytes_per_window, window_ms, max_age_ms)
            .map_err(host_error)?)
    }

    /// Run one idempotent expiry pass; native hosts normally call this on a timer.
    #[napi(js_name = "evictExpiredStagedLargeValues")]
    pub fn evict_expired_staged_large_values(&self) -> js::Result<u32> {
        let evicted = block_on(self.binding()?.db.evict_expired_staged_large_values())
            .map_err(|error| napi_core_error(&error))?;
        Ok(evicted as u32)
    }

    /// This store's uploads: how many still wait for the server's verdict
    /// and since when, and what the server refused since it opened
    /// (`{ pending, oldestPendingAt, rejected, lastRejection }`).
    #[napi(js_name = "uploadStatus")]
    pub fn upload_status(&self) -> js::Result<JsonValue> {
        serde_json::to_value(self.binding()?.upload_status())
            .map_err(|error| napi::Error::from_reason(error.to_string()).into())
    }

    /// Ids of `table`'s rows with a write the server has neither accepted
    /// nor refused yet (recovered ones included), sorted.
    #[napi(js_name = "pendingUploadRows")]
    pub fn pending_upload_rows(&self, table: String) -> js::Result<Vec<String>> {
        Ok(self
            .binding()?
            .pending_upload_rows(&table)
            .map_err(host_error)?
            .into_iter()
            .map(|row| row.0.to_string())
            .collect())
    }

    /// The optimistic in-memory side of a client/worker pair: its
    /// subscriptions open with the worker's answer.
    #[napi(js_name = "setNonDurableClient")]
    pub fn set_non_durable_client(&self) -> js::Result<()> {
        jazz::db::block_on(self.binding()?.set_non_durable_client());
        Ok(())
    }

    /// Report what the host knows about the path to the authoritative server
    /// (`"none" | "attempting" | "live" | "failed"`; the TypeScript names
    /// `"connecting" | "connected" | "unavailable"` are accepted as aliases).
    /// Drives only local-first reads that wait for the server on first load.
    #[napi(js_name = "setRemoteLinkHint")]
    pub fn set_remote_link_hint(&self, state: String) -> js::Result<()> {
        block_on(self.binding()?.set_remote_link_hint(&state)).map_err(host_error)?;
        settle_promises();
        Ok(())
    }

    /// A fresh upstream link machine for this runtime's server connection;
    /// `seed` drives its retry jitter.
    #[napi(js_name = "createUpstreamLink")]
    pub fn create_upstream_link(&self, seed: f64) -> NapiUpstreamLink {
        NapiUpstreamLink::new(seed)
    }

    /// The slow-query report due now: one line per slow query shape,
    /// naming `app`: the first at once, then at most every 10 seconds. The caller
    /// writes each line with `console.warn`.
    #[napi(js_name = "takeSlowQueryReport")]
    pub fn take_slow_query_report(&self, app: Option<String>) -> js::Result<Vec<String>> {
        Ok(self.binding()?.take_slow_query_report(app.as_deref()))
    }

    /// Report query runs of at least `ms` milliseconds (default 50); 0
    /// turns the slow-query log off.
    #[napi(js_name = "setSlowQueryThresholdMs")]
    pub fn set_slow_query_threshold_ms(&self, ms: f64) -> js::Result<()> {
        self.binding()?
            .set_slow_query_threshold_ms(ms)
            .map_err(host_error)?;
        Ok(())
    }

    /// Coalesce a subscription's undelivered deltas past `rows` changed
    /// rows (default 10000, at least 1) into one reset of its current
    /// result.
    #[napi(js_name = "setSubscriptionBacklogRows")]
    pub fn set_subscription_backlog_rows(&self, rows: f64) -> js::Result<()> {
        self.binding()?
            .set_subscription_backlog_rows(rows)
            .map_err(host_error)?;
        Ok(())
    }

    /// Connect to the server over byte frames the host carries: each frame
    /// is one postcard `SyncMessage`.
    #[napi(js_name = "connectUpstream")]
    pub fn connect_upstream(&self) -> js::Result<Transport> {
        Ok(self.transport(self.binding()?.connect_upstream()))
    }

    /// Connect after the carrier accepted the server Hello. Negotiated
    /// features must be compiled in; the rest of the session context
    /// (authority endpoints, epochs) has no counterpart on SQLite's sync, so
    /// this is otherwise [`Self::connect_upstream`].
    #[napi(js_name = "connectUpstreamWithSession")]
    pub fn connect_upstream_with_session(
        &self,
        _protocol_version: u32,
        features: u32,
        remote_node: Buffer,
        _remote_epoch: BigInt,
        local_node: Buffer,
        _local_epoch: BigInt,
    ) -> js::Result<Transport> {
        if remote_node.len() != 16 {
            return Err(
                napi::Error::from_reason("server hello authority node must be 16 bytes").into(),
            );
        }
        if local_node.len() != 16 {
            return Err(napi::Error::from_reason("local peer identity must be 16 bytes").into());
        }
        let unsupported = u64::from(features) & !jazz::wire::current_wire_features();
        if unsupported != 0 {
            return Err(napi::Error::from_reason(format!(
                "server negotiated wire features {features:#x}, but this native binding was not compiled with {unsupported:#x}"
            ))
            .into());
        }
        // The link sends what the server negotiated it can take (a
        // serializable commit that read through a branch view, say).
        let link = self.binding()?.connect_upstream();
        link.set_features(u64::from(features));
        Ok(self.transport(link))
    }

    /// The newest transaction time this database handed out under its node
    /// (a `TxTime` is the commit seq). A leased node's next holder seeds it
    /// through `seedForegroundTxTimeHighWater`.
    #[napi(js_name = "foregroundTxTimeHighWater")]
    pub fn foreground_tx_time_high_water(&self) -> js::Result<BigInt> {
        Ok(BigInt::from(self.binding()?.db.seq_high_water()))
    }

    /// Continue after a previous holder of this node: no transaction time at
    /// or below `high_water` is reused.
    #[napi(js_name = "seedForegroundTxTimeHighWater")]
    pub fn seed_foreground_tx_time_high_water(&self, high_water: BigInt) -> js::Result<()> {
        let (negative, value, lossless) = high_water.get_u64();
        if negative || !lossless {
            return Err(napi::Error::from_reason(
                "foreground transaction time high water must be an unsigned 64-bit integer",
            )
            .into());
        }
        self.binding()?.db.seed_seq_floor(value);
        Ok(())
    }

    /// Serve one of a thin session's transaction requests (SPEC 8.14), as
    /// the backend's client linked to the core as that session: `request`
    /// is the request's wire frame, the answer the response's. A commit
    /// answers once the core accepted it. One request at a time.
    #[napi(js_name = "serveThinTransaction")]
    pub fn serve_thin_transaction(
        &self,
        request: Uint8Array,
    ) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        let inner = self.open_inner()?;
        let request = match jazz::wire::decode_postcard_frame(&request) {
            Ok(jazz::wire::WireFrame::ThinRequest(request)) => request,
            _ => {
                return Err(napi::Error::from_reason(
                    "serveThinTransaction takes a thin request frame",
                )
                .into());
            }
        };
        let binding = inner.binding.clone();
        let thin = Rc::clone(&inner.thin);
        let state = thin.borrow_mut().replace(ThinState::Serving);
        let transactions = match state {
            Some(ThinState::Idle(transactions)) => transactions,
            None => {
                let made = if binding.author() == jazz::ids::AuthorSubject::SYSTEM {
                    // A trusted backend client's session, linked as the
                    // backend itself: its writes are the backend's, as the
                    // core ran them as SYSTEM.
                    Ok(jazz::thin_transactions::ThinTransactions::trusted())
                } else {
                    jazz::thin_transactions::ThinTransactions::new(binding.author())
                };
                match made {
                    Ok(transactions) => transactions,
                    Err(error) => {
                        *thin.borrow_mut() = None;
                        return Err(host_error(HostError::Engine(error)).into());
                    }
                }
            }
            Some(ThinState::Serving) => {
                *thin.borrow_mut() = Some(ThinState::Serving);
                return Err(napi::Error::from_reason(
                    "serveThinTransaction serves one request at a time",
                )
                .into());
            }
        };
        let mut serving = Serving {
            cell: thin,
            transactions: Some(transactions),
        };
        read_or_pending(Box::pin(async move {
            let body = serving
                .transactions
                .as_mut()
                .expect("held until dropped")
                .serve(&binding.db, request.id, &request.op)
                .await;
            drop(serving);
            jazz::wire::encode_frame(&jazz::wire::WireFrame::ThinResponse(
                jazz::thin::ThinResponse {
                    id: request.id,
                    body,
                },
            ))
            .map_err(|error| HostError::Argument(format!("encode thin answer: {error}")))
        }))
    }

    /// End the thin session's open transactions and uploads: nothing of
    /// them reached the core.
    #[napi(js_name = "releaseThinTransactions")]
    pub fn release_thin_transactions(&self) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        let inner = self.open_inner()?;
        let binding = inner.binding.clone();
        let state = inner.thin.borrow_mut().take();
        read_or_pending(Box::pin(async move {
            if let Some(ThinState::Idle(transactions)) = state {
                transactions.release(&binding.db).await;
            }
            Ok(Vec::new())
        }))
    }

    /// Capture a snapshot reference, as its persisted string's bytes.
    #[napi(js_name = "captureSnapshot")]
    pub fn capture_snapshot(&self) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        read_or_pending(self.binding()?.capture_snapshot())
    }

    /// Complete once every local write so far reached `tier`. Commits are in
    /// the SQLite file when their write call returns, so only `global` waits.
    #[napi(js_name = "waitForPendingWrites")]
    pub fn wait_for_pending_writes(
        &self,
        tier: String,
    ) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        let binding = self.binding()?;
        let tier = jazz::binding_args::durability_tier(&tier).map_err(napi::Error::from_reason)?;
        match tier {
            DurabilityTier::None | DurabilityTier::Local => Ok(Either::A(Vec::new().into())),
            DurabilityTier::Global => {
                let wait = binding.wait_for_pending_writes_global();
                read_or_pending(Box::pin(wait.map(|result| result.map(|()| Vec::new()))))
            }
        }
    }

    /// TS `waitForSync`: complete once `writer`'s writes so far (a trusted
    /// session's author bytes; none for this database's own) reached `tier`,
    /// failing with a rejection of theirs, including one that settled
    /// before and no wait reported.
    #[napi(js_name = "waitForSync")]
    pub fn wait_for_sync(
        &self,
        tier: String,
        writer: Option<Uint8Array>,
    ) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        let binding = self.binding()?;
        let tier = jazz::binding_args::durability_tier(&tier).map_err(napi::Error::from_reason)?;
        match tier {
            DurabilityTier::None | DurabilityTier::Local => Ok(Either::A(Vec::new().into())),
            DurabilityTier::Global => {
                let wait = binding
                    .wait_for_sync_global(writer.as_deref())
                    .map_err(host_error)?;
                read_or_pending(Box::pin(wait.map(|result| result.map(|()| Vec::new()))))
            }
        }
    }

    /// Close the database (a schema view leaves its owner open). The
    /// package's `close()` wraps this pollable form.
    #[napi(js_name = "__closePollable", skip_typescript)]
    pub fn close(&self) -> js::Result<Either<Uint8Array, PendingNativeRead>> {
        // A close failure still consumes the binding: retrying a partially
        // failed close would re-enter an indeterminate runtime.
        let Some(inner) = self.take_inner() else {
            return Ok(Either::A(Vec::new().into()));
        };
        if self.owns_runtime {
            inner.host.mutation_errors.borrow_mut().take();
            inner.host.upstream_mutation_errors.borrow_mut().take();
            block_on(inner.binding.close()).map_err(host_error)?;
        }
        Ok(Either::A(Vec::new().into()))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JazzServerStartOptions {
    app_id: String,
    port: Option<u16>,
    host: Option<String>,
    data_dir: Option<String>,
    in_memory: Option<bool>,
    /// Required unless the server trusts a workload, which replaces them.
    backend_secret: Option<String>,
    admin_secret: Option<String>,
    upstream_url: Option<String>,
    telemetry_collector_url: Option<String>,
    schema: Option<Vec<u8>>,
    /// Live WebSocket connections one signed-in user may hold.
    max_connections_per_user: Option<u32>,
    /// Trust this OIDC workload instead of the shared secrets.
    trusted_workload: Option<TrustedWorkloadOptions>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustedWorkloadOptions {
    issuer: String,
    audience: String,
    subject: String,
}

fn parse_jazz_server_start_options(options: JsonValue) -> napi::Result<JazzServerStartOptions> {
    serde_json::from_value(options)
        .map_err(|error| napi::Error::from_reason(format!("Invalid JazzServer options: {error}")))
}

/// Where a server this process runs reports: to `collector_url` when one
/// is given, else to stderr ([`init_server_logs`]).
pub(crate) fn init_jazz_server_telemetry(collector_url: Option<&str>) {
    let Some(collector_url) = collector_url else {
        init_server_logs("warn");
        return;
    };
    // The first collector this process reports to; a later core naming the
    // same one shares it.
    static COLLECTOR: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    if let Some(installed) = COLLECTOR.get() {
        if installed != collector_url {
            tracing::warn!(
                "telemetry: this process already reports to {installed}; the collector at {collector_url} gets nothing"
            );
        }
        return;
    }
    if tracing::dispatcher::has_been_set() {
        tracing::warn!(
            "telemetry: this process already reports its logs elsewhere; the collector at {collector_url} gets nothing"
        );
        return;
    }
    let _ = COLLECTOR.set(collector_url.to_owned());
    jazz_otel::init_process_tracing_with_endpoint_once(
        "jazz-server",
        collector_url,
        &["jazz_tools=trace", "tower_http=debug"],
    );
}

/// Print Jazz's logs at `level` and up to stderr (`RUST_LOG` replaces the
/// filter), unless the process already reports elsewhere. Without it, a
/// server inside a Node process says nothing: a blob store refusing its
/// credentials, or a failing upload, goes unnoticed. An in-process core
/// passes `warn`, to stay quiet in the app's own output; a served core
/// passes `info`, as the standalone server logs.
pub(crate) fn init_server_logs(level: &str) {
    use std::io::IsTerminal;
    // `RUST_LOG` sets the level of any target it names; the defaults cover
    // the rest.
    let jazz = format!("jazz={level}");
    let jazz_tools = format!("jazz_tools={level}");
    let filter = jazz_otel::env_filter_with_defaults(&["error", &jazz, &jazz_tools]);
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .with_env_filter(filter)
        .try_init();
}

// ============================================================================
// JazzServer
// ============================================================================

#[napi]
pub struct JazzServer {
    lifecycle: Arc<JazzServerLifecycle>,
}

struct JazzServerLifecycle {
    state: Mutex<JazzServerState>,
    changed: tokio::sync::watch::Sender<()>,
}

enum JazzServerState {
    Running(Box<CoreJazzServer>),
    Stopping,
    Stopped(std::result::Result<(), String>),
}

#[napi]
impl JazzServer {
    #[napi(factory, ts_return_type = "Promise<JazzServer>")]
    pub async fn start(
        #[napi(
            ts_arg_type = "{ appId: string; backendSecret?: string; adminSecret?: string; port?: number; host?: string; dataDir?: string; inMemory?: boolean; telemetryCollectorUrl?: string; schema?: Buffer | Uint8Array | number[]; maxConnectionsPerUser?: number; trustedWorkload?: { issuer: string; audience: string; subject: string } }"
        )]
        options: JsonValue,
    ) -> napi::Result<Self> {
        let mut opts = parse_jazz_server_start_options(options)?;
        init_jazz_server_telemetry(opts.telemetry_collector_url.as_deref());

        let core_server_shell_schema = opts
            .schema
            .take()
            .map(|schema_bytes| binding_host::decode_schema(&schema_bytes))
            .transpose()
            .map_err(host_error)?;

        let app_id = AppId::from_text(&opts.app_id);

        // A trusted workload replaces the shared secrets.
        let trusted_workload = opts.trusted_workload.take().map(|workload| {
            jazz_server::middleware::workload::TrustedWorkload {
                issuer: workload.issuer,
                audience: workload.audience,
                subject: workload.subject,
            }
        });
        let (admin_secret, backend_secret) = match (
            &trusted_workload,
            opts.admin_secret.take(),
            opts.backend_secret.take(),
        ) {
            (None, Some(admin), Some(backend)) => (Some(admin), Some(backend)),
            (Some(_), None, None) => (None, None),
            (None, _, _) => {
                return Err(napi::Error::from_reason(
                    "JazzServer needs adminSecret and backendSecret (or a trustedWorkload)",
                ));
            }
            (Some(_), _, _) => {
                return Err(napi::Error::from_reason(
                    "a trustedWorkload replaces adminSecret and backendSecret; pass neither",
                ));
            }
        };

        let auth_config = AuthConfig {
            backend_secret: backend_secret.clone(),
            admin_secret: admin_secret.clone(),
            trusted_workload,
            ..Default::default()
        };

        let in_memory = opts.in_memory.unwrap_or(false);
        let data_dir = if in_memory {
            String::new()
        } else {
            opts.data_dir.unwrap_or_else(|| "./data".to_string())
        };

        let mut server_builder = ServerBuilder::new(app_id).with_auth_config(auth_config);
        if let Some(max) = opts.max_connections_per_user {
            server_builder = server_builder.with_max_websocket_connections_per_user(max as usize);
        }
        if let Some(schema) = core_server_shell_schema {
            server_builder = server_builder.with_core_server_shell_schema(schema);
        }
        if opts.upstream_url.is_some() {
            return Err(napi::Error::from_reason(
                "server edges are no longer supported; remove upstreamUrl and connect clients directly to Core",
            ));
        }
        server_builder = server_builder.with_storage(if in_memory {
            StorageBackend::InMemory
        } else {
            StorageBackend::Persistent {
                path: data_dir.clone().into(),
            }
        });

        let built = server_builder
            .build()
            .await
            .map_err(napi::Error::from_reason)?;

        let server = CoreJazzServer::from_built(
            built,
            opts.port,
            opts.host,
            app_id,
            ServerDataDir::from_path(std::path::PathBuf::from(&data_dir)),
            admin_secret.unwrap_or_default(),
            backend_secret.unwrap_or_default(),
        )
        .await
        .map_err(napi::Error::from_reason)?;

        let (changed, _) = tokio::sync::watch::channel(());
        Ok(Self {
            lifecycle: Arc::new(JazzServerLifecycle {
                state: Mutex::new(JazzServerState::Running(Box::new(server))),
                changed,
            }),
        })
    }

    #[napi(getter, js_name = "appId")]
    pub fn app_id(&self) -> napi::Result<String> {
        self.with_server(|server| server.app_id().to_string())
    }

    #[napi(getter)]
    pub fn url(&self) -> napi::Result<String> {
        self.with_server(|server| server.base_url())
    }

    #[napi(getter)]
    pub fn port(&self) -> napi::Result<u16> {
        self.with_server(|server| server.port())
    }

    #[napi(getter, js_name = "dataDir")]
    pub fn data_dir(&self) -> napi::Result<String> {
        self.with_server(|server| server.data_dir().to_string_lossy().into_owned())
    }

    #[napi(getter, js_name = "backendSecret")]
    pub fn backend_secret(&self) -> napi::Result<String> {
        self.with_server(|server| server.backend_secret().to_string())
    }

    #[napi(getter, js_name = "adminSecret")]
    pub fn admin_secret(&self) -> napi::Result<String> {
        self.with_server(|server| server.admin_secret().to_string())
    }

    #[napi]
    pub async fn stop(&self) -> napi::Result<()> {
        let mut changes = self.lifecycle.changed.subscribe();
        let shutdown_owner = {
            let mut state = self
                .lifecycle
                .state
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            match &*state {
                JazzServerState::Running(_) => {
                    let JazzServerState::Running(server) =
                        std::mem::replace(&mut *state, JazzServerState::Stopping)
                    else {
                        unreachable!("running state changed while locked")
                    };
                    Some(server)
                }
                JazzServerState::Stopping | JazzServerState::Stopped(_) => None,
            }
        };

        if let Some(server) = shutdown_owner {
            let lifecycle = Arc::clone(&self.lifecycle);
            tokio::spawn(async move {
                let result = std::panic::AssertUnwindSafe(shutdown_jazz_server(*server))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err("JazzServer shutdown task panicked".to_owned()));
                let mut state = lifecycle
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *state = JazzServerState::Stopped(result);
                drop(state);
                lifecycle.changed.send_replace(());
            });
        }

        loop {
            let result = {
                let state = self
                    .lifecycle
                    .state
                    .lock()
                    .map_err(|_| napi::Error::from_reason("lock"))?;
                match &*state {
                    JazzServerState::Running(_) => {
                        return Err(napi::Error::from_reason(
                            "JazzServer shutdown state returned to running",
                        ));
                    }
                    JazzServerState::Stopping => None,
                    JazzServerState::Stopped(result) => Some(result.clone()),
                }
            };
            if let Some(result) = result {
                return result.map_err(napi::Error::from_reason);
            }
            changes.changed().await.map_err(|_| {
                napi::Error::from_reason("JazzServer shutdown state closed unexpectedly")
            })?;
        }
    }

    fn with_server<T>(&self, f: impl FnOnce(&CoreJazzServer) -> T) -> napi::Result<T> {
        let state = self
            .lifecycle
            .state
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        match &*state {
            JazzServerState::Running(server) => Ok(f(server.as_ref())),
            JazzServerState::Stopping => Err(napi::Error::from_reason("JazzServer is stopping")),
            JazzServerState::Stopped(_) => {
                Err(napi::Error::from_reason("JazzServer has been stopped"))
            }
        }
    }
}

async fn shutdown_jazz_server(server: CoreJazzServer) -> std::result::Result<(), String> {
    let phase = server.shutdown().await;
    if phase == jazz_server::ShutdownPhase::StorageClosed {
        Ok(())
    } else {
        Err(format!("JazzServer shutdown failed during phase {phase:?}"))
    }
}

// ============================================================================
// Identity crypto utilities
// ============================================================================

fn decode_seed_napi(seed_b64: &str) -> napi::Result<[u8; 32]> {
    let bytes = URL_SAFE_NO_PAD
        .decode(seed_b64)
        .map_err(|e| napi::Error::from_reason(format!("seed base64 decode error: {e}")))?;
    bytes
        .try_into()
        .map_err(|_| napi::Error::from_reason("seed must be exactly 32 bytes"))
}

#[napi(js_name = "mintLocalFirstToken")]
pub fn mint_local_first_token(
    seed_b64: String,
    audience: String,
    ttl_seconds: u32,
) -> napi::Result<String> {
    let seed = decode_seed_napi(&seed_b64)?;
    identity::mint_jazz_self_signed_token(
        &seed,
        identity::DEVICE_ACCOUNT_ISSUER,
        &audience,
        ttl_seconds as u64,
    )
    .map_err(napi::Error::from_reason)
}

#[napi(object)]
pub struct VerifyTokenResult {
    pub ok: bool,
    pub id: String,
    pub error: Option<String>,
}

#[napi(js_name = "verifyLocalFirstIdentityProof")]
pub fn verify_local_first_identity_proof_napi(
    token: Option<String>,
    expected_audience: String,
) -> VerifyTokenResult {
    let token = match token {
        Some(t) if !t.is_empty() => t,
        _ => {
            return VerifyTokenResult {
                ok: false,
                id: String::new(),
                error: Some("proofToken is required".to_string()),
            };
        }
    };
    match identity::verify_jazz_self_signed_proof(&token, &expected_audience) {
        Ok(verified) => VerifyTokenResult {
            ok: true,
            id: verified.user_id,
            error: None,
        },
        Err(e) => VerifyTokenResult {
            ok: false,
            id: String::new(),
            error: Some(e),
        },
    }
}

#[cfg(test)]
mod native_root_tests {
    use super::names_foreign_store;

    #[test]
    fn jazz_sqlite_files_and_unrelated_strays_are_admitted() {
        for name in [
            "jazz.sqlite",
            "jazz.sqlite-wal",
            "jazz.sqlite-shm",
            "jazz.sqlite-journal",
            ".DS_Store",
            "notes.txt",
            "jazz.sqlite.bak",
            "application.data",
        ] {
            assert!(!names_foreign_store(name), "{name} should be admitted");
        }
    }

    #[test]
    fn other_sqlite_and_rocksdb_files_are_refused() {
        for name in [
            "other.sqlite",
            "other.SQLITE3",
            "app.db",
            "app.db3",
            "app.db-wal",
            "app.db-shm",
            "app.db-journal",
            "jazz.sqlite.bak-wal",
            "CURRENT",
            "LOCK",
            "IDENTITY",
            "MANIFEST-000001",
            "OPTIONS-000005",
            "000003.sst",
        ] {
            assert!(names_foreign_store(name), "{name} should be refused");
        }
    }
}

#[cfg(all(test, feature = "rn-test-bridge"))]
mod encryption_tests {
    use super::*;
    use jazz::binding_host::{OpenConfig, OpenIdentity};
    use jazz::ids::{AuthorSubject, NodeUuid};

    /// Alice's malformed device secret is refused at binding construction, before
    /// her application declarations or private store are read.
    #[test]
    fn device_secret_is_checked_before_readiness() {
        let result = EncryptionRuntime::from_bindings(
            "invalid app".into(),
            "test".into(),
            "test".into(),
            "alice".into(),
            Some("jazz-auth-v1:short".into()),
        );
        assert!(result.is_err_and(|error| error.reason == "invalid device account secret"));
    }

    /// A build whose store never answers refuses a second `ready` as still
    /// running, not as built, and frees the slot once it is abandoned.
    #[test]
    fn encryption_startup_refuses_a_second_ready_while_the_store_is_silent() {
        let app =
            include_bytes!("../../../packages/jazz-tools/tests/fixtures/encrypted/jazz.app.json");
        let alice = AuthorSubject::for_test_bytes([1; 16]);
        let runtime = EncryptionRuntime::from_bindings(
            jazz_codegen::compile_app(app).unwrap(),
            "test".into(),
            "test".into(),
            alice.account_id().unwrap().0.to_string(),
            None,
        )
        .unwrap();
        let mut context = Context::from_waker(Waker::noop());
        let mut silent = runtime.builder.start_build().ok().unwrap();
        assert!(silent.as_mut().poll(&mut context).is_pending());
        assert!(!runtime.take_store_requests().is_empty());
        assert_eq!(
            runtime.builder.start_build().err(),
            Some(encryption::STILL_BUILDING)
        );
        drop(silent);
        assert!(
            runtime.builder.start_build().is_ok(),
            "an abandoned build frees the slot"
        );
    }

    /// Alice can retry readiness after her private store refuses a request,
    /// and Bob's refused open leaves the built runtime available to Alice.
    /// Rust drives the same build future as `ready` because its JS promise
    /// requires a Node environment.
    #[test]
    fn encryption_startup_can_retry_after_store_failure_and_account_mismatch() {
        let app =
            include_bytes!("../../../packages/jazz-tools/tests/fixtures/encrypted/jazz.app.json");
        let definition = jazz::tools::deployment::artifact::load_app(app).unwrap();
        let schema = serde_json::to_vec(definition.current_schema()).unwrap();
        let alice = AuthorSubject::for_test_bytes([1; 16]);
        let bob = AuthorSubject::for_test_bytes([2; 16]);
        let runtime = EncryptionRuntime::from_bindings(
            jazz_codegen::compile_app(app).unwrap(),
            "test".into(),
            "test".into(),
            alice.account_id().unwrap().0.to_string(),
            Some(
                jazz::identity::DeviceAccountSecret::from_bytes([1; 32])
                    .format()
                    .to_string(),
            ),
        )
        .unwrap();
        let mut context = Context::from_waker(Waker::noop());
        let mut failed = runtime.builder.build_future().unwrap();
        assert!(failed.as_mut().poll(&mut context).is_pending());
        assert!(
            runtime.builder.build_future().is_none(),
            "build in progress"
        );
        let id = |request: &str| {
            serde_json::from_str::<JsonValue>(request).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_owned()
        };
        let requests = runtime.take_store_requests();
        assert_eq!(requests.len(), 1);
        assert!(runtime.reply_store(id(&requests[0]), false, "store unavailable".into()));
        let Poll::Ready(Err(failure)) = failed.as_mut().poll(&mut context) else {
            panic!("readiness should report the failed store request");
        };
        assert!(failure.contains("store unavailable"), "{failure}");
        let store =
            jazz_e2ee_client::host::bridge::serve(Rc::new(jazz_e2ee_client::MemoryStore::new()));
        let mut retry = runtime
            .builder
            .build_future()
            .expect("readiness can be retried");
        loop {
            if let Poll::Ready(result) = retry.as_mut().poll(&mut context) {
                result.expect("the recovered store builds the runtime");
                break;
            }
            let requests = runtime.take_store_requests();
            assert!(!requests.is_empty());
            for request in requests {
                let request_id = id(&request);
                let (ok, payload) = block_on(store(request));
                assert!(ok, "{payload}");
                assert!(runtime.reply_store(request_id, ok, payload));
            }
        }
        assert!(runtime.builder.build_future().is_none(), "build completed");

        let open = |author| {
            let config = postcard::to_allocvec(&OpenConfig {
                identity: OpenIdentity {
                    node: NodeUuid::from_bytes([3; 16]),
                    author,
                },
                row_id_seed: None,
                history_complete: false,
                initial_sync_flush_every: None,
                migrations: None,
                system_node: None,
            })
            .unwrap();
            NapiDb::open_with(
                &schema,
                &config,
                None,
                OpenAs::Untrusted,
                Some(Hooks::Client(&runtime)),
            )
        };
        let failure = open(bob).err().expect("Bob cannot open Alice's runtime");
        assert!(failure.reason.contains("for another account"), "{failure}");
        let owner = open(alice).expect("Alice can still open her runtime");
        assert!(owner.encryption().is_ok());
        let failure = open(alice)
            .err()
            .expect("the runtime is attached only once");
        assert!(failure.reason.contains("not ready"), "{failure}");
    }
}

#[cfg(test)]
mod subscription_pool_tests {
    use super::{CoreEvent, EventSource, drain_streams};
    use std::collections::{BTreeMap, VecDeque};

    struct Scripted(VecDeque<CoreEvent>);

    impl EventSource for Scripted {
        fn try_next(&mut self) -> Option<CoreEvent> {
            self.0.pop_front()
        }
    }

    fn rejected(reason: &str) -> CoreEvent {
        CoreEvent::Rejected {
            code: jazz::db::ErrorCode::Query,
            message: reason.to_owned(),
            reason: reason.to_owned(),
        }
    }

    fn scripted(events: Vec<CoreEvent>) -> Scripted {
        Scripted(events.into())
    }

    /// Encodes an event as its reason; "unencodable" fails to encode.
    fn encode(event: CoreEvent) -> napi::Result<String> {
        match event {
            CoreEvent::Rejected { reason, .. } if reason == "unencodable" => {
                Err(napi::Error::from_reason("encode failed"))
            }
            CoreEvent::Rejected { reason, .. } => Ok(reason),
            CoreEvent::Closed => Ok("closed".to_owned()),
            CoreEvent::Delta { .. } => Ok("delta".to_owned()),
        }
    }

    #[test]
    fn drain_returns_only_streams_with_events_in_id_order() {
        let mut streams = BTreeMap::from([
            (1, scripted(vec![])),
            (2, scripted(vec![rejected("a"), rejected("b")])),
            (3, scripted(vec![])),
            (4, scripted(vec![rejected("c")])),
        ]);
        let drained = drain_streams(&mut streams, encode);
        let got: Vec<_> = drained
            .iter()
            .map(|(id, read)| (*id, read.events.clone()))
            .collect();
        assert_eq!(
            got,
            vec![(2, vec!["a".into(), "b".into()]), (4, vec!["c".into()])]
        );
        assert_eq!(streams.len(), 4, "open streams stay pooled");
        assert!(drain_streams(&mut streams, encode).is_empty());
    }

    #[test]
    fn an_encode_error_fails_only_its_own_stream() {
        let mut streams = BTreeMap::from([
            (
                1,
                scripted(vec![
                    rejected("before"),
                    rejected("unencodable"),
                    rejected("after"),
                ]),
            ),
            (2, scripted(vec![rejected("neighbour")])),
        ]);
        let drained = drain_streams(&mut streams, encode);
        assert_eq!(drained.len(), 2);
        let (id, failed) = &drained[0];
        assert_eq!(*id, 1);
        assert_eq!(failed.events, vec!["before".to_owned()]);
        assert_eq!(failed.error.as_ref().unwrap().reason, "encode failed");
        let (id, ok) = &drained[1];
        assert_eq!(*id, 2);
        assert_eq!(ok.events, vec!["neighbour".to_owned()]);
        assert!(ok.error.is_none());
        assert_eq!(
            streams.keys().copied().collect::<Vec<_>>(),
            vec![2],
            "the failed stream leaves"
        );
    }

    #[test]
    fn a_closed_stream_leaves_after_its_last_batch() {
        let mut streams =
            BTreeMap::from([(7, scripted(vec![rejected("last"), CoreEvent::Closed]))]);
        let drained = drain_streams(&mut streams, encode);
        assert_eq!(
            drained[0].1.events,
            vec!["last".to_owned(), "closed".to_owned()]
        );
        assert!(drained[0].1.closed);
        assert!(streams.is_empty());
    }
}

/// One thin connection's frames split between the core and the session's
/// backend, in the client's order (SPEC 8.14; `jazz::thin_transactions::
/// ThinRouter`). Messages are WebSocket messages (frame batches).
#[napi]
pub struct NapiThinRouter {
    router: RefCell<jazz::thin_transactions::ThinRouter>,
}

/// One step a [`NapiThinRouter`] releases: a message for the core, or a
/// request frame for the backend's client (`serveThinTransaction`).
#[napi(object)]
pub struct ThinStep {
    pub core: Option<Uint8Array>,
    pub backend: Option<Uint8Array>,
    /// The backend request's id.
    pub id: Option<f64>,
    /// Whether the backend request is a commit.
    pub commit: Option<bool>,
}

#[napi]
impl NapiThinRouter {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            router: RefCell::default(),
        }
    }

    /// Take one client message. Throws when it does not decode: the
    /// connection is broken.
    #[napi(js_name = "fromClient")]
    pub fn from_client(&self, message: Uint8Array) -> napi::Result<()> {
        let frames = jazz::wire::decode_websocket_frame_batch(&message)
            .map_err(|error| napi::Error::from_reason(format!("undecodable message: {error}")))?;
        self.router
            .borrow_mut()
            .from_client(frames)
            .map_err(napi::Error::from_reason)
    }

    /// Note one message the core sent the client: whether it answered a
    /// request sent to the core.
    #[napi(js_name = "fromCore")]
    pub fn from_core(&self, message: Uint8Array) -> bool {
        jazz::wire::decode_websocket_frame_batch(&message)
            .is_ok_and(|frames| self.router.borrow_mut().from_core(&frames))
    }

    /// Note the backend's answer (a response frame, as
    /// `serveThinTransaction` gives it), and return the messages that carry
    /// it to the client.
    #[napi]
    pub fn answer(&self, answer: Uint8Array) -> napi::Result<Vec<Uint8Array>> {
        let Ok(jazz::wire::WireFrame::ThinResponse(answer)) =
            jazz::wire::decode_postcard_frame(&answer)
        else {
            return Err(napi::Error::from_reason(
                "answer takes a thin response frame",
            ));
        };
        self.router.borrow_mut().answered(&answer);
        jazz::thin_transactions::answer_frames(answer)
            .into_iter()
            .map(|frame| {
                jazz::wire::encode_websocket_frame_batch(&[frame])
                    .map(Uint8Array::from)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))
            })
            .collect()
    }

    /// Answer backend request `id` with an error the backend gave in its
    /// place (`code`, `message`): a request it stopped waiting for, or one
    /// it could not serve. Returns the messages that carry it to the client.
    #[napi]
    pub fn refuse(&self, id: f64, code: String, message: String) -> napi::Result<Vec<Uint8Array>> {
        let frame = jazz::wire::encode_frame(&jazz::wire::WireFrame::ThinResponse(
            jazz::thin::ThinResponse {
                id: id as u64,
                body: jazz::thin::ThinBody::Error { code, message },
            },
        ))
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        self.answer(frame.into())
    }

    /// Whether a client's Hello (its first message after the prelude) asks
    /// for thin results: a thin client, whose transactions its backend
    /// serves.
    #[napi(js_name = "asksThinResults")]
    pub fn asks_thin_results(message: Uint8Array) -> bool {
        let Ok(frames) = jazz::wire::decode_websocket_frame_batch(&message) else {
            return false;
        };
        frames.first().is_some_and(|frame| {
            matches!(
                jazz::wire::decode_frame(frame),
                Ok(jazz::wire::WireFrame::Hello(hello))
                    if hello.features & jazz::wire::FEATURE_THIN_RESULTS != 0
            )
        })
    }

    /// What may go now, in order.
    #[napi]
    pub fn ready(&self) -> napi::Result<Vec<ThinStep>> {
        let mut steps = Vec::new();
        for step in self.router.borrow_mut().ready() {
            match step {
                jazz::thin_transactions::Step::ToCore(frames) => {
                    for frame in frames {
                        let message = jazz::wire::encode_websocket_frame_batch(&[frame])
                            .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                        steps.push(ThinStep {
                            core: Some(message.into()),
                            backend: None,
                            id: None,
                            commit: None,
                        });
                    }
                }
                jazz::thin_transactions::Step::ToBackend(request) => {
                    let id = request.id as f64;
                    let commit = matches!(request.op, jazz::thin::ThinOp::Commit { .. });
                    let frame =
                        jazz::wire::encode_frame(&jazz::wire::WireFrame::ThinRequest(request))
                            .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                    steps.push(ThinStep {
                        core: None,
                        backend: Some(frame.into()),
                        id: Some(id),
                        commit: Some(commit),
                    });
                }
            }
        }
        Ok(steps)
    }

    /// The public schema JSON the client bound the connection to, once it
    /// did.
    #[napi]
    pub fn schema(&self) -> Option<Uint8Array> {
        self.router
            .borrow()
            .schema()
            .map(|schema| schema.to_vec().into())
    }

    /// Whether something routed waits on an answer that has not come.
    #[napi(js_name = "isWaiting")]
    pub fn is_waiting(&self) -> bool {
        self.router.borrow().is_waiting()
    }
}

impl Default for NapiThinRouter {
    fn default() -> Self {
        Self::new()
    }
}
