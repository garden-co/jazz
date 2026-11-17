//! Jazz CLI - Create apps and run servers.
//!
//! # Commands
//!
//! ```text
//! jazz-tools create app [--name <NAME>]    # Returns AppId (random or deterministic from name)
//! jazz-tools server <APP_ID> [--port 1625] [--data-dir ./data] [--in-memory]
//! ```

// The global allocator is a per-binary choice; library code in `jazz-tools`
// does not declare one so that consumers (jazz-napi, todo-server, third-party
// embedders) keep theirs. mimalloc replaces the system allocator for
// throughput on the server's allocation-heavy paths (query/insert/observer).
//
// With the `heap-profiling` feature on Linux, where production servers run,
// a sampling wrapper records a stack for about one allocation per 512 KiB
// (`JAZZ_HEAP_PROFILE_SAMPLE_BYTES`; `0` turns it off), so operators can see
// which code holds memory (see `jazz_cli::heap_profiling`).
#[cfg(heap_profiling)]
#[global_allocator]
static GLOBAL: jazz_cli::heap_profiling::SamplingAllocator<mimalloc::MiMalloc> =
    jazz_cli::heap_profiling::SamplingAllocator::new(mimalloc::MiMalloc);

#[cfg(not(heap_profiling))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use clap::{Parser, Subcommand};
use jazz::tools::AppId;
#[cfg(feature = "otel")]
use jazz_otel as otel;
use jazz_server::AuthConfig;
use jazz_server::profiling::DiagnosticsConfig;

const DEFAULT_SHUTDOWN_TIMEOUT_SECS: u64 = 30;
const MAX_SHUTDOWN_TIMEOUT_SECS: u64 = 60 * 60;

fn parse_app_id(value: &str) -> Result<String, String> {
    jazz::tools::AppId::from_string(value).map_err(|_| {
        "Application ID must be a UUID, not an app name. Generate one with `jazz-tools create app` and reuse it for the server and client.".to_owned()
    })?;
    Ok(value.to_owned())
}

fn parse_shutdown_timeout_secs(value: &str) -> Result<u64, String> {
    let seconds = value
        .parse::<u64>()
        .map_err(|error| format!("invalid shutdown timeout: {error}"))?;

    if !(1..=MAX_SHUTDOWN_TIMEOUT_SECS).contains(&seconds) {
        return Err(format!(
            "shutdown timeout must be between 1 and {MAX_SHUTDOWN_TIMEOUT_SECS} seconds"
        ));
    }

    Ok(seconds)
}

#[derive(Parser)]
#[command(name = "jazz-tools")]
#[command(bin_name = "jazz-tools")]
#[command(about = "Jazz distributed database CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Commands {
    /// With --check (for CI): check the committed jazz.app.json (and
    /// jazz.permissions.json) and fail when the bindings are stale, changing
    /// nothing. Writing them is `npx jazz-tools generate`'s, from schema.ts.
    Generate {
        /// The directory holding jazz.app.json.
        #[arg(long, default_value = ".")]
        dir: std::path::PathBuf,
        /// The bindings' language (repeatable); by default each whose
        /// marker (package.json, Cargo.toml, Package.swift, build.gradle(.kts), go.mod) sits beside jazz.app.json.
        #[arg(long)]
        lang: Vec<String>,
        /// Fail when the files or bindings are stale; change nothing.
        #[arg(long)]
        check: bool,
    },
    /// Renamed to `generate`.
    #[command(hide = true)]
    Validate {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Migrations are written from schema.ts by `npx jazz-tools migrations
    /// create`; this binary refuses.
    Migrations {
        #[command(subcommand)]
        action: MigrationsAction,
    },
    /// Create a new resource
    Create {
        #[command(subcommand)]
        resource: CreateResource,
    },
    /// Run a Jazz server
    Server {
        /// Application UUID (from `jazz-tools create app`)
        #[arg(value_parser = parse_app_id)]
        app_id: String,

        /// Port to listen on
        #[arg(short, long, default_value = "1625")]
        port: u16,

        /// Data directory for persistent storage (ignored if --in-memory)
        #[arg(short, long, default_value = "./data")]
        data_dir: String,

        /// Use in-memory storage instead of persistent files.
        #[arg(long)]
        in_memory: bool,

        /// Where large values' bytes live once they settle: `files` (the
        /// `blobs/` directory next to the data, or --blobs-dir), `s3` (an
        /// S3-compatible bucket, see --s3-*), `vercel-blob` (a Vercel Blob
        /// store, see --vercel-blob-*) or `sqlite` (inside the database
        /// file). A data directory keeps its choice, except that `sqlite`
        /// may later move to any other.
        #[arg(long, default_value = "files", value_parser = ["files", "s3", "vercel-blob", "sqlite"])]
        blobs: String,

        /// Directory for `--blobs files` (defaults to `<data-dir>/blobs`).
        #[arg(long)]
        blobs_dir: Option<String>,

        /// Bucket for `--blobs s3` (defaults to S3_BUCKET_NAME). Credentials
        /// come, as in the AWS SDKs, from AWS_ACCESS_KEY_ID and
        /// AWS_SECRET_ACCESS_KEY, AWS_ROLE_ARN with an OIDC token, the
        /// container's role (ECS, Fargate, EKS Pod Identity), or the EC2
        /// instance profile.
        #[arg(long)]
        s3_bucket: Option<String>,

        /// The bucket's region (defaults to AWS_REGION; `auto` for R2).
        #[arg(long)]
        s3_region: Option<String>,

        /// An S3-compatible endpoint other than AWS, e.g. R2's.
        #[arg(long)]
        s3_endpoint: Option<String>,

        /// Prefix for every object name; defaults to jazz/<store id>/,
        /// one per data dir. A prefix is claimed by the first store to use it.
        #[arg(long)]
        s3_prefix: Option<String>,

        /// Assume AWS_ROLE_ARN with the trusted workload's own OIDC token
        /// (as a `vercel()` core does), not AWS_WEB_IDENTITY_TOKEN_FILE.
        #[arg(long)]
        s3_role_from_workload: bool,

        /// Store for `--blobs vercel-blob` (defaults to BLOB_STORE_ID, or the
        /// store BLOB_READ_WRITE_TOKEN is for). Credentials come from
        /// VERCEL_OIDC_TOKEN, or BLOB_READ_WRITE_TOKEN.
        #[arg(long)]
        vercel_blob_store_id: Option<String>,

        /// Prefix for every blob's pathname; defaults to
        /// jazz/<store id>/, one per data dir.
        #[arg(long)]
        vercel_blob_prefix: Option<String>,

        /// Authenticate to Vercel Blob with the trusted workload's own OIDC
        /// token (as a `vercel()` core does), not VERCEL_OIDC_TOKEN.
        #[arg(long)]
        vercel_blob_token_from_workload: bool,

        /// The deployment environment (production, preview, …) this server
        /// runs in. With `--blobs s3` or `--blobs vercel-blob`, the bucket or
        /// Blob store is claimed for it, and a server of another environment
        /// is refused there (a `vercel()` core passes it).
        #[arg(long)]
        blobs_environment: Option<String>,

        /// Trust only this OIDC workload instead of shared secrets: every
        /// request and session must carry its token in
        /// X-Jazz-Backend-Credential, and states its access in
        /// X-Jazz-Backend-Access. Needs the subject and audience flags too;
        /// replaces the backend and admin secrets. Graceful shutdown then
        /// goes through SIGTERM.
        #[arg(
            long,
            env = "JAZZ_TRUSTED_WORKLOAD_ISSUER",
            requires_all = ["trusted_workload_audience", "trusted_workload_subject"],
            conflicts_with_all = ["backend_secret", "admin_secret"]
        )]
        trusted_workload_issuer: Option<String>,

        /// Audience a trusted workload token must target.
        #[arg(
            long,
            env = "JAZZ_TRUSTED_WORKLOAD_AUDIENCE",
            requires = "trusted_workload_issuer"
        )]
        trusted_workload_audience: Option<String>,

        /// Exact subject a trusted workload token must carry.
        #[arg(
            long,
            env = "JAZZ_TRUSTED_WORKLOAD_SUBJECT",
            requires = "trusted_workload_issuer"
        )]
        trusted_workload_subject: Option<String>,

        /// Secret for backend session impersonation
        #[arg(long, env = "JAZZ_BACKEND_SECRET")]
        backend_secret: Option<String>,

        /// Secret for admin operations (schema/policy sync)
        #[arg(long, env = "JAZZ_ADMIN_SECRET")]
        admin_secret: Option<String>,

        /// Graceful shutdown network-drain timeout in seconds.
        #[arg(
            long,
            env = "JAZZ_SHUTDOWN_TIMEOUT_SECS",
            default_value_t = DEFAULT_SHUTDOWN_TIMEOUT_SECS,
            value_parser = parse_shutdown_timeout_secs,
        )]
        shutdown_timeout_secs: u64,

        /// Maximum live WebSocket connections. Upgrades beyond it get
        /// `503 Service Unavailable` with `Retry-After`.
        #[arg(
            long,
            env = "JAZZ_MAX_CONNECTIONS",
            default_value_t = jazz_server::server::DEFAULT_MAX_WEBSOCKET_CONNECTIONS,
            value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..),
        )]
        max_connections: usize,

        /// Maximum live WebSocket connections of one signed-in user. Past
        /// it, a connection silent for longer than a keepalive is replaced;
        /// otherwise the new one is refused with a retryable error.
        #[arg(
            long,
            env = "JAZZ_MAX_CONNECTIONS_PER_USER",
            default_value_t = jazz_server::server::DEFAULT_MAX_WEBSOCKET_CONNECTIONS_PER_IDENTITY,
            value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..),
        )]
        max_connections_per_user: usize,

        /// Memory the server sizes its caches against, in bytes. Defaults to
        /// the container's cgroup memory limit.
        #[arg(long, env = "JAZZ_MEMORY_LIMIT_BYTES", value_parser = clap::value_parser!(u64).range(1..))]
        memory_limit_bytes: Option<u64>,

        /// Read the query indexes of stored tables from storage by key,
        /// keeping only a bounded cache of them in memory. `0` holds them
        /// all in memory instead.
        #[arg(
            long,
            env = "JAZZ_STORED_ARRANGEMENTS",
            default_value_t = true,
            action = clap::ArgAction::Set,
            value_parser = clap::builder::BoolishValueParser::new(),
        )]
        stored_arrangements: bool,

        /// Bytes the query cache may keep while memory is plentiful.
        /// Defaults to a quarter of the memory limit; it is lowered under
        /// memory pressure either way. `0` keeps nothing resident, so every
        /// query reads the indexes of stored tables from storage.
        #[arg(long, env = "JAZZ_QUERY_CACHE_BUDGET_BYTES", value_parser = clap::value_parser!(u64))]
        query_cache_budget_bytes: Option<u64>,

        /// Internal testing hook: write the resolved listen port after binding.
        #[arg(long, env = "JAZZ_BOUND_PORT_FILE", hide = true)]
        bound_port_file: Option<String>,

        /// Address for a diagnostics listener serving `/debug/pprof/heap`,
        /// e.g. `0.0.0.0:6060`.
        ///
        /// Non-loopback addresses require `--diagnostics-token`. The same
        /// profile is served on the main port with the admin secret.
        #[arg(long, env = "JAZZ_DIAGNOSTICS_LISTEN")]
        diagnostics_listen: Option<std::net::SocketAddr>,

        /// Bearer token the diagnostics listener requires
        /// (`Authorization: Bearer <TOKEN>`). A read-only scrape credential
        /// that can be shared across servers without their admin secrets.
        #[arg(long, env = "JAZZ_DIAGNOSTICS_TOKEN", requires = "diagnostics_listen")]
        diagnostics_token: Option<String>,
    },
}

#[derive(Subcommand)]
enum MigrationsAction {
    /// Refused: run `npx jazz-tools migrations create --name <name>`, which
    /// writes the migration from schema.ts.
    Create {
        /// The migration's name.
        #[arg(long)]
        name: String,
        /// The directory holding jazz.app.json.
        #[arg(long, default_value = ".")]
        dir: std::path::PathBuf,
        /// The bindings' language (repeatable); by default each whose
        /// marker (package.json, Cargo.toml, Package.swift, build.gradle(.kts), go.mod) sits beside jazz.app.json.
        #[arg(long)]
        lang: Vec<String>,
    },
}

/// Print a command's outcome and exit 0, or its refusal and exit 1.
fn finish_app_files(result: Result<jazz_cli::app_files::Outcome, String>) -> ! {
    let code = match result {
        Ok(outcome) => {
            for warning in outcome.warnings {
                eprintln!("warning: {warning}");
            }
            for note in outcome.notes {
                println!("{note}");
            }
            0
        }
        Err(error) => {
            eprintln!("error: {error}");
            1
        }
    };
    std::process::exit(code)
}

/// Where a TS app keeps its `schema.ts`, relative to the app's directory
/// (jazz-tools' `SCHEMA_ROOTS`, `packages/jazz-tools/src/upgrade/project.ts`).
const SCHEMA_ROOTS: &[&str] = &[".", "src", "src/lib"];

/// The app's `schema.ts`, when it is authored in TypeScript.
fn ts_schema(dir: &std::path::Path) -> Option<std::path::PathBuf> {
    SCHEMA_ROOTS
        .iter()
        .map(|root| dir.join(root).join("schema.ts"))
        .find(|path| path.is_file())
}

/// The refusal of `jazz.app.json`-only apps (alpha.66 makes TypeScript the
/// only authoring source): shared with `npx jazz-tools`
/// (`packages/jazz-tools/src/dev/ts-project.ts`).
const JSON_ONLY_APP: &str = "TS is the source in alpha.66: this app has jazz.app.json but no \
     schema.ts. Run `npx jazz-tools upgrade` to write schema.ts from it (it keeps the schema \
     hashes and bundle id), then edit schema.ts beside your app's manifest (package.json, \
     Cargo.toml, go.mod, Package.swift, build.gradle, …) and run `npx jazz-tools generate`.";

/// Refuse to write an app's files: TypeScript is the only authoring source,
/// so `jazz.app.json`, `jazz.permissions.json` and the bindings are written
/// from `schema.ts` by `npx jazz-tools generate` (and migrations by `npx
/// jazz-tools migrations create`), which this binary can't run. An app
/// with `jazz.app.json` but no `schema.ts` is pointed at `npx jazz-tools
/// upgrade`, which writes its `schema.ts`.
fn refuse_writing(dir: &std::path::Path, command: &str) -> String {
    if let Some(schema) = ts_schema(dir) {
        return format!(
            "{} exists: this app is authored in TypeScript, and its jazz.app.json and \
             jazz.permissions.json are generated from it. Run `npx jazz-tools {command}` instead \
             (the standalone binary can't read TypeScript)",
            schema.display()
        );
    }
    if dir
        .join(jazz::tools::deployment::artifact::APP_FILE)
        .is_file()
    {
        return JSON_ONLY_APP.to_owned();
    }
    format!(
        "no schema.ts in {} (looked in {}): write the app's schema in schema.ts and run `npx \
         jazz-tools {command}` (the standalone binary only checks committed files, with \
         `generate --check`)",
        dir.display(),
        SCHEMA_ROOTS.join(", ")
    )
}

/// `generate --check` in `dir`: checks the committed files (their validity
/// and, when written from TypeScript, their `$generated` digests) and the
/// bindings against them, but can't check that the files are fresh from
/// the TypeScript. Without `--check` it refuses: see [`refuse_writing`].
fn generate(
    dir: &std::path::Path,
    names: &[String],
    check: bool,
) -> Result<jazz_cli::app_files::Outcome, String> {
    if !check {
        return Err(refuse_writing(dir, "generate"));
    }
    let ts = ts_schema(dir);
    if ts.is_some() {
        check_generated_digests(dir)?;
    }
    let mut outcome = jazz_cli::app_files::generate(dir, &langs(dir, names)?, check)?;
    if let Some(schema) = ts {
        outcome.notes.push(format!(
            "note: {} exists: checked the bindings against the committed jazz.app.json and \
             jazz.permissions.json, not that those are fresh from the TypeScript; run `npx \
             jazz-tools generate --check` for that",
            schema.display()
        ));
    }
    Ok(outcome)
}

/// Refuse a committed file whose `$generated` digest no longer matches: it
/// was edited by hand since `npx jazz-tools generate` wrote it.
fn check_generated_digests(dir: &std::path::Path) -> Result<(), String> {
    use jazz::tools::deployment::artifact::{self, GeneratedCheck};
    for file in [artifact::APP_FILE, artifact::PERMISSIONS_FILE] {
        let path = dir.join(file);
        let Ok(text) = std::fs::read(&path) else {
            continue;
        };
        match artifact::verify_generated(&text).map_err(|error| format!("{file}: {error}"))? {
            GeneratedCheck::Unmarked | GeneratedCheck::Matches(_) => {}
            GeneratedCheck::Mismatch { .. } => {
                return Err(format!(
                    "{file} was edited since `npx jazz-tools generate` wrote it (its \
                     `$generated` digest doesn't match): edit the TypeScript and run `npx \
                     jazz-tools generate`"
                ));
            }
        }
    }
    Ok(())
}

fn langs(
    dir: &std::path::Path,
    names: &[String],
) -> Result<Vec<jazz_cli::app_files::Selected>, String> {
    let given = names
        .iter()
        .map(|name| jazz_codegen::Lang::parse(name).map_err(|error| error.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    jazz_cli::app_files::languages(dir, &given)
}

#[derive(Subcommand)]
enum CreateResource {
    /// Create a new application
    App {
        /// Optional name for deterministic ID generation
        #[arg(short, long)]
        name: Option<String>,
    },
}

fn main() {
    // Before the runtime starts its worker threads: only threads that start
    // after sampling is turned on sample.
    #[cfg(heap_profiling)]
    jazz_cli::heap_profiling::configure();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to start the async runtime")
        .block_on(run())
}

async fn run() {
    let cli = Cli::parse();
    init_tracing();
    if let Err(error) = validate_server_cli_options(&cli.command) {
        eprintln!("Server error: {error}");
        shutdown_tracing();
        std::process::exit(1);
    }

    match cli.command {
        Commands::Validate { .. } => finish_app_files(Err(
            "`jazz-tools validate` is renamed to `jazz-tools generate`: run `npx jazz-tools generate` (`jazz-tools generate --check` for CI)".into(),
        )),
        Commands::Generate {
            dir,
            lang: name,
            check,
        } => finish_app_files(generate(&dir, &name, check)),
        Commands::Migrations {
            action: MigrationsAction::Create { dir, .. },
        } => finish_app_files(Err(refuse_writing(&dir, "migrations create"))),
        Commands::Create { resource } => match resource {
            CreateResource::App { name } => {
                let app_id = name.map_or_else(AppId::random, |name| AppId::from_name(&name));
                println!("{app_id}");
            }
        },
        Commands::Server {
            app_id,
            port,
            data_dir,
            in_memory,
            blobs,
            blobs_dir,
            s3_bucket,
            s3_region,
            s3_endpoint,
            s3_prefix,
            s3_role_from_workload,
            vercel_blob_store_id,
            vercel_blob_prefix,
            vercel_blob_token_from_workload,
            blobs_environment,
            backend_secret,
            admin_secret,
            trusted_workload_issuer,
            trusted_workload_audience,
            trusted_workload_subject,
            shutdown_timeout_secs,
            max_connections,
            max_connections_per_user,
            memory_limit_bytes,
            stored_arrangements,
            query_cache_budget_bytes,
            bound_port_file,
            diagnostics_listen,
            diagnostics_token,
        } => {
            let auth_config = AuthConfig {
                backend_secret,
                admin_secret,
                trusted_workload: trusted_workload_issuer.map(|issuer| {
                    jazz_server::middleware::workload::TrustedWorkload {
                        issuer,
                        audience: trusted_workload_audience.unwrap_or_default(),
                        subject: trusted_workload_subject.unwrap_or_default(),
                    }
                }),
                ..Default::default()
            };
            let blobs = match (jazz_server::BlobOptions {
                kind: blobs,
                dir: blobs_dir,
                s3_bucket,
                s3_region,
                s3_endpoint,
                s3_prefix,
                s3_role_from_workload,
                vercel_blob_store_id,
                vercel_blob_prefix,
                vercel_blob_token_from_workload,
                environment: blobs_environment,
            })
            .resolve()
            {
                Ok(blobs) => blobs,
                Err(error) => {
                    eprintln!("Error: {error}");
                    std::process::exit(1);
                }
            };
            if let Err(e) = jazz_server::run(
                &app_id,
                port,
                &data_dir,
                in_memory,
                blobs,
                auth_config,
                bound_port_file,
                std::time::Duration::from_secs(shutdown_timeout_secs),
                DiagnosticsConfig {
                    #[cfg(heap_profiling)]
                    heap_profiler: Some(jazz_cli::heap_profiling::activate()),
                    #[cfg(not(heap_profiling))]
                    heap_profiler: None,
                    listen: diagnostics_listen,
                    token: diagnostics_token,
                },
                jazz_server::resources::ServerResourceLimits {
                    max_websocket_connections: max_connections,
                    max_websocket_connections_per_user: max_connections_per_user,
                    memory_limit_bytes: memory_limit_bytes
                        .or_else(jazz_server::resources::detect_memory_limit_bytes),
                    stored_arrangements,
                    query_cache_budget_bytes,
                },
            )
            .await
            {
                eprintln!("Server error: {}", e);
                shutdown_tracing();
                std::process::exit(1);
            }
            shutdown_tracing();
        }
    }
}

fn validate_removed_edge_environment(
    upstream_configured: bool,
    cache_configured: bool,
) -> Result<(), String> {
    if upstream_configured || cache_configured {
        return Err("Semantic edge servers are no longer supported. Remove JAZZ_UPSTREAM_URL and JAZZ_EDGE_CACHE_BUDGET_BYTES only if this process is intended to be Core; otherwise connect clients directly to the existing Core server.".to_owned());
    }
    Ok(())
}

/// User-auth settings a core no longer reads: the app's backend verifies its
/// users itself and opens their sessions with its backend secret.
const REMOVED_USER_AUTH_ENV: [&str; 6] = [
    "JAZZ_JWKS_URL",
    "JAZZ_JWT_PUBLIC_KEY",
    "JAZZ_JWT_ISSUER",
    "JAZZ_JWT_AUDIENCE",
    "JAZZ_AUTH_COOKIE_NAME",
    "JAZZ_TRUST_PROXY",
];

fn validate_server_cli_options(command: &Commands) -> Result<(), String> {
    let Commands::Server { .. } = command else {
        return Ok(());
    };

    validate_removed_edge_environment(
        std::env::var_os("JAZZ_UPSTREAM_URL").is_some(),
        std::env::var_os("JAZZ_EDGE_CACHE_BUDGET_BYTES").is_some(),
    )?;
    validate_removed_user_auth_environment(
        REMOVED_USER_AUTH_ENV
            .iter()
            .copied()
            .filter(|name| std::env::var_os(name).is_some()),
    )?;
    validate_local_first_auth_environment(
        std::env::var("JAZZ_ALLOW_LOCAL_FIRST_AUTH").ok().as_deref(),
    )
}

/// A core no longer has its own switch for local-first (device) accounts:
/// the app's backend decides which ones sync, and the core admits only the
/// sockets its backend opens. Turning it on (as older servers needed) is
/// harmless; turning it off can't be honoured, so it is refused.
fn validate_local_first_auth_environment(value: Option<&str>) -> Result<(), String> {
    match value {
        None => Ok(()),
        // The values clap read as "on" for the old flag.
        Some(value)
            if ["y", "yes", "t", "true", "on", "1"]
                .iter()
                .any(|on| value.trim().eq_ignore_ascii_case(on)) =>
        {
            Ok(())
        }
        Some(value) => Err(format!(
            "JAZZ_ALLOW_LOCAL_FIRST_AUTH={value} is not supported: a core has no local-first switch of its own. The app's backend decides which device accounts sync (return acceptDeviceAccounts(request) from createJazz's authenticate only for those you admit)."
        )),
    }
}

fn validate_removed_user_auth_environment<'a>(
    set: impl Iterator<Item = &'a str>,
) -> Result<(), String> {
    let set: Vec<&str> = set.collect();
    if set.is_empty() {
        return Ok(());
    }
    Err(format!(
        "{} {} not supported: a core verifies no user tokens or cookies. Authenticate users in your app's backend (createJazz), which syncs for them with its backend secret.",
        set.join(", "),
        if set.len() == 1 { "is" } else { "are" },
    ))
}

/// The defaults, under `RUST_LOG`: a later directive replaces an earlier
/// one for the same target, so a target `RUST_LOG` names takes the level it
/// gives there (`RUST_LOG=jazz=debug` logs `jazz` at debug).
fn make_env_filter() -> tracing_subscriber::EnvFilter {
    let env = std::env::var("RUST_LOG").unwrap_or_default();
    tracing_subscriber::EnvFilter::new(format!(
        "jazz=info,jazz_tools=info,tower_http=debug,{}",
        env.trim()
    ))
}

#[cfg(feature = "otel")]
static OTEL_TRACER_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();
#[cfg(feature = "otel")]
static OTEL_LOGGER_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::logs::SdkLoggerProvider> =
    std::sync::OnceLock::new();
#[cfg(feature = "otel")]
static OTEL_METER_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::metrics::SdkMeterProvider> =
    std::sync::OnceLock::new();

fn init_tracing() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let fmt_layer = tracing_subscriber::fmt::layer();

    #[cfg(feature = "otel")]
    {
        if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
            let tracer_provider = otel::init_tracer_provider();
            let otel_trace_layer = otel::layer(&tracer_provider);
            let _ = OTEL_TRACER_PROVIDER.set(tracer_provider);

            let logger_provider = otel::init_logger_provider();
            let otel_log_layer = otel::log_bridge::<tracing_subscriber::Registry>(&logger_provider);
            // Route Rust panics through the OTLP log pipeline (with a flush)
            // before the process dies; see otel::install_panic_hook.
            otel::install_panic_hook(logger_provider.clone());
            let _ = OTEL_LOGGER_PROVIDER.set(logger_provider);

            let meter_provider = otel::init_meter_provider();
            opentelemetry::global::set_meter_provider(meter_provider.clone());
            let _ = OTEL_METER_PROVIDER.set(meter_provider);

            tracing_subscriber::registry()
                .with(make_env_filter())
                .with(fmt_layer)
                .with(otel_trace_layer)
                .with(otel_log_layer)
                .init();
            return;
        }
    }

    tracing_subscriber::registry()
        .with(make_env_filter())
        .with(fmt_layer)
        .init();
}

fn shutdown_tracing() {
    #[cfg(feature = "otel")]
    {
        if let Some(provider) = OTEL_TRACER_PROVIDER.get()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("OTel tracer shutdown error: {e}");
        }
        if let Some(provider) = OTEL_LOGGER_PROVIDER.get()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("OTel logger shutdown error: {e}");
        }
        if let Some(provider) = OTEL_METER_PROVIDER.get()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("OTel meter shutdown error: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_app_id_explains_uuid_requirement() {
        let _lock = ENV_LOCK.lock().expect("env lock");
        let error = Cli::try_parse_from(["jazz-tools", "server", "my-app"])
            .err()
            .expect("invalid app name must fail");
        let message = error.to_string();
        assert!(
            message.contains("Application ID must be a UUID"),
            "{message}"
        );
        assert!(message.contains("jazz-tools create app"), "{message}");
        assert!(
            Cli::try_parse_from([
                "jazz-tools",
                "server",
                "7c5fd0da-4bd1-4ba9-9203-41e1f0da142c",
            ])
            .is_ok()
        );
    }

    // Clap reads env-backed args during parsing, so every Cli::try_parse_from
    // test in this module holds this lock. The tests that mutate env vars keep
    // it held until their EnvVarGuard restores the previous value.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            // SAFETY: all CLI parser tests hold ENV_LOCK, and env-mutating tests
            // keep holding it until EnvVarGuard restores the previous value.
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, previous }
        }

        fn remove(key: &'static str) -> Self {
            let previous = std::env::var_os(key);
            // SAFETY: all CLI parser tests hold ENV_LOCK, and env-mutating tests
            // keep holding it until EnvVarGuard restores the previous value.
            unsafe {
                std::env::remove_var(key);
            }
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            // SAFETY: all CLI parser tests hold ENV_LOCK, and env-mutating tests
            // keep holding it until EnvVarGuard restores the previous value.
            unsafe {
                match &self.previous {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    #[test]
    fn removed_user_auth_settings_are_refused() {
        assert!(validate_removed_user_auth_environment(std::iter::empty()).is_ok());
        let error = validate_removed_user_auth_environment(
            ["JAZZ_JWKS_URL", "JAZZ_AUTH_COOKIE_NAME"].into_iter(),
        )
        .expect_err("a core no longer reads user-auth settings");
        assert!(
            error.starts_with("JAZZ_JWKS_URL, JAZZ_AUTH_COOKIE_NAME are not supported"),
            "{error}"
        );
        assert!(
            Cli::try_parse_from([
                "jazz-tools",
                "server",
                "7c5fd0da-4bd1-4ba9-9203-41e1f0da142c",
                "--jwks-url",
                "https://issuer.example/.well-known/jwks.json",
            ])
            .is_err(),
            "the flag is gone"
        );
    }

    #[test]
    fn server_command_reads_query_cache_switches_from_env() {
        let _lock = ENV_LOCK.lock().expect("env lock");
        let _stored_guard = EnvVarGuard::set("JAZZ_STORED_ARRANGEMENTS", "0");
        let _budget_guard = EnvVarGuard::set("JAZZ_QUERY_CACHE_BUDGET_BYTES", "1048576");
        let cli = Cli::try_parse_from([
            "jazz-tools",
            "server",
            "7c5fd0da-4bd1-4ba9-9203-41e1f0da142c",
        ])
        .expect("server command should parse");
        match cli.command {
            Commands::Server {
                stored_arrangements,
                query_cache_budget_bytes,
                ..
            } => {
                assert!(!stored_arrangements);
                assert_eq!(query_cache_budget_bytes, Some(1 << 20));
            }
            _ => panic!("expected server command"),
        }
    }

    #[test]
    fn server_command_defaults_and_validates_max_connections() {
        let _lock = ENV_LOCK.lock().expect("env lock");
        let _env_guard = EnvVarGuard::remove("JAZZ_MAX_CONNECTIONS");
        let _stored_guard = EnvVarGuard::remove("JAZZ_STORED_ARRANGEMENTS");
        let _budget_guard = EnvVarGuard::remove("JAZZ_QUERY_CACHE_BUDGET_BYTES");
        let cli = Cli::try_parse_from([
            "jazz-tools",
            "server",
            "7c5fd0da-4bd1-4ba9-9203-41e1f0da142c",
        ])
        .expect("server command should parse");
        match cli.command {
            Commands::Server {
                max_connections,
                max_connections_per_user,
                memory_limit_bytes,
                stored_arrangements,
                query_cache_budget_bytes,
                ..
            } => {
                assert_eq!(
                    max_connections,
                    jazz_server::server::DEFAULT_MAX_WEBSOCKET_CONNECTIONS
                );
                assert_eq!(
                    max_connections_per_user,
                    jazz_server::server::DEFAULT_MAX_WEBSOCKET_CONNECTIONS_PER_IDENTITY
                );
                assert_eq!(memory_limit_bytes, None);
                assert!(stored_arrangements);
                assert_eq!(query_cache_budget_bytes, None);
            }
            _ => panic!("expected server command"),
        }

        assert!(
            Cli::try_parse_from([
                "jazz-tools",
                "server",
                "7c5fd0da-4bd1-4ba9-9203-41e1f0da142c",
                "--max-connections",
                "0",
            ])
            .is_err(),
            "a zero connection cap would refuse every client"
        );
    }

    #[test]
    fn server_command_defaults_shutdown_timeout_secs() {
        let _lock = ENV_LOCK.lock().expect("env lock");
        let _env_guard = EnvVarGuard::remove("JAZZ_SHUTDOWN_TIMEOUT_SECS");
        let cli = Cli::try_parse_from([
            "jazz-tools",
            "server",
            "7c5fd0da-4bd1-4ba9-9203-41e1f0da142c",
        ])
        .expect("server command should parse");

        match cli.command {
            Commands::Server {
                shutdown_timeout_secs,
                ..
            } => assert_eq!(shutdown_timeout_secs, DEFAULT_SHUTDOWN_TIMEOUT_SECS),
            _ => panic!("expected server command"),
        }
    }

    #[test]
    fn server_command_parses_shutdown_timeout_secs() {
        let _lock = ENV_LOCK.lock().expect("env lock");
        let cli = Cli::try_parse_from([
            "jazz-tools",
            "server",
            "7c5fd0da-4bd1-4ba9-9203-41e1f0da142c",
            "--shutdown-timeout-secs",
            "7",
        ])
        .expect("server command should parse");

        match cli.command {
            Commands::Server {
                shutdown_timeout_secs,
                ..
            } => assert_eq!(shutdown_timeout_secs, 7),
            _ => panic!("expected server command"),
        }
    }

    #[test]
    fn server_command_reads_shutdown_timeout_secs_from_env() {
        let _lock = ENV_LOCK.lock().expect("env lock");
        let _env_guard = EnvVarGuard::set("JAZZ_SHUTDOWN_TIMEOUT_SECS", "11");
        let cli = Cli::try_parse_from([
            "jazz-tools",
            "server",
            "7c5fd0da-4bd1-4ba9-9203-41e1f0da142c",
        ])
        .expect("server command should parse");

        match cli.command {
            Commands::Server {
                shutdown_timeout_secs,
                ..
            } => assert_eq!(shutdown_timeout_secs, 11),
            _ => panic!("expected server command"),
        }
    }

    #[test]
    fn server_command_rejects_shutdown_timeout_secs_above_limit() {
        let _lock = ENV_LOCK.lock().expect("env lock");
        let error = match Cli::try_parse_from([
            "jazz-tools",
            "server",
            "7c5fd0da-4bd1-4ba9-9203-41e1f0da142c",
            "--shutdown-timeout-secs",
            "18446744073709551615",
        ]) {
            Ok(_) => panic!("absurd shutdown timeout should be rejected"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("shutdown timeout must be between 1 and")
        );
    }

    #[test]
    fn server_command_rejects_removed_edge_flags() {
        let _lock = ENV_LOCK.lock().expect("env lock");
        for flag in ["--upstream-url", "--edge-cache-budget-bytes"] {
            let error = Cli::try_parse_from([
                "jazz-tools",
                "server",
                "00000000-0000-0000-0000-000000000001",
                flag,
                "unused",
            ])
            .err()
            .expect("removed server roles must not silently become Core");
            assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
        }
    }

    #[test]
    fn obsolete_edge_environment_cannot_silently_start_core() {
        assert!(validate_removed_edge_environment(false, false).is_ok());
        for configured in [(true, false), (false, true), (true, true)] {
            let error = validate_removed_edge_environment(configured.0, configured.1).unwrap_err();
            assert!(error.contains("no longer supported"));
            assert!(error.contains("existing Core"));
        }
    }

    #[test]
    fn local_first_auth_off_is_refused() {
        // Old servers needed it on; it changes nothing now.
        for on in ["true", "1", "TRUE", "yes"] {
            assert!(validate_local_first_auth_environment(Some(on)).is_ok());
        }
        assert!(validate_local_first_auth_environment(None).is_ok());
        for off in ["false", "0", "no", ""] {
            let error = validate_local_first_auth_environment(Some(off)).unwrap_err();
            assert!(
                error.starts_with("JAZZ_ALLOW_LOCAL_FIRST_AUTH=")
                    && error.contains("acceptDeviceAccounts"),
                "{error}"
            );
        }
        assert!(
            Cli::try_parse_from([
                "jazz-tools",
                "server",
                "7c5fd0da-4bd1-4ba9-9203-41e1f0da142c",
                "--allow-local-first-auth",
            ])
            .is_err(),
            "the flag is gone"
        );
    }
}
