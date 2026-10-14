use std::env;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::process::ExitCode;

use jazz::account_registry::AccountId;
use jazz::db::DbIdentity;
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::schema::JazzSchema;
use jazz::serving::{
    DeploymentProfile, DrainState, DryRunReport, HealthStatus, NodeRole, ServerShell,
    StorageConfig, StorageKind,
    auth_admission::{AuthAdmissionConfig, JwtVerifierConfig},
};
use jazz::tools::AppId;
use jazz_server::loopback::websocket::{LoopbackWebSocketServer, LoopbackWebSocketServerConfig};

fn empty_runtime_schema() -> JazzSchema {
    let source = jazz::tools::public_schema::SchemaBuilder::new().build();
    jazz::schema::JazzSchema::new(&source).expect("the empty public schema is valid")
}

fn start_loopback_server(
    config: LoopbackWebSocketServerConfig,
) -> jazz_server::loopback::websocket::LoopbackWebSocketResult<LoopbackWebSocketServer> {
    if matches!(config.storage, StorageConfig::RocksDb { .. }) {
        #[cfg(feature = "rocksdb")]
        {
            return LoopbackWebSocketServer::start_with_config_and_storage_factory(
                config,
                std::sync::Arc::new(jazz_storage_rocksdb::RocksDbStorageFactory),
            );
        }
    }
    LoopbackWebSocketServer::start_with_config(config)
}

fn main() -> ExitCode {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "jazz-server".to_owned());
    let is_help = |arg: &str| arg == "-h" || arg == "--help";

    match args.next().as_deref() {
        Some("dry-run") => run_dry_run(args.collect(), &program),
        Some("server") => match args.next() {
            Some(flag) if is_help(&flag) => {
                println!("{}", server_usage(&program));
                ExitCode::SUCCESS
            }
            Some(app_id) => run_server_app(&app_id, args.collect(), &program),
            None => usage_error("missing_app_id", server_usage(&program)),
        },
        Some(command @ ("serve" | "dev-server" | "serve-loopback-websocket-schema")) => {
            match args.next() {
                Some(flag) if is_help(&flag) => {
                    println!("{}", serve_usage(&program, command));
                    ExitCode::SUCCESS
                }
                Some(schema_hex) => run_loopback_websocket_schema(
                    &schema_hex,
                    args.collect(),
                    serve_usage(&program, command),
                    &program,
                ),
                None => usage_error("missing_schema", serve_usage(&program, command)),
            }
        }
        Some("serve-loopback-websocket-schema-data-dir") => match (args.next(), args.next()) {
            (Some(flag), None) if is_help(&flag) => {
                println!("{}", serve_data_dir_usage(&program));
                ExitCode::SUCCESS
            }
            (Some(schema_hex), Some(data_dir)) => {
                let mut rest: Vec<String> = args.collect();
                rest.splice(0..0, ["--data-dir".to_owned(), data_dir]);
                run_loopback_websocket_schema(
                    &schema_hex,
                    rest,
                    serve_data_dir_usage(&program),
                    &program,
                )
            }
            (None, _) => usage_error("missing_schema", serve_data_dir_usage(&program)),
            _ => usage_error("missing_data_dir", serve_data_dir_usage(&program)),
        },
        Some("-h" | "--help") | None => {
            println!("{}", usage(&program));
            ExitCode::SUCCESS
        }
        _ => usage_error("unsupported_command", usage(&program)),
    }
}

/// Report a command-line error and the relevant usage on stderr; exit 2.
fn usage_error(error: impl std::fmt::Display, usage: String) -> ExitCode {
    eprintln!("error={error}");
    eprintln!("{usage}");
    ExitCode::from(2)
}

fn run_dry_run(args: Vec<String>, program: &str) -> ExitCode {
    let options = match CliOptions::parse(args, program) {
        Ok(options) => options,
        Err(error) => return usage_error(error, usage(program)),
    };
    let mut config = jazz::serving::ServerConfig::local("dev-core");
    apply_shell_options(&mut config, &options);
    let report = match ServerShell::new(config).and_then(|shell| shell.start_dry_run()) {
        Ok(report) => report,
        Err(error) => {
            eprintln!("error={error}");
            return ExitCode::FAILURE;
        }
    };

    print_report(&report);
    print_auth_report(&options.auth_admission);
    ExitCode::SUCCESS
}

fn run_server_app(app_id: &str, args: Vec<String>, program: &str) -> ExitCode {
    let options = match CliOptions::parse_for_server_app(args, program, app_id) {
        Ok(options) => options,
        Err(error) => return usage_error(error, server_usage(program)),
    };
    if let Err(error) = options.reject_legacy_implicit_storage() {
        eprintln!("error={error}");
        return ExitCode::FAILURE;
    }
    let storage = options.storage.clone();
    let auth_admission = options.auth_admission.clone();
    serve_loopback_until_stdin_closes(empty_runtime_schema(), options, |websocket_path| {
        println!("command=server");
        println!("app_id={app_id}");
        println!("websocket_path={websocket_path}");
        print_storage_report(&storage);
        print_auth_report(&auth_admission);
        println!("schema_catalogue=empty");
        println!("runtime_schema_loading=static_empty_schema");
    })
}

fn run_loopback_websocket_schema(
    schema_hex: &str,
    args: Vec<String>,
    usage: String,
    program: &str,
) -> ExitCode {
    let options = match CliOptions::parse(args, program) {
        Ok(options) => options,
        Err(error) => return usage_error(error, usage),
    };
    let schema_bytes = match decode_hex(schema_hex) {
        Ok(bytes) => bytes,
        Err(error) => return usage_error(error, usage),
    };
    let schema = match jazz::tools::public_schema_convert::decode_public_schema_json(&schema_bytes)
    {
        Ok(schema) => schema,
        Err(error) => {
            eprintln!("error=decode_schema: {error}");
            return ExitCode::from(2);
        }
    };
    serve_loopback_until_stdin_closes(schema, options, |_| {})
}

/// Start a loopback WebSocket server from the CLI options, print `report`
/// (given the WebSocket path) and the `ws_url` line, then serve until stdin
/// closes.
fn serve_loopback_until_stdin_closes(
    schema: JazzSchema,
    options: CliOptions,
    report: impl FnOnce(&str),
) -> ExitCode {
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0x5e; 16]),
        author: AuthorSubject::SYSTEM,
    };
    let mut config = match options.storage {
        StorageConfig::InMemory => LoopbackWebSocketServerConfig::in_memory(schema, identity),
        StorageConfig::RocksDb { path } => {
            LoopbackWebSocketServerConfig::persistent_data_dir(schema, identity, path)
        }
        StorageConfig::SQLite { .. } => unreachable!("CLI does not expose sqlite storage"),
    }
    .with_row_id_seed(0x5e)
    .with_auth_admission(options.auth_admission);
    if let Some(account) = options.admitted_account {
        config = config.with_admitted_account(account);
    }
    config.listener.bind_addr = options.listen;
    config.listener.websocket_path = options.websocket_path;

    let websocket_path = config.listener.websocket_path.clone();
    let server = match start_loopback_server(config) {
        Ok(server) => server,
        Err(error) => {
            eprintln!("error={error}");
            return ExitCode::FAILURE;
        }
    };

    report(&websocket_path);
    println!("ws_url=ws://{}{}", server.local_addr(), websocket_path);
    let _ = io::stdout().flush();

    let mut stdin = String::new();
    let _ = io::stdin().read_to_string(&mut stdin);
    server.shutdown();
    ExitCode::SUCCESS
}

fn usage(program: &str) -> String {
    let env = ENV
        .iter()
        .filter(|(_, setting)| !matches!(setting, Setting::UpstreamUrl | Setting::AdminSecret))
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "usage={program} dry-run {}\n{}\n{}\nalias={program} dev-server <schema-source-json-hex> [same options as serve]\n{}\n{}\nenv={env}",
        options_usage(true, true),
        server_usage(program),
        serve_usage(program, "serve"),
        serve_usage(program, "serve-loopback-websocket-schema"),
        serve_data_dir_usage(program),
    )
}

/// The option list shown in usage lines; `server` takes no `--jwt-audience`
/// (the app id is the audience) and the data-dir command takes no storage flag.
fn options_usage(storage: bool, audience: bool) -> String {
    format!(
        "[--listen <addr>|--bind <addr>] [--port <port>]{} [--websocket-path <path>|--ws-path <path>] [--auth-static-bearer <token>|--static-bearer <token>] [--loopback-admitted-account <uuid>] [--auth-jwt-ed-public-key-pem <pem>] [--jwt-issuer <issuer>]{} [--allow-local-first-auth <bool>] [--anonymous-subject <subject>]",
        if storage {
            " [--data-dir <dir>|--dataDir <dir>|--in-memory|--memory]"
        } else {
            ""
        },
        if audience {
            " [--jwt-audience <audience>]"
        } else {
            ""
        },
    )
}

const SERVER_STORAGE_HELP: &str = "storage_default=./data/apps/<canonical-app-uuid> (UUID APP_ID spellings are canonicalised; other names use a deterministic UUID). Explicit storage paths are unchanged. If ./data/CURRENT exists, the implicit default refuses to start: deliberately use --data-dir ./data or JAZZ_SERVER_DATA_DIR=./data to reopen that store, or migrate it manually. No legacy data is moved or adopted automatically.";

fn server_usage(program: &str) -> String {
    format!(
        "usage={program} server <APP_ID> {}\n{SERVER_STORAGE_HELP}",
        options_usage(true, false)
    )
}

fn serve_usage(program: &str, command: &str) -> String {
    format!(
        "usage={program} {command} <schema-source-json-hex> {}",
        options_usage(true, true)
    )
}

fn serve_data_dir_usage(program: &str) -> String {
    format!(
        "usage={program} serve-loopback-websocket-schema-data-dir <schema-source-json-hex> <data-dir> {}",
        options_usage(false, true)
    )
}

fn print_report(report: &DryRunReport) {
    let (plan, health, metrics) = (&report.runtime_plan, &report.health, &report.metrics);
    let rows: &[(&str, &dyn std::fmt::Display)] = &[
        ("command", &"dry-run"),
        ("role", &role_name(report.role)),
        ("profile", &profile_name(report.profile)),
        ("listener", &report.listener),
        ("storage", &storage_name(&report.storage)),
        ("runtime_plan.core_role", &role_name(plan.core_role)),
        ("runtime_plan.profile", &profile_name(plan.profile)),
        (
            "runtime_plan.storage_kind",
            &storage_kind_name(plan.storage_kind),
        ),
        (
            "runtime_plan.schema_column_family_count",
            &plan.schema_column_family_count,
        ),
        ("health.status", &health_status_name(health.status)),
        ("health.role", &role_name(health.role)),
        ("health.profile", &profile_name(health.profile)),
        ("health.drain_state", &drain_state_name(health.drain_state)),
        ("health.message", &health.message),
        ("metrics.active_sessions", &metrics.active_sessions),
        ("metrics.total_sessions", &metrics.total_sessions),
        ("metrics.rejected_sessions", &metrics.rejected_sessions),
        ("metrics.frames_received", &metrics.frames_received),
        ("metrics.frames_sent", &metrics.frames_sent),
        ("metrics.bytes_received", &metrics.bytes_received),
        ("metrics.bytes_sent", &metrics.bytes_sent),
        ("metrics.ticks", &metrics.ticks),
        ("metrics.tick_inbound", &metrics.tick_inbound),
        ("metrics.tick_outbound", &metrics.tick_outbound),
        (
            "metrics.tick_subscription_wakes",
            &metrics.tick_subscription_wakes,
        ),
        ("metrics.tick_write_wakes", &metrics.tick_write_wakes),
        ("metrics.last_tick_inbound", &metrics.last_tick_inbound),
        ("metrics.last_tick_outbound", &metrics.last_tick_outbound),
        (
            "metrics.last_tick_subscription_wakes",
            &metrics.last_tick_subscription_wakes,
        ),
        (
            "metrics.last_tick_write_wakes",
            &metrics.last_tick_write_wakes,
        ),
        (
            "metrics.protocol_version_mismatches",
            &metrics.protocol_version_mismatches,
        ),
        (
            "metrics.subscription_full_diff_fallbacks",
            &metrics.subscription_full_diff_fallbacks,
        ),
        (
            "metrics.storage_migrations_applied",
            &metrics.storage_migrations_applied,
        ),
        ("sockets_bound", &false),
        ("storage_opened", &false),
        ("runtime_started", &false),
    ];
    for (key, value) in rows {
        println!("{key}={value}");
    }
}

fn print_auth_report(auth_admission: &AuthAdmissionConfig) {
    println!(
        "auth.mode={}",
        if auth_admission.static_bearer_token.is_some() {
            "static-bearer"
        } else if auth_admission.jwt_verifier.is_some() {
            "jwt"
        } else {
            "anonymous"
        }
    );
    println!(
        "auth.allow_local_first_auth={}",
        auth_admission.allow_local_first_auth
    );
    println!(
        "auth.anonymous_subject={}",
        auth_admission.anonymous_subject
    );
}

fn print_storage_report(storage: &StorageConfig) {
    println!("storage={}", storage_name(storage));
    if let StorageConfig::RocksDb { path } = storage {
        println!("data_dir={}", path.display());
    }
}

fn role_name(role: NodeRole) -> &'static str {
    match role {
        NodeRole::Relay => "relay",
        NodeRole::Core => "core",
    }
}

fn profile_name(profile: DeploymentProfile) -> &'static str {
    match profile {
        DeploymentProfile::Local => "local",
        DeploymentProfile::Test => "test",
        DeploymentProfile::Production => "production",
    }
}

fn storage_name(storage: &StorageConfig) -> &'static str {
    match storage {
        StorageConfig::InMemory => "in-memory",
        StorageConfig::RocksDb { .. } => "rocksdb",
        StorageConfig::SQLite { .. } => "sqlite",
    }
}

fn storage_kind_name(storage_kind: StorageKind) -> &'static str {
    match storage_kind {
        StorageKind::InMemory => "in-memory",
        StorageKind::RocksDb => "rocksdb",
        StorageKind::SQLite => "sqlite",
    }
}

fn health_status_name(status: HealthStatus) -> &'static str {
    match status {
        HealthStatus::Ready => "ready",
        HealthStatus::Draining => "draining",
        HealthStatus::Unhealthy => "unhealthy",
    }
}

fn drain_state_name(drain_state: DrainState) -> &'static str {
    match drain_state {
        DrainState::Running => "running",
        DrainState::ShutdownRequested => "shutdown-requested",
        DrainState::Draining => "draining",
        DrainState::Drained => "drained",
        DrainState::Stopped => "stopped",
    }
}

fn decode_hex(text: &str) -> Result<Vec<u8>, String> {
    if !text.len().is_multiple_of(2) {
        return Err("hex input has odd length".to_owned());
    }
    let digit = |byte: u8| {
        (byte as char)
            .to_digit(16)
            .ok_or("hex input contains non-hex digit")
    };
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| Ok((digit(pair[0])? << 4 | digit(pair[1])?) as u8))
        .collect()
}

#[derive(Clone, Debug)]
struct CliOptions {
    listen: SocketAddr,
    websocket_path: String,
    storage: StorageConfig,
    implicit_app_storage: bool,
    auth_admission: AuthAdmissionConfig,
    admitted_account: Option<AccountId>,
    upstream_url: Option<String>,
}

/// One configurable setting, reachable from a flag (`--x v` or `--x=v`) and,
/// for most settings, an environment variable.
#[derive(Clone, Copy)]
enum Setting {
    Listen,
    Port,
    DataDir,
    /// Flag-only form takes no value; the environment form is gated on a truthy value.
    InMemory,
    WebSocketPath,
    StaticBearer,
    AdmittedAccount,
    /// Always rejected: privileged secrets are not jazz-server bearer credentials.
    AdminSecret,
    JwtPublicKeyPem,
    JwtIssuer,
    JwtAudience,
    AllowLocalFirstAuth,
    AnonymousSubject,
    /// Accepted only to be rejected with a pointer at the removed edge mode.
    UpstreamUrl,
    Help,
}

/// Flags and their aliases; a later flag overrides an earlier one.
const FLAGS: &[(&str, Setting)] = &[
    ("--listen", Setting::Listen),
    ("--bind", Setting::Listen),
    ("--port", Setting::Port),
    ("--data-dir", Setting::DataDir),
    ("--dataDir", Setting::DataDir),
    ("--in-memory", Setting::InMemory),
    ("--memory", Setting::InMemory),
    ("--websocket-path", Setting::WebSocketPath),
    ("--ws-path", Setting::WebSocketPath),
    ("--auth-static-bearer", Setting::StaticBearer),
    ("--static-bearer", Setting::StaticBearer),
    ("--loopback-admitted-account", Setting::AdmittedAccount),
    ("--admin-secret", Setting::AdminSecret),
    ("--auth-jwt-ed-public-key-pem", Setting::JwtPublicKeyPem),
    ("--jwt-issuer", Setting::JwtIssuer),
    ("--jwt-audience", Setting::JwtAudience),
    ("--allow-local-first-auth", Setting::AllowLocalFirstAuth),
    ("--anonymous-subject", Setting::AnonymousSubject),
    ("--upstream-url", Setting::UpstreamUrl),
    ("--help", Setting::Help),
    ("-h", Setting::Help),
];

/// Environment variables, applied in this order before any flag.
const ENV: &[(&str, Setting)] = &[
    ("JAZZ_UPSTREAM_URL", Setting::UpstreamUrl),
    ("JAZZ_SERVER_LISTEN", Setting::Listen),
    ("JAZZ_SERVER_PORT", Setting::Port),
    ("JAZZ_SERVER_DATA_DIR", Setting::DataDir),
    ("JAZZ_SERVER_IN_MEMORY", Setting::InMemory),
    ("JAZZ_SERVER_WEBSOCKET_PATH", Setting::WebSocketPath),
    ("JAZZ_ADMIN_SECRET", Setting::AdminSecret),
    ("JAZZ_BACKEND_SECRET", Setting::AdminSecret),
    ("JAZZ_SERVER_AUTH_STATIC_BEARER", Setting::StaticBearer),
    (
        "JAZZ_SERVER_AUTH_JWT_ED_PUBLIC_KEY_PEM",
        Setting::JwtPublicKeyPem,
    ),
    ("JAZZ_JWT_ISSUER", Setting::JwtIssuer),
    ("JAZZ_JWT_AUDIENCE", Setting::JwtAudience),
    ("JAZZ_ALLOW_LOCAL_FIRST_AUTH", Setting::AllowLocalFirstAuth),
    ("JAZZ_SERVER_ANONYMOUS_SUBJECT", Setting::AnonymousSubject),
];

impl CliOptions {
    fn parse(args: Vec<String>, program: &str) -> Result<Self, String> {
        let mut options = Self::defaults(StorageConfig::InMemory, "/sync".to_owned());
        options.apply_env_and_args(args, program)?;
        options.reject_unsupported_upstream_url()?;
        Ok(options)
    }

    fn parse_for_server_app(
        args: Vec<String>,
        program: &str,
        app_id: &str,
    ) -> Result<Self, String> {
        if app_id.trim().is_empty() {
            return Err("empty_app_id".to_owned());
        }
        let canonical_app_id =
            AppId::from_string(app_id).unwrap_or_else(|_| AppId::from_name(app_id));
        let mut options = Self::defaults(
            StorageConfig::data_dir(format!("./data/apps/{canonical_app_id}")),
            format!("/apps/{app_id}/ws"),
        );
        options.implicit_app_storage = true;
        options.apply_env_and_args(args, program)?;
        options.auth_admission.expected_audience = Some(app_id.to_owned());
        options.reject_unsupported_upstream_url()?;
        Ok(options)
    }

    /// Apply the environment, then the flags in order, over the defaults.
    fn apply_env_and_args(&mut self, args: Vec<String>, program: &str) -> Result<(), String> {
        for &(name, setting) in ENV {
            let Some(value) = env::var_os(name) else {
                continue;
            };
            // A non-UTF-8 value counts as unset, except that a secret is refused either way.
            match value.into_string() {
                Ok(value) => self.set(setting, value, name)?,
                Err(_) if matches!(setting, Setting::AdminSecret) => {
                    self.set(setting, String::new(), name)?;
                }
                Err(_) => {}
            }
        }

        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            let (flag, inline) = match arg.split_once('=') {
                Some((flag, value)) if flag.starts_with("--") => (flag, Some(value)),
                _ => (arg.as_str(), None),
            };
            let Some(&(_, setting)) = FLAGS.iter().find(|(name, _)| *name == flag) else {
                return Err(format!("unsupported_option={arg}"));
            };
            let value = match (setting, inline) {
                (Setting::AdminSecret, _) => String::new(),
                (Setting::Help | Setting::InMemory, Some(_)) => {
                    return Err(format!("unsupported_option={arg}"));
                }
                (Setting::Help, None) => return Err(format!("help_requested usage={program}")),
                (Setting::InMemory, None) => "true".to_owned(),
                (_, Some("")) => return Err(format!("missing_value_for={arg}")),
                (_, Some(value)) => value.to_owned(),
                (_, None) => args
                    .next()
                    .ok_or_else(|| format!("missing_value_for={arg}"))?,
            };
            self.set(setting, value, flag)?;
        }
        Ok(())
    }

    /// Apply one setting; `source` is the flag or environment variable name.
    fn set(&mut self, setting: Setting, value: String, source: &str) -> Result<(), String> {
        let auth = &mut self.auth_admission;
        match setting {
            Setting::Listen => self.listen = parse_socket_addr(&value)?,
            Setting::Port => self.listen.set_port(
                value
                    .parse::<u16>()
                    .map_err(|error| format!("invalid {source}: {error}"))?,
            ),
            Setting::DataDir => self.select_storage(StorageConfig::data_dir(value)),
            Setting::InMemory => {
                if parse_bool(&value, source) == Ok(true) {
                    self.select_storage(StorageConfig::InMemory);
                }
            }
            Setting::WebSocketPath => self.websocket_path = value,
            // Deliberately resets every earlier auth setting.
            Setting::StaticBearer => *auth = AuthAdmissionConfig::static_bearer(value),
            Setting::AdmittedAccount => {
                self.admitted_account = Some(parse_admitted_account(&value, source)?);
            }
            Setting::AdminSecret if source.starts_with("--") => {
                return Err(
                    "--admin-secret is not a jazz-server bearer credential; use --auth-static-bearer for client bearer admission or jazz-tools server --admin-secret for administrative access"
                        .to_owned(),
                );
            }
            Setting::AdminSecret => {
                return Err(format!(
                    "{source} is not a jazz-server bearer credential; use JAZZ_SERVER_AUTH_STATIC_BEARER for explicit client bearer admission or jazz-tools server for privileged admin/backend access"
                ));
            }
            Setting::JwtPublicKeyPem => {
                auth.static_bearer_token = None;
                auth.jwt_verifier = Some(JwtVerifierConfig::ed_public_key_pem(value));
            }
            Setting::JwtIssuer => auth.expected_issuer = Some(value),
            Setting::JwtAudience => auth.expected_audience = Some(value),
            Setting::AllowLocalFirstAuth => {
                auth.allow_local_first_auth = parse_bool(&value, source)?;
            }
            Setting::AnonymousSubject => auth.anonymous_subject = value,
            Setting::UpstreamUrl => self.upstream_url = Some(value),
            Setting::Help => unreachable!("--help is handled before a value is read"),
        }
        Ok(())
    }

    fn defaults(storage: StorageConfig, websocket_path: String) -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 0)),
            websocket_path,
            storage,
            implicit_app_storage: false,
            auth_admission: AuthAdmissionConfig::default(),
            admitted_account: None,
            upstream_url: None,
        }
    }

    fn select_storage(&mut self, storage: StorageConfig) {
        self.storage = storage;
        self.implicit_app_storage = false;
    }

    fn reject_legacy_implicit_storage(&self) -> Result<(), String> {
        if !self.implicit_app_storage {
            return Ok(());
        }
        // Inspect the entry itself: even a dangling CURRENT link is legacy evidence.
        // Do not create or open the app root until this read-only check succeeds.
        match std::fs::symlink_metadata("./data/CURRENT") {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Ok(_) => Err(format!(
                "legacy_default_storage: ./data/CURRENT exists; {SERVER_STORAGE_HELP}"
            )),
            Err(error) => Err(format!(
                "legacy_default_storage_probe_failed: cannot inspect ./data/CURRENT: {error}; resolve this filesystem error before using the implicit default. {SERVER_STORAGE_HELP}"
            )),
        }
    }

    fn reject_unsupported_upstream_url(&self) -> Result<(), String> {
        if self.upstream_url.is_some() {
            return Err(
                "unsupported_upstream_url: server edges are no longer supported; remove --upstream-url/JAZZ_UPSTREAM_URL and connect clients directly to Core".to_owned()
            );
        }
        Ok(())
    }
}

fn parse_admitted_account(value: &str, option: &str) -> Result<AccountId, String> {
    let account = AccountId(
        uuid::Uuid::parse_str(value).map_err(|error| format!("invalid {option}: {error}"))?,
    );
    if account.0.is_nil() {
        return Err(format!("invalid {option}: system account is reserved"));
    }
    Ok(account)
}

fn apply_shell_options(config: &mut jazz::serving::ServerConfig, options: &CliOptions) {
    config.listener.bind_addr = options.listen;
    config.listener.websocket_path = options.websocket_path.clone();
    config.storage = options.storage.clone();
}

fn parse_socket_addr(value: &str) -> Result<SocketAddr, String> {
    value
        .parse::<SocketAddr>()
        .map_err(|error| format!("invalid listen address {value:?}: {error}"))
}

fn parse_bool(value: &str, name: &str) -> Result<bool, String> {
    match value {
        "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON" => Ok(true),
        "0" | "false" | "FALSE" | "no" | "NO" | "off" | "OFF" => Ok(false),
        _ => Err(format!("invalid_bool {name}={value:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_app_parse_accepts_alpha_flags_and_in_memory_opt_out() {
        let options = CliOptions::parse_for_server_app(
            vec![
                "--listen".to_owned(),
                "127.0.0.1:9999".to_owned(),
                "--in-memory".to_owned(),
                "--auth-static-bearer".to_owned(),
                "secret".to_owned(),
                "--loopback-admitted-account".to_owned(),
                "7c5fd0da-4bd1-4ba9-9203-41e1f0da142c".to_owned(),
            ],
            "jazz-server",
            "demo",
        )
        .unwrap();

        assert_eq!(options.listen, SocketAddr::from(([127, 0, 0, 1], 9999)));
        assert_eq!(options.websocket_path, "/apps/demo/ws");
        assert_eq!(options.storage, StorageConfig::InMemory);
        assert!(options.auth_admission.static_bearer_token.is_some());
        assert_eq!(
            options.admitted_account,
            Some(AccountId(
                uuid::Uuid::parse_str("7c5fd0da-4bd1-4ba9-9203-41e1f0da142c").unwrap()
            ))
        );
    }

    #[test]
    fn loopback_admitted_account_rejects_the_system_account() {
        let error = CliOptions::parse(
            vec![
                "--loopback-admitted-account".to_owned(),
                "00000000-0000-0000-0000-000000000000".to_owned(),
            ],
            "jazz-server",
        )
        .expect_err("system attribution account must not be a public session account");
        assert!(error.contains("system account is reserved"));
    }
}
