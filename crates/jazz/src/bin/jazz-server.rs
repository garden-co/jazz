use std::env;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::process::ExitCode;

use jazz::db::DbIdentity;
use jazz::ids::{AuthorId, NodeUuid};
use jazz::schema::JazzSchema;
use jazz_server::{
    DeploymentProfile, DrainState, DryRunReport, HealthStatus, NodeRole, ServerShell,
    StorageConfig, StorageKind,
    auth_admission::{AuthAdmissionConfig, JwtVerifierConfig},
    loopback_http::load_latest_admin_schema_for_app,
    loopback_websocket::{LoopbackWebSocketServer, LoopbackWebSocketServerConfig},
};

fn main() -> ExitCode {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "jazz-server".to_owned());

    match args.next().as_deref() {
        Some("dry-run") => run_dry_run(args.collect(), &program),
        Some("server") => match args.next() {
            Some(flag) if flag == "-h" || flag == "--help" => {
                print_server_usage(&program);
                ExitCode::SUCCESS
            }
            Some(app_id) => run_server_app(&app_id, args.collect(), &program),
            None => {
                eprintln!("error=missing_app_id");
                print_server_usage_stderr(&program);
                ExitCode::from(2)
            }
        },
        Some(command @ ("serve" | "dev-server" | "serve-loopback-websocket-schema")) => {
            match args.next() {
                Some(flag) if flag == "-h" || flag == "--help" => {
                    print_serve_usage(&program, command);
                    ExitCode::SUCCESS
                }
                Some(schema_hex) => {
                    run_loopback_websocket_schema(command, &schema_hex, args.collect(), &program)
                }
                None => {
                    eprintln!("error=missing_schema");
                    print_serve_usage_stderr(&program, command);
                    ExitCode::from(2)
                }
            }
        }
        Some("serve-loopback-websocket-schema-data-dir") => match (args.next(), args.next()) {
            (Some(flag), None) if flag == "-h" || flag == "--help" => {
                print_serve_data_dir_usage(&program);
                ExitCode::SUCCESS
            }
            (Some(schema_hex), Some(data_dir)) => {
                let mut rest: Vec<String> = args.collect();
                rest.splice(0..0, ["--data-dir".to_owned(), data_dir]);
                run_loopback_websocket_schema(
                    "serve-loopback-websocket-schema-data-dir",
                    &schema_hex,
                    rest,
                    &program,
                )
            }
            (None, _) => {
                eprintln!("error=missing_schema");
                print_serve_data_dir_usage_stderr(&program);
                ExitCode::from(2)
            }
            _ => {
                eprintln!("error=missing_data_dir");
                print_serve_data_dir_usage_stderr(&program);
                ExitCode::from(2)
            }
        },
        Some("-h" | "--help") | None => {
            print_usage(&program);
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!("error=unsupported_command");
            print_usage_stderr(&program);
            ExitCode::from(2)
        }
    }
}

fn run_dry_run(args: Vec<String>, program: &str) -> ExitCode {
    let options = match CliOptions::parse(args, program) {
        Ok(options) => options,
        Err(error) => {
            eprintln!("error={error}");
            print_usage_stderr(program);
            return ExitCode::from(2);
        }
    };
    let mut config = jazz_server::ServerConfig::local("dev-core");
    apply_shell_options(&mut config, &options);
    let shell = match ServerShell::new(config) {
        Ok(shell) => shell,
        Err(error) => {
            eprintln!("error={error}");
            return ExitCode::FAILURE;
        }
    };
    let report = match shell.start_dry_run() {
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
        Err(error) => {
            eprintln!("error={error}");
            print_server_usage_stderr(program);
            return ExitCode::from(2);
        }
    };
    let schema = match &options.storage {
        StorageConfig::RocksDb { path } => match load_latest_admin_schema_for_app(path, app_id) {
            Ok(Some(schema)) => schema,
            Ok(None) => JazzSchema::new([]),
            Err(error) => {
                eprintln!("error=load_admin_schema_store: {error}");
                return ExitCode::FAILURE;
            }
        },
        _ => JazzSchema::new([]),
    };
    let schema_catalogue = if schema.tables.is_empty() {
        "empty"
    } else {
        "admin_schema_store"
    };
    let runtime_schema_loading = if schema.tables.is_empty() {
        "static_empty_schema"
    } else {
        "admin_schema_store_latest"
    };
    let admin_schema_store = if schema.tables.is_empty() {
        "not_opened"
    } else {
        "opened"
    };
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0x5e; 16]),
        author: AuthorId::SYSTEM,
    };
    let storage = options.storage.clone();
    let auth_admission = options.auth_admission.clone();
    let mut config = match options.storage {
        StorageConfig::InMemory => LoopbackWebSocketServerConfig::in_memory(schema, identity),
        StorageConfig::RocksDb { path } => {
            LoopbackWebSocketServerConfig::persistent_data_dir(schema, identity, path)
        }
        StorageConfig::SQLite { .. } => unreachable!("CLI does not expose sqlite storage"),
    }
    .with_row_id_seed(0x5e)
    .with_auth_admission(options.auth_admission);
    config.listener.bind_addr = options.listen;
    config.listener.websocket_path = options.websocket_path;

    let websocket_path = config.listener.websocket_path.clone();
    let server = match LoopbackWebSocketServer::start_with_config(config) {
        Ok(server) => server,
        Err(error) => {
            eprintln!("error={error}");
            return ExitCode::FAILURE;
        }
    };

    println!("command=server");
    println!("app_id={app_id}");
    println!("websocket_path={websocket_path}");
    print_storage_report(&storage);
    print_auth_report(&auth_admission);
    println!("schema_catalogue={schema_catalogue}");
    println!("runtime_schema_loading={runtime_schema_loading}");
    println!("admin_schema_api=not_started");
    println!("admin_schema_store={admin_schema_store}");
    println!("admin_schema_owner=loopback_http_only");
    println!("ws_url=ws://{}{}", server.local_addr(), websocket_path);
    let _ = io::stdout().flush();

    let mut stdin = String::new();
    let _ = io::stdin().read_to_string(&mut stdin);
    server.shutdown();
    ExitCode::SUCCESS
}

fn run_loopback_websocket_schema(
    command: &str,
    schema_hex: &str,
    args: Vec<String>,
    program: &str,
) -> ExitCode {
    let options = match CliOptions::parse(args, program) {
        Ok(options) => options,
        Err(error) => {
            eprintln!("error={error}");
            if command == "serve-loopback-websocket-schema-data-dir" {
                print_serve_data_dir_usage_stderr(program);
            } else {
                print_serve_usage_stderr(program, command);
            }
            return ExitCode::from(2);
        }
    };
    let schema_bytes = match decode_hex(schema_hex) {
        Ok(bytes) => bytes,
        Err(error) => {
            eprintln!("error={error}");
            if command == "serve-loopback-websocket-schema-data-dir" {
                print_serve_data_dir_usage_stderr(program);
            } else {
                print_serve_usage_stderr(program, command);
            }
            return ExitCode::from(2);
        }
    };
    let schema = match postcard::from_bytes::<JazzSchema>(&schema_bytes) {
        Ok(schema) => schema,
        Err(error) => {
            eprintln!("error=decode_schema: {error}");
            return ExitCode::from(2);
        }
    };

    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0x5e; 16]),
        author: AuthorId::SYSTEM,
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
    config.listener.bind_addr = options.listen;
    config.listener.websocket_path = options.websocket_path;

    let websocket_path = config.listener.websocket_path.clone();
    let server = match LoopbackWebSocketServer::start_with_config(config) {
        Ok(server) => server,
        Err(error) => {
            eprintln!("error={error}");
            return ExitCode::FAILURE;
        }
    };

    println!("ws_url=ws://{}{}", server.local_addr(), websocket_path);
    let _ = io::stdout().flush();

    let mut stdin = String::new();
    let _ = io::stdin().read_to_string(&mut stdin);
    server.shutdown();
    ExitCode::SUCCESS
}

fn print_usage(program: &str) {
    println!(
        "usage={program} dry-run [--listen <addr>|--bind <addr>] [--port <port>] [--data-dir <dir>|--dataDir <dir>|--in-memory|--memory] [--websocket-path <path>|--ws-path <path>] [--auth-static-bearer <token>|--static-bearer <token>|--admin-secret <token>] [--auth-jwt-ed-public-key-pem <pem>] [--allow-local-first-auth <bool>] [--anonymous-subject <subject>] [--upstream-url <url>]"
    );
    print_server_usage(program);
    print_serve_usage(program, "serve");
    println!("alias={program} dev-server <schema-postcard-hex> [same options as serve]");
    print_serve_usage(program, "serve-loopback-websocket-schema");
    print_serve_data_dir_usage(program);
    println!(
        "env=JAZZ_SERVER_LISTEN,JAZZ_SERVER_PORT,JAZZ_SERVER_DATA_DIR,JAZZ_SERVER_IN_MEMORY,JAZZ_SERVER_WEBSOCKET_PATH,JAZZ_SERVER_AUTH_STATIC_BEARER,JAZZ_ADMIN_SECRET,JAZZ_BACKEND_SECRET,JAZZ_SERVER_AUTH_JWT_ED_PUBLIC_KEY_PEM,JAZZ_ALLOW_LOCAL_FIRST_AUTH,JAZZ_SERVER_ANONYMOUS_SUBJECT,JAZZ_UPSTREAM_URL"
    );
}

fn print_usage_stderr(program: &str) {
    eprintln!(
        "usage={program} dry-run [--listen <addr>|--bind <addr>] [--port <port>] [--data-dir <dir>|--dataDir <dir>|--in-memory|--memory] [--websocket-path <path>|--ws-path <path>] [--auth-static-bearer <token>|--static-bearer <token>|--admin-secret <token>] [--auth-jwt-ed-public-key-pem <pem>] [--allow-local-first-auth <bool>] [--anonymous-subject <subject>] [--upstream-url <url>]"
    );
    print_server_usage_stderr(program);
    print_serve_usage_stderr(program, "serve");
    eprintln!("alias={program} dev-server <schema-postcard-hex> [same options as serve]");
    print_serve_usage_stderr(program, "serve-loopback-websocket-schema");
    print_serve_data_dir_usage_stderr(program);
    eprintln!(
        "env=JAZZ_SERVER_LISTEN,JAZZ_SERVER_PORT,JAZZ_SERVER_DATA_DIR,JAZZ_SERVER_IN_MEMORY,JAZZ_SERVER_WEBSOCKET_PATH,JAZZ_SERVER_AUTH_STATIC_BEARER,JAZZ_ADMIN_SECRET,JAZZ_BACKEND_SECRET,JAZZ_SERVER_AUTH_JWT_ED_PUBLIC_KEY_PEM,JAZZ_ALLOW_LOCAL_FIRST_AUTH,JAZZ_SERVER_ANONYMOUS_SUBJECT,JAZZ_UPSTREAM_URL"
    );
}

fn print_server_usage(program: &str) {
    println!(
        "usage={program} server <APP_ID> [--listen <addr>|--bind <addr>] [--port <port>] [--data-dir <dir>|--dataDir <dir>|--in-memory|--memory] [--websocket-path <path>|--ws-path <path>] [--auth-static-bearer <token>|--static-bearer <token>|--admin-secret <token>] [--auth-jwt-ed-public-key-pem <pem>] [--allow-local-first-auth <bool>] [--anonymous-subject <subject>] [--upstream-url <url>]"
    );
}

fn print_server_usage_stderr(program: &str) {
    eprintln!(
        "usage={program} server <APP_ID> [--listen <addr>|--bind <addr>] [--port <port>] [--data-dir <dir>|--dataDir <dir>|--in-memory|--memory] [--websocket-path <path>|--ws-path <path>] [--auth-static-bearer <token>|--static-bearer <token>|--admin-secret <token>] [--auth-jwt-ed-public-key-pem <pem>] [--allow-local-first-auth <bool>] [--anonymous-subject <subject>] [--upstream-url <url>]"
    );
}

fn print_serve_usage(program: &str, command: &str) {
    println!(
        "usage={program} {command} <schema-postcard-hex> [--listen <addr>|--bind <addr>] [--port <port>] [--data-dir <dir>|--dataDir <dir>|--in-memory|--memory] [--websocket-path <path>|--ws-path <path>] [--auth-static-bearer <token>|--static-bearer <token>|--admin-secret <token>] [--auth-jwt-ed-public-key-pem <pem>] [--allow-local-first-auth <bool>] [--anonymous-subject <subject>] [--upstream-url <url>]"
    );
}

fn print_serve_usage_stderr(program: &str, command: &str) {
    eprintln!(
        "usage={program} {command} <schema-postcard-hex> [--listen <addr>|--bind <addr>] [--port <port>] [--data-dir <dir>|--dataDir <dir>|--in-memory|--memory] [--websocket-path <path>|--ws-path <path>] [--auth-static-bearer <token>|--static-bearer <token>|--admin-secret <token>] [--auth-jwt-ed-public-key-pem <pem>] [--allow-local-first-auth <bool>] [--anonymous-subject <subject>] [--upstream-url <url>]"
    );
}

fn print_serve_data_dir_usage(program: &str) {
    println!(
        "usage={program} serve-loopback-websocket-schema-data-dir <schema-postcard-hex> <data-dir> [--listen <addr>|--bind <addr>] [--port <port>] [--websocket-path <path>|--ws-path <path>] [--auth-static-bearer <token>|--static-bearer <token>|--admin-secret <token>] [--auth-jwt-ed-public-key-pem <pem>] [--allow-local-first-auth <bool>] [--anonymous-subject <subject>] [--upstream-url <url>]"
    );
}

fn print_serve_data_dir_usage_stderr(program: &str) {
    eprintln!(
        "usage={program} serve-loopback-websocket-schema-data-dir <schema-postcard-hex> <data-dir> [--listen <addr>|--bind <addr>] [--port <port>] [--websocket-path <path>|--ws-path <path>] [--auth-static-bearer <token>|--static-bearer <token>|--admin-secret <token>] [--auth-jwt-ed-public-key-pem <pem>] [--allow-local-first-auth <bool>] [--anonymous-subject <subject>] [--upstream-url <url>]"
    );
}

fn print_report(report: &DryRunReport) {
    println!("command=dry-run");
    println!("role={}", role_name(report.role));
    println!("profile={}", profile_name(report.profile));
    println!("listener={}", report.listener);
    println!("storage={}", storage_name(&report.storage));
    println!(
        "runtime_plan.core_role={}",
        role_name(report.runtime_plan.core_role)
    );
    println!(
        "runtime_plan.profile={}",
        profile_name(report.runtime_plan.profile)
    );
    println!(
        "runtime_plan.storage_kind={}",
        storage_kind_name(report.runtime_plan.storage_kind)
    );
    println!(
        "runtime_plan.schema_column_family_count={}",
        report.runtime_plan.schema_column_family_count
    );
    println!("health.status={}", health_status_name(report.health.status));
    println!("health.role={}", role_name(report.health.role));
    println!("health.profile={}", profile_name(report.health.profile));
    println!(
        "health.drain_state={}",
        drain_state_name(report.health.drain_state)
    );
    println!("health.message={}", report.health.message);
    println!("metrics.active_sessions={}", report.metrics.active_sessions);
    println!("metrics.total_sessions={}", report.metrics.total_sessions);
    println!(
        "metrics.rejected_sessions={}",
        report.metrics.rejected_sessions
    );
    println!("metrics.frames_received={}", report.metrics.frames_received);
    println!("metrics.frames_sent={}", report.metrics.frames_sent);
    println!("metrics.bytes_received={}", report.metrics.bytes_received);
    println!("metrics.bytes_sent={}", report.metrics.bytes_sent);
    println!("metrics.ticks={}", report.metrics.ticks);
    println!("metrics.tick_inbound={}", report.metrics.tick_inbound);
    println!("metrics.tick_outbound={}", report.metrics.tick_outbound);
    println!(
        "metrics.tick_subscription_wakes={}",
        report.metrics.tick_subscription_wakes
    );
    println!(
        "metrics.tick_write_wakes={}",
        report.metrics.tick_write_wakes
    );
    println!(
        "metrics.last_tick_inbound={}",
        report.metrics.last_tick_inbound
    );
    println!(
        "metrics.last_tick_outbound={}",
        report.metrics.last_tick_outbound
    );
    println!(
        "metrics.last_tick_subscription_wakes={}",
        report.metrics.last_tick_subscription_wakes
    );
    println!(
        "metrics.last_tick_write_wakes={}",
        report.metrics.last_tick_write_wakes
    );
    println!(
        "metrics.protocol_version_mismatches={}",
        report.metrics.protocol_version_mismatches
    );
    println!(
        "metrics.subscription_full_diff_fallbacks={}",
        report.metrics.subscription_full_diff_fallbacks
    );
    println!(
        "metrics.storage_migrations_applied={}",
        report.metrics.storage_migrations_applied
    );
    println!("admin_schema_api=not_started");
    println!("admin_schema_store=not_opened");
    println!("admin_schema_owner=loopback_http_only");
    println!("sockets_bound=false");
    println!("storage_opened=false");
    println!("runtime_started=false");
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
        NodeRole::Edge => "edge",
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
    let mut bytes = Vec::with_capacity(text.len() / 2);
    for pair in text.as_bytes().chunks_exact(2) {
        let high = hex_value(pair[0]).ok_or("hex input contains non-hex digit")?;
        let low = hex_value(pair[1]).ok_or("hex input contains non-hex digit")?;
        bytes.push(high << 4 | low);
    }
    Ok(bytes)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[derive(Clone, Debug)]
struct CliOptions {
    listen: SocketAddr,
    websocket_path: String,
    storage: StorageConfig,
    auth_admission: AuthAdmissionConfig,
    upstream_url: Option<String>,
}

impl CliOptions {
    fn parse(args: Vec<String>, program: &str) -> Result<Self, String> {
        let mut options = Self::from_env()?;
        Self::parse_into(&mut options, args, program)?;
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
        let mut options = Self::from_env()?;
        options.storage = StorageConfig::data_dir("./data");
        options.websocket_path = format!("/apps/{app_id}/ws");
        Self::parse_into(&mut options, args, program)?;
        options.auth_admission.expected_audience = Some(app_id.to_owned());
        options.reject_unsupported_upstream_url()?;
        Ok(options)
    }

    fn parse_into(options: &mut Self, args: Vec<String>, program: &str) -> Result<(), String> {
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--listen" | "--bind" => {
                    options.listen = parse_socket_addr(&next_value(&mut args, &arg)?)?;
                }
                "--port" => {
                    let port = next_value(&mut args, &arg)?
                        .parse::<u16>()
                        .map_err(|error| format!("invalid --port: {error}"))?;
                    options.listen.set_port(port);
                }
                "--data-dir" | "--dataDir" => {
                    options.storage = StorageConfig::data_dir(next_value(&mut args, &arg)?);
                }
                "--in-memory" | "--memory" => {
                    options.storage = StorageConfig::InMemory;
                }
                "--websocket-path" | "--ws-path" => {
                    options.websocket_path = next_value(&mut args, &arg)?;
                }
                "--auth-static-bearer" | "--static-bearer" | "--admin-secret" => {
                    options.auth_admission =
                        AuthAdmissionConfig::static_bearer(next_value(&mut args, &arg)?);
                }
                "--auth-jwt-ed-public-key-pem" => {
                    options.auth_admission = AuthAdmissionConfig::jwt(
                        JwtVerifierConfig::ed_public_key_pem(next_value(&mut args, &arg)?),
                    );
                }
                "--allow-local-first-auth" => {
                    options.auth_admission.allow_local_first_auth =
                        parse_bool(&next_value(&mut args, &arg)?, &arg)?;
                }
                "--anonymous-subject" => {
                    options.auth_admission.anonymous_subject = next_value(&mut args, &arg)?;
                }
                "--upstream-url" => {
                    options.upstream_url = Some(next_value(&mut args, &arg)?);
                }
                "--help" | "-h" => return Err(format!("help_requested usage={program}")),
                _ if arg.starts_with("--listen=") => {
                    options.listen = parse_socket_addr(value_after_equals(&arg)?)?;
                }
                _ if arg.starts_with("--bind=") => {
                    options.listen = parse_socket_addr(value_after_equals(&arg)?)?;
                }
                _ if arg.starts_with("--port=") => {
                    let port = value_after_equals(&arg)?
                        .parse::<u16>()
                        .map_err(|error| format!("invalid --port: {error}"))?;
                    options.listen.set_port(port);
                }
                _ if arg.starts_with("--data-dir=") || arg.starts_with("--dataDir=") => {
                    options.storage = StorageConfig::data_dir(value_after_equals(&arg)?);
                }
                _ if arg.starts_with("--websocket-path=") || arg.starts_with("--ws-path=") => {
                    options.websocket_path = value_after_equals(&arg)?.to_owned();
                }
                _ if arg.starts_with("--auth-static-bearer=")
                    || arg.starts_with("--static-bearer=")
                    || arg.starts_with("--admin-secret=") =>
                {
                    options.auth_admission =
                        AuthAdmissionConfig::static_bearer(value_after_equals(&arg)?);
                }
                _ if arg.starts_with("--auth-jwt-ed-public-key-pem=") => {
                    options.auth_admission = AuthAdmissionConfig::jwt(
                        JwtVerifierConfig::ed_public_key_pem(value_after_equals(&arg)?),
                    );
                }
                _ if arg.starts_with("--allow-local-first-auth=") => {
                    options.auth_admission.allow_local_first_auth =
                        parse_bool(value_after_equals(&arg)?, "--allow-local-first-auth")?;
                }
                _ if arg.starts_with("--anonymous-subject=") => {
                    options.auth_admission.anonymous_subject = value_after_equals(&arg)?.to_owned();
                }
                _ if arg.starts_with("--upstream-url=") => {
                    options.upstream_url = Some(value_after_equals(&arg)?.to_owned());
                }
                _ => return Err(format!("unsupported_option={arg}")),
            }
        }
        Ok(())
    }

    fn from_env() -> Result<Self, String> {
        let mut options = Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 0)),
            websocket_path: "/sync".to_owned(),
            storage: StorageConfig::InMemory,
            auth_admission: AuthAdmissionConfig::default(),
            upstream_url: env::var("JAZZ_UPSTREAM_URL").ok(),
        };
        if let Ok(value) = env::var("JAZZ_SERVER_LISTEN") {
            options.listen = parse_socket_addr(&value)?;
        }
        if let Ok(value) = env::var("JAZZ_SERVER_PORT") {
            let port = value
                .parse::<u16>()
                .map_err(|error| format!("invalid JAZZ_SERVER_PORT: {error}"))?;
            options.listen.set_port(port);
        }
        if env_truthy("JAZZ_SERVER_IN_MEMORY") {
            options.storage = StorageConfig::InMemory;
        } else if let Ok(value) = env::var("JAZZ_SERVER_DATA_DIR") {
            options.storage = StorageConfig::data_dir(value);
        }
        if let Ok(value) = env::var("JAZZ_SERVER_WEBSOCKET_PATH") {
            options.websocket_path = value;
        }
        if let Ok(value) = env::var("JAZZ_SERVER_AUTH_STATIC_BEARER")
            .or_else(|_| env::var("JAZZ_ADMIN_SECRET"))
            .or_else(|_| env::var("JAZZ_BACKEND_SECRET"))
        {
            options.auth_admission = AuthAdmissionConfig::static_bearer(value);
        }
        if let Ok(value) = env::var("JAZZ_SERVER_AUTH_JWT_ED_PUBLIC_KEY_PEM") {
            options.auth_admission =
                AuthAdmissionConfig::jwt(JwtVerifierConfig::ed_public_key_pem(value));
        }
        if let Ok(value) = env::var("JAZZ_ALLOW_LOCAL_FIRST_AUTH") {
            options.auth_admission.allow_local_first_auth =
                parse_bool(&value, "JAZZ_ALLOW_LOCAL_FIRST_AUTH")?;
        }
        if let Ok(value) = env::var("JAZZ_SERVER_ANONYMOUS_SUBJECT") {
            options.auth_admission.anonymous_subject = value;
        }
        Ok(options)
    }

    fn reject_unsupported_upstream_url(&self) -> Result<(), String> {
        if let Some(url) = self.upstream_url.as_deref() {
            return Err(format!(
                "unsupported_upstream_url={url}: this jazz-server mode is local-only and does not relay to an upstream server"
            ));
        }
        Ok(())
    }
}

fn apply_shell_options(config: &mut jazz_server::ServerConfig, options: &CliOptions) {
    config.listener.bind_addr = options.listen;
    config.listener.websocket_path = options.websocket_path.clone();
    config.storage = options.storage.clone();
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing_value_for={flag}"))
}

fn value_after_equals(arg: &str) -> Result<&str, String> {
    arg.split_once('=')
        .map(|(_, value)| value)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("missing_value_for={arg}"))
}

fn parse_socket_addr(value: &str) -> Result<SocketAddr, String> {
    value
        .parse::<SocketAddr>()
        .map_err(|error| format!("invalid listen address {value:?}: {error}"))
}

fn env_truthy(name: &str) -> bool {
    env::var(name)
        .map(|value| {
            matches!(
                value.as_str(),
                "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
            )
        })
        .unwrap_or(false)
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
    fn server_app_parse_defaults_to_data_dir_and_app_ws_path() {
        let options = CliOptions::parse_for_server_app(Vec::new(), "jazz-server", "demo").unwrap();

        assert_eq!(options.listen, SocketAddr::from(([127, 0, 0, 1], 0)));
        assert_eq!(options.websocket_path, "/apps/demo/ws");
        assert!(
            matches!(options.storage, StorageConfig::RocksDb { ref path } if path == std::path::Path::new("./data"))
        );
    }

    #[test]
    fn server_app_parse_accepts_alpha_flags_and_in_memory_opt_out() {
        let options = CliOptions::parse_for_server_app(
            vec![
                "--listen".to_owned(),
                "127.0.0.1:9999".to_owned(),
                "--in-memory".to_owned(),
                "--admin-secret".to_owned(),
                "secret".to_owned(),
            ],
            "jazz-server",
            "demo",
        )
        .unwrap();

        assert_eq!(options.listen, SocketAddr::from(([127, 0, 0, 1], 9999)));
        assert_eq!(options.websocket_path, "/apps/demo/ws");
        assert_eq!(options.storage, StorageConfig::InMemory);
        assert!(options.auth_admission.static_bearer_token.is_some());
    }
}
