//! Jazz CLI - Create apps and run servers.
//!
//! # Commands
//!
//! ```text
//! jazz-tools create app [--name <NAME>]    # Returns AppId (random or deterministic from name)
//! jazz-tools server <APP_ID> [--port 1625] [--data-dir ./data] [--in-memory]
//! ```

mod commands;
#[cfg(feature = "otel")]
mod otel;

use clap::{Parser, Subcommand};
use jazz_tools::middleware::AuthConfig;
use jazz_tools::server::CatalogueAuthorityMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeEnvMode {
    Production,
    DevelopmentLike,
}

fn resolve_node_env_mode() -> NodeEnvMode {
    match std::env::var("NODE_ENV") {
        Ok(value) if value.eq_ignore_ascii_case("production") => NodeEnvMode::Production,
        _ => NodeEnvMode::DevelopmentLike,
    }
}

fn resolve_dev_default_flag(mode: NodeEnvMode, enabled_in_production: bool) -> bool {
    match mode {
        NodeEnvMode::Production => enabled_in_production,
        NodeEnvMode::DevelopmentLike => true,
    }
}

#[derive(Parser)]
#[command(name = "jazz-tools")]
#[command(bin_name = "jazz-tools")]
#[command(about = "Jazz distributed database CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
enum CatalogueAuthorityArg {
    Local,
    Forward,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new resource
    Create {
        #[command(subcommand)]
        resource: CreateResource,
    },
    /// Operate on the run-agent control-plane store with agent-specific commands
    AgentInfra(commands::agent_infra::AgentInfraCommand),
    /// Inspect and query a local Jazz database through a JSON-friendly CLI
    Db(commands::db::DbCommand),
    /// Run a Jazz server
    Server {
        /// Application ID (from `jazz-tools create app`)
        app_id: String,

        /// Port to listen on
        #[arg(short, long, default_value = "1625")]
        port: u16,

        /// Data directory for persistent storage (ignored if --in-memory)
        #[arg(short, long, default_value = "./data")]
        data_dir: String,

        /// Use in-memory storage instead of Fjall-backed files.
        #[arg(long)]
        in_memory: bool,

        /// URL to fetch JWKS keys for JWT validation (production)
        #[arg(long, env = "JAZZ_JWKS_URL")]
        jwks_url: Option<String>,

        /// Enable local-first auth (Authorization: Bearer <self-signed Jazz JWT>).
        ///
        /// Required in NODE_ENV=production.
        #[arg(long, env = "JAZZ_ALLOW_LOCAL_FIRST_AUTH")]
        allow_local_first_auth: bool,

        /// Secret for backend session impersonation
        #[arg(long, env = "JAZZ_BACKEND_SECRET")]
        backend_secret: Option<String>,

        /// Secret for admin operations (schema/policy sync)
        #[arg(long, env = "JAZZ_ADMIN_SECRET")]
        admin_secret: Option<String>,

        /// Whether this server is the catalogue authority or forwards admin catalogue requests upstream.
        #[arg(long, env = "JAZZ_CATALOGUE_AUTHORITY", default_value = "local")]
        catalogue_authority: CatalogueAuthorityArg,

        /// Base URL for the upstream catalogue authority when --catalogue-authority=forward.
        #[arg(long, env = "JAZZ_CATALOGUE_AUTHORITY_URL")]
        catalogue_authority_url: Option<String>,

        /// Admin secret used by this server when forwarding catalogue requests upstream.
        #[arg(long, env = "JAZZ_CATALOGUE_AUTHORITY_ADMIN_SECRET")]
        catalogue_authority_admin_secret: Option<String>,
    },
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

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let quiet_tracing = matches!(cli.command, Commands::Db(_) | Commands::AgentInfra(_));

    // Initialize tracing after command parsing so JSON-centric commands can
    // keep stdout clean by default.
    init_tracing(quiet_tracing);

    match cli.command {
        Commands::Create { resource } => match resource {
            CreateResource::App { name } => {
                commands::create::app(name);
            }
        },
        Commands::AgentInfra(command) => {
            if let Err(e) = commands::agent_infra::run(command) {
                eprintln!("Agent infra command error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Db(command) => {
            if let Err(e) = commands::db::run(command) {
                eprintln!("DB command error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Server {
            app_id,
            port,
            data_dir,
            in_memory,
            jwks_url,
            allow_local_first_auth,
            backend_secret,
            admin_secret,
            catalogue_authority,
            catalogue_authority_url,
            catalogue_authority_admin_secret,
        } => {
            let node_env_mode = resolve_node_env_mode();
            let allow_local_first_auth =
                resolve_dev_default_flag(node_env_mode, allow_local_first_auth);

            let auth_config = AuthConfig {
                jwks_url,
                allow_local_first_auth,
                backend_secret,
                admin_secret,
                ..Default::default()
            };
            let catalogue_authority = match catalogue_authority {
                CatalogueAuthorityArg::Local => CatalogueAuthorityMode::Local,
                CatalogueAuthorityArg::Forward => {
                    let base_url = match catalogue_authority_url {
                        Some(base_url) => base_url,
                        None => {
                            eprintln!(
                                "Server error: missing --catalogue-authority-url for --catalogue-authority=forward"
                            );
                            shutdown_tracing();
                            std::process::exit(1);
                        }
                    };
                    let admin_secret = match catalogue_authority_admin_secret {
                        Some(admin_secret) => admin_secret,
                        None => {
                            eprintln!(
                                "Server error: missing --catalogue-authority-admin-secret for --catalogue-authority=forward"
                            );
                            shutdown_tracing();
                            std::process::exit(1);
                        }
                    };
                    CatalogueAuthorityMode::Forward {
                        base_url,
                        admin_secret,
                    }
                }
            };
            if let Err(e) = commands::server::run(
                &app_id,
                port,
                &data_dir,
                in_memory,
                auth_config,
                catalogue_authority,
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

fn make_env_filter(quiet_defaults: bool) -> tracing_subscriber::EnvFilter {
    if std::env::var("RUST_LOG").is_ok() {
        return tracing_subscriber::EnvFilter::from_default_env();
    }

    let (jazz_level, jazz_tools_level, tower_http_level) = if quiet_defaults {
        ("error", "error", "error")
    } else {
        ("info", "info", "debug")
    };

    tracing_subscriber::EnvFilter::default()
        .add_directive(format!("jazz={jazz_level}").parse().unwrap())
        .add_directive(format!("jazz_tools={jazz_tools_level}").parse().unwrap())
        .add_directive(format!("tower_http={tower_http_level}").parse().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_command_parses_allow_local_first_auth_flag() {
        let cli = Cli::try_parse_from([
            "jazz-tools",
            "server",
            "test-app",
            "--allow-local-first-auth",
        ])
        .expect("server command should parse");

        match cli.command {
            Commands::Server {
                allow_local_first_auth,
                ..
            } => assert!(allow_local_first_auth),
            _ => panic!("expected server command"),
        }
    }

    #[test]
    fn dev_defaults_enable_local_first_auth() {
        assert!(resolve_dev_default_flag(
            NodeEnvMode::DevelopmentLike,
            false
        ));
    }

    #[test]
    fn production_requires_explicit_local_first_opt_in() {
        assert!(!resolve_dev_default_flag(NodeEnvMode::Production, false));
        assert!(resolve_dev_default_flag(NodeEnvMode::Production, true));
    }
}

#[cfg(feature = "otel")]
static OTEL_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();

fn init_tracing(quiet_defaults: bool) {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    #[cfg(feature = "otel")]
    {
        if std::env::var("JAZZ_OTEL").map_or(false, |v| v == "1") {
            let provider = otel::init_tracer_provider();
            let otel_layer = otel::layer(&provider);
            let _ = OTEL_PROVIDER.set(provider);
            tracing_subscriber::registry()
                .with(make_env_filter(quiet_defaults))
                .with(tracing_subscriber::fmt::layer())
                .with(otel_layer)
                .init();
            return;
        }
    }

    tracing_subscriber::registry()
        .with(make_env_filter(quiet_defaults))
        .with(tracing_subscriber::fmt::layer())
        .init();
}

fn shutdown_tracing() {
    #[cfg(feature = "otel")]
    {
        if let Some(provider) = OTEL_PROVIDER.get() {
            if let Err(e) = provider.shutdown() {
                eprintln!("OTel shutdown error: {e}");
            }
        }
    }
}
