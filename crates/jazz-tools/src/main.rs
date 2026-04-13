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

        /// Enable anonymous local auth (X-Jazz-Local-Mode: anonymous).
        ///
        /// Required in NODE_ENV=production.
        #[arg(long, env = "JAZZ_ALLOW_ANONYMOUS")]
        allow_anonymous: bool,

        /// Enable demo local auth (X-Jazz-Local-Mode: demo).
        ///
        /// Required in NODE_ENV=production.
        #[arg(long, env = "JAZZ_ALLOW_DEMO")]
        allow_demo: bool,

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
    // Initialize tracing with layered subscriber
    init_tracing();

    let cli = Cli::parse();

    match cli.command {
        Commands::Create { resource } => match resource {
            CreateResource::App { name } => {
                commands::create::app(name);
            }
        },
        Commands::Server {
            app_id,
            port,
            data_dir,
            in_memory,
            jwks_url,
            allow_anonymous,
            allow_demo,
            backend_secret,
            admin_secret,
            catalogue_authority,
            catalogue_authority_url,
            catalogue_authority_admin_secret,
        } => {
            let node_env_mode = resolve_node_env_mode();
            let allow_anonymous = match node_env_mode {
                NodeEnvMode::Production => allow_anonymous,
                NodeEnvMode::DevelopmentLike => true,
            };
            let allow_demo = match node_env_mode {
                NodeEnvMode::Production => allow_demo,
                NodeEnvMode::DevelopmentLike => true,
            };

            let auth_config = AuthConfig {
                jwks_url,
                allow_anonymous,
                allow_demo,
                allow_local_first_auth: true,
                backend_secret,
                admin_secret,
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

fn make_env_filter() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::from_default_env()
        .add_directive("jazz=info".parse().unwrap())
        .add_directive("jazz_tools=info".parse().unwrap())
        .add_directive("tower_http=debug".parse().unwrap())
}

#[cfg(feature = "otel")]
static OTEL_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();

fn init_tracing() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    #[cfg(feature = "otel")]
    {
        if std::env::var("JAZZ_OTEL").map_or(false, |v| v == "1") {
            let provider = otel::init_tracer_provider();
            let otel_layer = otel::layer(&provider);
            let _ = OTEL_PROVIDER.set(provider);
            tracing_subscriber::registry()
                .with(make_env_filter())
                .with(tracing_subscriber::fmt::layer())
                .with(otel_layer)
                .init();
            return;
        }
    }

    tracing_subscriber::registry()
        .with(make_env_filter())
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
