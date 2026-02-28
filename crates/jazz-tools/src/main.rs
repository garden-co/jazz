//! Jazz CLI - Create apps and run servers.
//!
//! # Commands
//!
//! ```text
//! jazz-tools create app [--name <NAME>]    # Returns AppId (random or deterministic from name)
//! jazz-tools server <APP_ID> [--port 1625] [--data-dir ./data] [--in-memory]
//! ```

mod commands;
mod middleware;
#[cfg(feature = "otel")]
mod otel;
mod routes;

use clap::{Parser, Subcommand};
use middleware::AuthConfig;

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

#[derive(Subcommand)]
enum Commands {
    /// Build schema files and generate lenses
    Build {
        /// Path to schema directory
        #[arg(long, default_value = "./schema")]
        schema_dir: String,

        /// Generate TypeScript lens stubs instead of SQL lens files
        #[arg(long)]
        ts: bool,
    },
    /// Push schema catalogue objects to a sync server
    #[command(name = "schema:push")]
    SchemaPush {
        /// Application ID
        app_id: String,

        /// Sync server URL
        #[arg(long)]
        server_url: String,

        /// Secret for admin operations (schema/policy sync)
        #[arg(long, env = "JAZZ_ADMIN_SECRET")]
        admin_secret: String,

        /// Environment name
        #[arg(long, default_value = "dev")]
        env: String,

        /// User branch name
        #[arg(long, default_value = "main")]
        user_branch: String,

        /// Path to schema directory
        #[arg(long, default_value = "./schema")]
        schema_dir: String,
    },
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

        /// Use a temporary directory for storage (ephemeral, created on the fly)
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
        Commands::Build { schema_dir, ts } => {
            if let Err(e) = commands::build::run(&schema_dir, ts) {
                eprintln!("Build error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::SchemaPush {
            server_url,
            app_id,
            admin_secret,
            env,
            user_branch,
            schema_dir,
        } => {
            if let Err(e) = commands::schema_push::run(
                &server_url,
                &app_id,
                &env,
                &user_branch,
                &admin_secret,
                &schema_dir,
            )
            .await
            {
                eprintln!("Schema push error: {}", e);
                std::process::exit(1);
            }
        }
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
        } => {
            let data_dir = if in_memory {
                let tmp =
                    std::env::temp_dir().join(format!("jazz-server-{}", uuid::Uuid::new_v4()));
                std::fs::create_dir_all(&tmp).expect("failed to create temp dir for --in-memory");
                tmp.into_os_string()
                    .into_string()
                    .expect("temp path is valid UTF-8")
            } else {
                data_dir
            };

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
                jwks_set: None,
                allow_anonymous,
                allow_demo,
                backend_secret,
                admin_secret,
            };
            if let Err(e) = commands::server::run(&app_id, port, &data_dir, auth_config).await {
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
