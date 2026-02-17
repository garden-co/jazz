//! Jazz CLI - Create apps and run servers.
//!
//! # Commands
//!
//! ```text
//! jazz-tools create app [--name <NAME>]    # Returns AppId (random or deterministic from name)
//! jazz-tools server <APP_ID> [--port 1625] [--data-dir ./data]
//! ```

mod commands;
mod middleware;
mod routes;

use clap::{Parser, Subcommand};
use middleware::AuthConfig;

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

        /// Data directory for persistent storage
        #[arg(short, long, default_value = "./data")]
        data_dir: String,

        /// HMAC secret for JWT validation (development/testing)
        #[arg(long, env = "JAZZ_JWT_SECRET")]
        jwt_secret: Option<String>,

        /// URL to fetch JWKS keys for JWT validation (production)
        #[arg(long, env = "JAZZ_JWKS_URL")]
        jwks_url: Option<String>,

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
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("jazz=info".parse().unwrap())
                .add_directive("jazz_tools=info".parse().unwrap())
                .add_directive("tower_http=debug".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Build { schema_dir, ts } => {
            if let Err(e) = commands::build::run(&schema_dir, ts) {
                eprintln!("Build error: {}", e);
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
            jwt_secret,
            jwks_url,
            backend_secret,
            admin_secret,
        } => {
            let auth_config = AuthConfig {
                jwt_secret,
                jwks_url,
                backend_secret,
                admin_secret,
            };
            if let Err(e) = commands::server::run(&app_id, port, &data_dir, auth_config).await {
                eprintln!("Server error: {}", e);
                std::process::exit(1);
            }
        }
    }
}
