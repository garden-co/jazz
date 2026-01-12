//! Jazz CLI - Command-line interface for Jazz/Groove database operations
//!
//! Provides commands for:
//! - Schema migrations (`jazz migrate`)
//! - Schema inspection (`jazz schema`)

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use colored::Colorize;
use groove::sql::{
    diff_schemas, generate_lens, parse, ColumnDef, ColumnTransform, ColumnType, CreateTable, Lens,
    LensGenerationOptions, Statement, TableSchema,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

/// Jazz CLI - Database management tool
#[derive(Parser)]
#[command(name = "jazz")]
#[command(about = "Jazz database CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Schema migration commands
    Migrate {
        #[command(subcommand)]
        action: MigrateAction,
    },
}

#[derive(Subcommand)]
enum MigrateAction {
    /// Show current schema status for a table
    Status {
        /// Table name
        table: String,

        /// Server URL
        #[arg(long, default_value = "http://localhost:8080")]
        server: String,
    },

    /// Preview schema changes (diff between local and server)
    Diff {
        /// Table name
        table: String,

        /// Path to schema SQL file
        #[arg(long, short)]
        file: PathBuf,

        /// Server URL
        #[arg(long, default_value = "http://localhost:8080")]
        server: String,
    },

    /// Deploy schema migration to server
    Push {
        /// Table name
        table: String,

        /// Path to schema SQL file
        #[arg(long, short)]
        file: PathBuf,

        /// Environment (dev, staging, prod)
        #[arg(long, short, default_value = "dev")]
        env: String,

        /// Server URL
        #[arg(long, default_value = "http://localhost:8080")]
        server: String,

        /// Skip confirmation prompt
        #[arg(long, short)]
        yes: bool,
    },
}

// API response types
#[derive(Debug, Deserialize)]
struct SchemaResponse {
    descriptor_id: String,
    columns: Vec<ColumnInfo>,
    parent_descriptors: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct ColumnInfo {
    name: String,
    column_type: String,
    nullable: bool,
}

#[derive(Debug, Serialize)]
struct DeployRequest {
    schema: SchemaForServer,
    environment: String,
    lens: Option<LensForServer>,
}

#[derive(Debug, Serialize)]
struct SchemaForServer {
    columns: Vec<ColumnForServer>,
}

#[derive(Debug, Serialize)]
struct ColumnForServer {
    name: String,
    column_type: String,
    nullable: bool,
}

#[derive(Debug, Serialize)]
struct LensForServer {
    forward: Vec<TransformForServer>,
    backward: Vec<TransformForServer>,
}

#[derive(Debug, Serialize)]
struct TransformForServer {
    transform_type: String,
    from: Option<String>,
    to: Option<String>,
    column: Option<String>,
    default_value: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DeployResponse {
    new_descriptor_id: String,
    rows_migrated: u64,
    warnings: Vec<WarningInfo>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct WarningInfo {
    kind: String,
    message: String,
    column: Option<String>,
}

fn get_api_key() -> Result<String> {
    std::env::var("JAZZ_API_KEY").context(
        "JAZZ_API_KEY environment variable not set. \
        Get an API key from your Jazz server admin.",
    )
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Migrate { action } => match action {
            MigrateAction::Status { table, server } => migrate_status(&table, &server),
            MigrateAction::Diff {
                table,
                file,
                server,
            } => migrate_diff(&table, &file, &server),
            MigrateAction::Push {
                table,
                file,
                env,
                server,
                yes,
            } => migrate_push(&table, &file, &env, &server, yes),
        },
    }
}

/// Fetch and display current schema status from server
fn migrate_status(table: &str, server: &str) -> Result<()> {
    println!(
        "{} Fetching schema status for table: {}",
        "INFO".blue(),
        table.cyan()
    );

    let api_key = get_api_key()?;
    let client = reqwest::blocking::Client::new();

    let response = client
        .get(format!("{}/api/schema/{}", server, table))
        .header("Authorization", format!("Bearer {}", api_key))
        .send()
        .context("Failed to connect to server")?;

    if !response.status().is_success() {
        let error = response.text().unwrap_or_default();
        anyhow::bail!("Failed to fetch schema: {}", error);
    }

    let data: SchemaResponse = response.json().context("Failed to parse response")?;

    println!("\n{}", "Current Schema:".bold());
    println!("  Table: {}", table.cyan());
    println!("  Descriptor ID: {}", data.descriptor_id.yellow());
    println!("  Columns:");

    for col in &data.columns {
        let nullable = if col.nullable { " (nullable)" } else { "" };
        println!(
            "    - {}: {}{}",
            col.name.green(),
            col.column_type,
            nullable.dimmed()
        );
    }

    if let Some(parents) = &data.parent_descriptors {
        if !parents.is_empty() {
            println!("  Parent Descriptors: {}", parents.join(", ").dimmed());
        }
    }

    Ok(())
}

/// Parse a schema SQL file and extract the schema for a specific table
fn parse_schema_from_file(path: &PathBuf, table: &str) -> Result<TableSchema> {
    let content = fs::read_to_string(path).context("Failed to read schema file")?;

    // Parse all CREATE TABLE statements (split by semicolon like WASM does)
    for stmt_str in content
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        let stmt = parse(stmt_str).context("Failed to parse SQL")?;

        if let Statement::CreateTable(CreateTable { name, columns }) = stmt {
            if name.eq_ignore_ascii_case(table) {
                return Ok(TableSchema::new(&name, columns));
            }
        }
    }

    anyhow::bail!("Table '{}' not found in schema file", table)
}

/// Convert server schema response to TableSchema
fn schema_from_response(table: &str, response: &SchemaResponse) -> TableSchema {
    let columns: Vec<ColumnDef> = response
        .columns
        .iter()
        .map(|col| {
            ColumnDef::new(
                col.name.clone(),
                parse_column_type(&col.column_type),
                col.nullable,
            )
        })
        .collect();
    TableSchema::new(table, columns)
}

fn parse_column_type(s: &str) -> ColumnType {
    match s.to_uppercase().as_str() {
        "I64" => ColumnType::I64,
        "F64" => ColumnType::F64,
        "STRING" => ColumnType::String,
        "BOOL" => ColumnType::Bool,
        "BYTES" => ColumnType::Bytes,
        _ => ColumnType::String, // fallback
    }
}

/// Preview schema changes between local file and server
fn migrate_diff(table: &str, file: &PathBuf, server: &str) -> Result<()> {
    println!(
        "{} Computing diff for table: {}",
        "INFO".blue(),
        table.cyan()
    );

    let api_key = get_api_key()?;
    let client = reqwest::blocking::Client::new();

    // Fetch current schema from server
    let response = client
        .get(format!("{}/api/schema/{}", server, table))
        .header("Authorization", format!("Bearer {}", api_key))
        .send()
        .context("Failed to connect to server")?;

    if !response.status().is_success() {
        let error = response.text().unwrap_or_default();
        anyhow::bail!("Failed to fetch current schema: {}", error);
    }

    let server_schema: SchemaResponse = response.json().context("Failed to parse response")?;
    let old_schema = schema_from_response(table, &server_schema);

    // Parse local schema
    let new_schema = parse_schema_from_file(file, table)?;

    // Compute diff using groove's diff function
    let diff = diff_schemas(&old_schema, &new_schema);

    println!("\n{}", "Schema Diff:".bold());

    let mut has_changes = false;

    if !diff.added.is_empty() {
        has_changes = true;
        println!("\n  {}:", "Added Columns".green());
        for col in &diff.added {
            println!("    {} {} ({:?})", "+".green(), col.name, col.ty);
        }
    }

    if !diff.removed.is_empty() {
        has_changes = true;
        println!("\n  {}:", "Removed Columns".red());
        for col in &diff.removed {
            println!("    {} {} ({:?})", "-".red(), col.name, col.ty);
        }
    }

    if !diff.potential_renames.is_empty() {
        has_changes = true;
        println!("\n  {}:", "Potential Renames".yellow());
        for rename in &diff.potential_renames {
            println!(
                "    {} -> {} ({})",
                rename.old_name.red(),
                rename.new_name.green(),
                format!("{:?}", rename.confidence).dimmed()
            );
        }
    }

    if !diff.type_changes.is_empty() {
        has_changes = true;
        println!("\n  {}:", "Type Changes".yellow());
        for change in &diff.type_changes {
            println!(
                "    {}: {:?} -> {:?}",
                change.column, change.old_type, change.new_type
            );
        }
    }

    if !has_changes {
        println!("  {}", "No changes detected".dimmed());
    } else {
        // Generate and show lens
        let result = generate_lens(&diff, &LensGenerationOptions::default());

        if !result.lens.forward.is_empty() {
            println!("\n  {}:", "Generated Lens".blue());
            println!("    Forward transforms:");
            for transform in &result.lens.forward {
                println!("      - {:?}", transform);
            }
            println!("    Backward transforms:");
            for transform in &result.lens.backward {
                println!("      - {:?}", transform);
            }
        }

        if !result.warnings.is_empty() {
            println!("\n  {}:", "Warnings".yellow());
            for warning in &result.warnings {
                println!("    - {:?}", warning);
            }
        }
    }

    Ok(())
}

/// Deploy schema migration to server
fn migrate_push(table: &str, file: &PathBuf, env: &str, server: &str, yes: bool) -> Result<()> {
    println!(
        "{} Deploying schema migration for table: {}",
        "INFO".blue(),
        table.cyan()
    );
    println!("  Environment: {}", env.yellow());

    let api_key = get_api_key()?;
    let client = reqwest::blocking::Client::new();

    // Fetch current schema from server
    let response = client
        .get(format!("{}/api/schema/{}", server, table))
        .header("Authorization", format!("Bearer {}", api_key))
        .send()
        .context("Failed to connect to server")?;

    if !response.status().is_success() {
        let error = response.text().unwrap_or_default();
        anyhow::bail!("Failed to fetch current schema: {}", error);
    }

    let server_schema: SchemaResponse = response.json().context("Failed to parse response")?;
    let old_schema = schema_from_response(table, &server_schema);

    // Parse local schema
    let new_schema = parse_schema_from_file(file, table)?;

    // Compute diff and generate lens locally
    let diff = diff_schemas(&old_schema, &new_schema);
    let result = generate_lens(&diff, &LensGenerationOptions::default());
    let lens = result.lens;
    let warnings = result.warnings;

    // Show preview
    println!("\n{}", "Migration Preview:".bold());

    if !diff.added.is_empty() {
        let names: Vec<_> = diff.added.iter().map(|c| c.name.as_str()).collect();
        println!("  Added: {}", names.join(", ").green());
    }
    if !diff.removed.is_empty() {
        let names: Vec<_> = diff.removed.iter().map(|c| c.name.as_str()).collect();
        println!("  Removed: {}", names.join(", ").red());
    }
    if !diff.potential_renames.is_empty() {
        for rename in &diff.potential_renames {
            println!(
                "  Rename: {} -> {}",
                rename.old_name.red(),
                rename.new_name.green()
            );
        }
    }

    if !warnings.is_empty() {
        println!("\n{}:", "Warnings".yellow());
        for warning in &warnings {
            println!("  - {:?}", warning);
        }
    }

    // Confirmation
    if !yes {
        println!(
            "\n{}",
            "This will deploy the migration to the server.".yellow()
        );
        println!("Use {} flag to skip this confirmation.", "--yes".cyan());

        // Simple confirmation prompt
        print!("\nProceed? [y/N] ");
        use std::io::Write;
        std::io::stdout().flush()?;

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("{}", "Aborted.".red());
            return Ok(());
        }
    }

    // Convert schema and lens for server
    let schema_for_server = SchemaForServer {
        columns: new_schema
            .columns
            .iter()
            .map(|col| ColumnForServer {
                name: col.name.clone(),
                column_type: format!("{:?}", col.ty),
                nullable: col.nullable,
            })
            .collect(),
    };

    let lens_for_server = if lens.forward.is_empty() && lens.backward.is_empty() {
        None
    } else {
        Some(convert_lens_for_server(&lens))
    };

    let deploy_request = DeployRequest {
        schema: schema_for_server,
        environment: env.to_string(),
        lens: lens_for_server,
    };

    // Deploy
    println!("\n{} Executing migration...", "INFO".blue());

    let response = client
        .post(format!("{}/api/schema/{}/deploy", server, table))
        .header("Authorization", format!("Bearer {}", api_key))
        .json(&deploy_request)
        .send()
        .context("Failed to connect to server")?;

    if !response.status().is_success() {
        let error = response.text().unwrap_or_default();
        anyhow::bail!("Failed to deploy migration: {}", error);
    }

    let result: DeployResponse = response.json().context("Failed to parse response")?;

    println!("\n{}", "Migration deployed successfully!".green().bold());
    println!("  New Descriptor ID: {}", result.new_descriptor_id.yellow());
    println!("  Rows Migrated: {}", result.rows_migrated);

    if !result.warnings.is_empty() {
        println!("\n  {}:", "Warnings".yellow());
        for warning in &result.warnings {
            let col = warning
                .column
                .as_ref()
                .map(|c| format!(" ({})", c))
                .unwrap_or_default();
            println!("    - {}{}", warning.message, col.dimmed());
        }
    }

    Ok(())
}

fn convert_lens_for_server(lens: &Lens) -> LensForServer {
    let convert_transforms = |transforms: &[ColumnTransform]| -> Vec<TransformForServer> {
        transforms
            .iter()
            .map(|t| match t {
                ColumnTransform::Rename { from, to } => TransformForServer {
                    transform_type: "rename".to_string(),
                    from: Some(from.clone()),
                    to: Some(to.clone()),
                    column: None,
                    default_value: None,
                },
                ColumnTransform::Add { name, default } => TransformForServer {
                    transform_type: "add".to_string(),
                    from: None,
                    to: None,
                    column: Some(name.clone()),
                    default_value: default.as_ref().map(|d| format!("{:?}", d)),
                },
                ColumnTransform::Remove { name } => TransformForServer {
                    transform_type: "remove".to_string(),
                    from: None,
                    to: None,
                    column: Some(name.clone()),
                    default_value: None,
                },
                ColumnTransform::Transform { column, expr, .. } => TransformForServer {
                    transform_type: "transform".to_string(),
                    from: None,
                    to: None,
                    column: Some(column.clone()),
                    default_value: Some(format!("{:?}", expr)),
                },
            })
            .collect()
    };

    LensForServer {
        forward: convert_transforms(&lens.forward),
        backward: convert_transforms(&lens.backward),
    }
}
