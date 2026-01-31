//! Build command for schema evolution.
//!
//! Processes schema/current.sql and generates frozen schema files and lenses.
//!
//! # Usage
//!
//! ```text
//! jazz build [--schema-dir ./schema]
//! ```
//!
//! # Algorithm
//!
//! 1. Parse `schema/current.sql` → Schema
//! 2. Compute SchemaHash::compute(&schema) → new_hash
//! 3. Find existing `schema_*.sql` files, identify latest hash
//! 4. If new_hash matches latest: "Schema unchanged" → exit
//! 5. If no prior schema: write `schema_{new_hash}.sql` → exit
//! 6. If changed:
//!    a. Write `schema_{new_hash}.sql`
//!    b. Diff old schema vs new schema
//!    c. Generate `lens_{old}_{new}_fwd.sql`
//!    d. Generate `lens_{old}_{new}_bwd.sql`
//!    e. Mark ambiguous ops with `-- TODO: Review` comments

use groove::query_manager::types::SchemaHash;
use groove::schema_manager::{SchemaDirectory, diff_schemas};

/// Run the build command.
pub fn run(schema_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let dir = SchemaDirectory::new(schema_dir);

    // Check if current.sql exists
    if !dir.has_current() {
        return Err(format!(
            "No current.sql found in {}. Create one with CREATE TABLE statements.",
            schema_dir
        )
        .into());
    }

    // Parse current.sql
    let schema = dir.current_schema()?;
    let new_hash = SchemaHash::compute(&schema);

    println!("Parsed schema: {} table(s)", schema.len());
    println!("Schema hash: {}", new_hash.short());

    // Check for existing versions
    let latest = dir.latest_version()?;

    match latest {
        None => {
            // First schema version
            let path = dir.write_schema(&schema, new_hash)?;
            println!("Created frozen schema: {}", path.display());
            println!("This is the first schema version.");
        }
        Some(old_hash) if old_hash.short() == new_hash.short() => {
            println!("Schema unchanged (hash: {})", new_hash.short());
        }
        Some(old_hash) => {
            // Schema changed - generate new version and lens
            println!(
                "Schema changed: {} -> {}",
                old_hash.short(),
                new_hash.short()
            );

            // Write new frozen schema
            let schema_path = dir.write_schema(&schema, new_hash)?;
            println!("Created frozen schema: {}", schema_path.display());

            // Load old schema for diff
            let old_schema = dir.schema(old_hash)?;

            // Compute diff
            let diff_result = diff_schemas(&old_schema, &schema);

            // Report ambiguities
            if !diff_result.ambiguities.is_empty() {
                println!("\nAmbiguities detected (marked with TODO in lens files):");
                for amb in &diff_result.ambiguities {
                    println!("  - {}", amb);
                }
            }

            // Write lens files
            let (fwd_path, bwd_path) =
                dir.write_lens_pair(old_hash, new_hash, &diff_result.transform)?;
            println!("\nCreated lens files:");
            println!("  Forward:  {}", fwd_path.display());
            println!("  Backward: {}", bwd_path.display());

            // Summary
            println!("\nLens operations: {}", diff_result.transform.ops.len());
            if diff_result.transform.has_drafts() {
                println!("WARNING: Some operations are marked as drafts and need review.");
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use groove::schema_manager::Direction;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn build_first_schema() {
        let temp = TempDir::new().unwrap();
        let schema_dir = temp.path().join("schema");
        fs::create_dir_all(&schema_dir).unwrap();

        // Create current.sql
        fs::write(
            schema_dir.join("current.sql"),
            r#"
CREATE TABLE todos (
    title TEXT NOT NULL,
    completed BOOLEAN NOT NULL
);
            "#,
        )
        .unwrap();

        // Run build
        run(schema_dir.to_str().unwrap()).unwrap();

        // Verify frozen schema was created
        let dir = SchemaDirectory::new(&schema_dir);
        let versions = dir.schema_versions().unwrap();
        assert_eq!(versions.len(), 1);
    }

    #[test]
    fn build_unchanged_schema() {
        let temp = TempDir::new().unwrap();
        let schema_dir = temp.path().join("schema");
        fs::create_dir_all(&schema_dir).unwrap();

        // Create current.sql
        fs::write(
            schema_dir.join("current.sql"),
            r#"
CREATE TABLE todos (
    title TEXT NOT NULL
);
            "#,
        )
        .unwrap();

        // Run build twice
        run(schema_dir.to_str().unwrap()).unwrap();
        run(schema_dir.to_str().unwrap()).unwrap();

        // Should still have only one version
        let dir = SchemaDirectory::new(&schema_dir);
        let versions = dir.schema_versions().unwrap();
        assert_eq!(versions.len(), 1);
    }

    #[test]
    fn build_schema_evolution() {
        let temp = TempDir::new().unwrap();
        let schema_dir = temp.path().join("schema");
        fs::create_dir_all(&schema_dir).unwrap();

        // Version 1
        fs::write(
            schema_dir.join("current.sql"),
            r#"
CREATE TABLE todos (
    title TEXT NOT NULL
);
            "#,
        )
        .unwrap();
        run(schema_dir.to_str().unwrap()).unwrap();

        // Version 2 - add column
        fs::write(
            schema_dir.join("current.sql"),
            r#"
CREATE TABLE todos (
    title TEXT NOT NULL,
    completed BOOLEAN NOT NULL
);
            "#,
        )
        .unwrap();
        run(schema_dir.to_str().unwrap()).unwrap();

        // Should have two versions and lens files
        let dir = SchemaDirectory::new(&schema_dir);
        let versions = dir.schema_versions().unwrap();
        assert_eq!(versions.len(), 2);

        // Check lens exists
        assert!(dir.has_lens(versions[0], versions[1], Direction::Forward));
        assert!(dir.has_lens(versions[0], versions[1], Direction::Backward));
    }

    #[test]
    fn build_missing_current() {
        let temp = TempDir::new().unwrap();
        let schema_dir = temp.path().join("schema");
        fs::create_dir_all(&schema_dir).unwrap();

        // No current.sql
        let result = run(schema_dir.to_str().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("current.sql"));
    }
}
