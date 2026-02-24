//! Build command for schema evolution.
//!
//! Processes schema/current.sql and generates frozen schema files and migrations.
//!
//! # Usage
//!
//! ```text
//! jazz-tools build [--schema-dir ./schema]
//! ```
//!
//! # Algorithm
//!
//! 1. Load all `schema_v*_*.sql` files
//! 2. For each file:
//!    a. Parse: version number, optional description, hash from filename
//!    b. Compute actual hash from content
//!    c. If filename hash ≠ computed hash → Error (frozen schemas must not be modified)
//! 3. Group schemas by version, validate versions start at 1 with no gaps
//!    (Multiple schemas at the same version are allowed - e.g., after branch merge)
//!
//! 4. Compute hash of current.sql
//! 5. Find if any schema file has this hash in its filename
//!    - If match exists: "Schema unchanged (matches vN)"
//!    - If no match: Create schema_vN_{hash}.sql (N = max_version + 1)
//!
//! 6. For each adjacent version pair (v1→v2, v2→v3...):
//!    - Generate migrations from ALL schemas at vN to ALL schemas at vN+1
//!    - If --ts: generate TypeScript migration stubs if missing
//!    - Else: generate SQL migration files if missing
//!
//! 7. Report results
//!
//! # Branch Merge Scenario
//!
//! After merging two branches that each created v2:
//! ```text
//! schema_v1_aaa.sql           (common ancestor)
//! schema_v2_feature_a_bbb.sql (from branch A)
//! schema_v2_feature_b_ccc.sql (from branch B)
//! schema_v3_ddd.sql           (merged result)
//! ```
//!
//! Build will generate migrations:
//! - v1→v2_a, v1→v2_b (if not already present)
//! - v2_a→v3, v2_b→v3 (so users on either branch can migrate to merged state)

use std::fs;

use jazz::query_manager::types::SchemaHash;
use jazz::schema_manager::{
    Direction, SchemaDirectory, SchemaFileInfo, diff_schemas, parse_schema, schema_filename,
};

/// Run the build command.
///
/// If `ts` is true, generates TypeScript migration stubs instead of SQL migration files.
pub fn run(schema_dir: &str, ts: bool) -> Result<(), Box<dyn std::error::Error>> {
    let dir = SchemaDirectory::new(schema_dir);

    // Check if current.sql exists
    if !dir.has_current() {
        return Err(format!(
            "No current.sql found in {}. Create one with CREATE TABLE statements.",
            schema_dir
        )
        .into());
    }

    // Step 1-3: Load and validate all existing schema versions
    let versions = dir.schema_versions()?;
    let mut validated_versions: Vec<SchemaFileInfo> = Vec::new();

    for info in &versions {
        // Read the file content and compute hash
        let filename = schema_filename(info);
        let path = dir.path().join(&filename);
        let content = fs::read_to_string(&path)?;
        let schema = parse_schema(&content)?;
        let computed_hash = SchemaHash::compute(&schema);

        // Validate hash matches filename
        if computed_hash.short() != info.hash {
            return Err(format!(
                "Hash mismatch for {}: filename has {} but content hashes to {}. \
                Frozen schemas must not be edited.",
                filename,
                info.hash,
                computed_hash.short()
            )
            .into());
        }

        validated_versions.push(info.clone());
    }

    // Group schemas by version and validate no gaps
    // Multiple schemas at the same version are allowed (branch merge scenario)
    let mut versions_seen: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
    for info in &validated_versions {
        versions_seen.insert(info.version);
    }

    // Validate versions start at 1 and have no gaps
    if let Some(&first) = versions_seen.first()
        && first != 1
    {
        return Err(format!(
            "Schema versions must start at v1. Found v{} instead.",
            first
        )
        .into());
    }

    let versions_vec: Vec<u32> = versions_seen.iter().copied().collect();
    for i in 1..versions_vec.len() {
        if versions_vec[i] != versions_vec[i - 1] + 1 {
            return Err(format!(
                "Gap in schema versions: v{} -> v{}. Versions must be sequential.",
                versions_vec[i - 1],
                versions_vec[i]
            )
            .into());
        }
    }

    // Step 4: Parse current.sql and compute hash
    let schema = dir.current_schema()?;
    let new_hash = SchemaHash::compute(&schema);

    println!("Parsed schema: {} table(s)", schema.len());
    println!("Schema hash: {}", new_hash.short());

    // Step 5: Check if this hash already exists
    let existing_version = validated_versions
        .iter()
        .find(|v| v.hash == new_hash.short());

    let latest_version = validated_versions.last().map(|v| v.version).unwrap_or(0);

    match existing_version {
        Some(existing) => {
            println!(
                "Schema unchanged (matches v{}, hash: {})",
                existing.version, existing.hash
            );
        }
        None => {
            // Create new schema version
            let new_version = latest_version + 1;
            let path = dir.write_schema(&schema, new_version, None, &new_hash.short())?;
            println!("Created frozen schema: {}", path.display());

            // Add to validated versions for migration generation
            validated_versions.push(SchemaFileInfo {
                version: new_version,
                description: None,
                hash: new_hash.short().to_string(),
            });

            if new_version > 1 {
                println!(
                    "Schema changed: v{} ({}) -> v{} ({})",
                    new_version - 1,
                    validated_versions[validated_versions.len() - 2].hash,
                    new_version,
                    new_hash.short()
                );
            } else {
                println!("This is the first schema version.");
            }
        }
    }

    // Step 6: Generate migrations for adjacent version pairs
    // For each version N, generate migrations from ALL schemas at N to ALL schemas at N+1
    let max_version = validated_versions.last().map(|v| v.version).unwrap_or(0);

    for version in 1..max_version {
        let from_schemas: Vec<&SchemaFileInfo> = validated_versions
            .iter()
            .filter(|v| v.version == version)
            .collect();
        let to_schemas: Vec<&SchemaFileInfo> = validated_versions
            .iter()
            .filter(|v| v.version == version + 1)
            .collect();

        // Generate migration for each (from, to) pair
        for from in &from_schemas {
            for to in &to_schemas {
                generate_migration_if_needed(&dir, from, to, ts)?;
            }
        }
    }

    Ok(())
}

/// Generate migration files for a version pair if they don't exist.
fn generate_migration_if_needed(
    dir: &SchemaDirectory,
    from: &SchemaFileInfo,
    to: &SchemaFileInfo,
    ts: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let has_ts_stub = dir.has_migration_ts_stub(from.version, to.version, &from.hash, &to.hash);
    let has_fwd_sql = dir.has_migration_sql(
        from.version,
        to.version,
        &from.hash,
        &to.hash,
        Direction::Forward,
    );
    let has_bwd_sql = dir.has_migration_sql(
        from.version,
        to.version,
        &from.hash,
        &to.hash,
        Direction::Backward,
    );

    if has_fwd_sql && has_bwd_sql {
        // Migration SQL files already exist
        return Ok(());
    }

    // Load schemas for diffing
    let from_schema = dir.schema_by_version(from.version)?;
    let to_schema = dir.schema_by_version(to.version)?;
    let diff_result = diff_schemas(&from_schema, &to_schema);

    if ts {
        // TypeScript mode: generate TS stub if missing
        if !has_ts_stub {
            let ts_path = dir.write_migration_ts_stub(
                from.version,
                to.version,
                &from.hash,
                &to.hash,
                &diff_result.transform,
            )?;
            println!("Created TypeScript migration stub: {}", ts_path.display());

            // Report ambiguities
            if !diff_result.ambiguities.is_empty() {
                println!("  Ambiguities (marked with TODO):");
                for amb in &diff_result.ambiguities {
                    println!("    - {}", amb);
                }
            }

            if diff_result.transform.has_drafts() {
                println!("  WARNING: Some operations are marked as drafts and need review.");
            }

            println!(
                "  Review the generated migration, then run the TypeScript schema build command again to generate SQL."
            );
        }
        // Note: When TS stub exists but SQL files are missing, the TypeScript CLI
        // will handle converting the TS stub to SQL files.
    } else {
        // Pure SQL mode: generate SQL files directly
        let (fwd_path, bwd_path) = dir.write_migration_sql_pair(
            from.version,
            to.version,
            &from.hash,
            &to.hash,
            &diff_result.transform,
        )?;
        println!("Created migration SQL files:");
        println!("  Forward:  {}", fwd_path.display());
        println!("  Backward: {}", bwd_path.display());

        // Report ambiguities
        if !diff_result.ambiguities.is_empty() {
            println!("  Ambiguities (marked with TODO in migration files):");
            for amb in &diff_result.ambiguities {
                println!("    - {}", amb);
            }
        }

        println!(
            "  Migration operations: {}",
            diff_result.transform.ops.len()
        );
        if diff_result.transform.has_drafts() {
            println!("  WARNING: Some operations are marked as drafts and need review.");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz::schema_manager::Direction;
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
        run(schema_dir.to_str().unwrap(), false).unwrap();

        // Verify frozen schema was created with versioned name
        let dir = SchemaDirectory::new(&schema_dir);
        let versions = dir.schema_versions().unwrap();
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].version, 1);

        // Check the file exists with proper name
        let filename = schema_filename(&versions[0]);
        assert!(schema_dir.join(&filename).exists());
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
        run(schema_dir.to_str().unwrap(), false).unwrap();
        run(schema_dir.to_str().unwrap(), false).unwrap();

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
        run(schema_dir.to_str().unwrap(), false).unwrap();

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
        run(schema_dir.to_str().unwrap(), false).unwrap();

        // Should have two versions and migration files
        let dir = SchemaDirectory::new(&schema_dir);
        let versions = dir.schema_versions().unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].version, 1);
        assert_eq!(versions[1].version, 2);

        // Check migration files exist
        assert!(dir.has_migration_sql(
            versions[0].version,
            versions[1].version,
            &versions[0].hash,
            &versions[1].hash,
            Direction::Forward
        ));
        assert!(dir.has_migration_sql(
            versions[0].version,
            versions[1].version,
            &versions[0].hash,
            &versions[1].hash,
            Direction::Backward
        ));
    }

    #[test]
    fn build_detects_modified_frozen_schema() {
        let temp = TempDir::new().unwrap();
        let schema_dir = temp.path().join("schema");
        fs::create_dir_all(&schema_dir).unwrap();

        // Create current.sql and build
        fs::write(
            schema_dir.join("current.sql"),
            r#"
CREATE TABLE todos (
    title TEXT NOT NULL
);
            "#,
        )
        .unwrap();
        run(schema_dir.to_str().unwrap(), false).unwrap();

        // Get the frozen schema filename
        let dir = SchemaDirectory::new(&schema_dir);
        let versions = dir.schema_versions().unwrap();
        let filename = schema_filename(&versions[0]);

        // Modify the frozen schema (this is not allowed!)
        fs::write(
            schema_dir.join(&filename),
            r#"
CREATE TABLE todos (
    title TEXT NOT NULL,
    description TEXT
);
            "#,
        )
        .unwrap();

        // Build should fail with hash mismatch
        let result = run(schema_dir.to_str().unwrap(), false);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Hash mismatch"));
        assert!(err.contains("Frozen schemas must not be edited"));
    }

    #[test]
    fn build_missing_current() {
        let temp = TempDir::new().unwrap();
        let schema_dir = temp.path().join("schema");
        fs::create_dir_all(&schema_dir).unwrap();

        // No current.sql
        let result = run(schema_dir.to_str().unwrap(), false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("current.sql"));
    }

    #[test]
    fn build_branch_merge_scenario() {
        // Simulates merging two branches that each created v2 with different schemas.
        // After merge, we have:
        //   v1 (common ancestor)
        //   v2_feature_a (from branch A)
        //   v2_feature_b (from branch B)
        //   v3 (merged result in current.sql)
        //
        // Build should generate migrations:
        //   v1 → v2_a, v1 → v2_b
        //   v2_a → v3, v2_b → v3

        let temp = TempDir::new().unwrap();
        let schema_dir = temp.path().join("schema");
        fs::create_dir_all(&schema_dir).unwrap();

        // v1: base schema
        let v1_sql = "CREATE TABLE todos (title TEXT NOT NULL);";
        fs::write(schema_dir.join("current.sql"), v1_sql).unwrap();
        run(schema_dir.to_str().unwrap(), false).unwrap();

        let dir = SchemaDirectory::new(&schema_dir);
        let v1_hash = dir.schema_versions().unwrap()[0].hash.clone();

        // Simulate branch A: adds 'completed' column
        let v2a_sql = "CREATE TABLE todos (title TEXT NOT NULL, completed BOOLEAN NOT NULL);";
        fs::write(schema_dir.join("current.sql"), v2a_sql).unwrap();
        run(schema_dir.to_str().unwrap(), false).unwrap();

        let versions = dir.schema_versions().unwrap();
        let v2a_hash = versions
            .iter()
            .find(|v| v.version == 2)
            .unwrap()
            .hash
            .clone();

        // Simulate branch B merging in: manually add a second v2 with different content
        // (In real merge, this would come from the other branch's schema_v2_*.sql file)
        let v2b_sql = "CREATE TABLE todos (title TEXT NOT NULL, priority INTEGER);";
        let v2b_schema = parse_schema(v2b_sql).unwrap();
        let v2b_hash = jazz::query_manager::types::SchemaHash::compute(&v2b_schema);
        dir.write_schema(&v2b_schema, 2, Some("feature_b"), &v2b_hash.short())
            .unwrap();

        // Now create v3 (merged result): has both columns
        let v3_sql = "CREATE TABLE todos (title TEXT NOT NULL, completed BOOLEAN NOT NULL, priority INTEGER);";
        fs::write(schema_dir.join("current.sql"), v3_sql).unwrap();
        run(schema_dir.to_str().unwrap(), false).unwrap();

        let versions = dir.schema_versions().unwrap();
        let v3_hash = versions
            .iter()
            .find(|v| v.version == 3)
            .unwrap()
            .hash
            .clone();

        // Verify we have 4 schema files (1 at v1, 2 at v2, 1 at v3)
        assert_eq!(versions.len(), 4);
        assert_eq!(versions.iter().filter(|v| v.version == 1).count(), 1);
        assert_eq!(versions.iter().filter(|v| v.version == 2).count(), 2);
        assert_eq!(versions.iter().filter(|v| v.version == 3).count(), 1);

        // Verify migrations exist:
        // v1 → v2_a
        assert!(dir.has_migration_sql(1, 2, &v1_hash, &v2a_hash, Direction::Forward));
        assert!(dir.has_migration_sql(1, 2, &v1_hash, &v2a_hash, Direction::Backward));

        // v1 → v2_b
        assert!(dir.has_migration_sql(1, 2, &v1_hash, &v2b_hash.short(), Direction::Forward));
        assert!(dir.has_migration_sql(1, 2, &v1_hash, &v2b_hash.short(), Direction::Backward));

        // v2_a → v3
        assert!(dir.has_migration_sql(2, 3, &v2a_hash, &v3_hash, Direction::Forward));
        assert!(dir.has_migration_sql(2, 3, &v2a_hash, &v3_hash, Direction::Backward));

        // v2_b → v3
        assert!(dir.has_migration_sql(2, 3, &v2b_hash.short(), &v3_hash, Direction::Forward));
        assert!(dir.has_migration_sql(2, 3, &v2b_hash.short(), &v3_hash, Direction::Backward));
    }
}
