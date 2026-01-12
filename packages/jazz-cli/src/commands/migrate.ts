/**
 * Migration commands for Jazz CLI
 *
 * These commands interact with the Jazz server's schema registry
 * to manage schema migrations.
 */

import { readFileSync } from "node:fs";
import {
  formatError,
  formatInfo,
  formatSuccess,
  formatWarning,
  getApiKey,
} from "../utils.js";

interface SchemaColumn {
  name: string;
  columnType: string;
  nullable: boolean;
}

interface SchemaDiff {
  addedColumns: string[];
  removedColumns: string[];
  potentialRenames: Array<{
    oldName: string;
    newName: string;
    confidence: string;
  }>;
  typeChanges: Array<{
    column: string;
    oldType: string;
    newType: string;
  }>;
}

interface LensWarning {
  kind: string;
  message: string;
  column?: string;
}

interface MigrationResult {
  newDescriptorId: string;
  rowsMigrated: number;
  warnings: LensWarning[];
}

/**
 * Show current schema version for a table
 */
export async function migrateStatus(
  table: string,
  options: { server: string },
): Promise<void> {
  console.log(formatInfo(`Fetching schema status for table: ${table}`));

  try {
    const response = await fetch(`${options.server}/api/schema/${table}`, {
      headers: {
        Authorization: `Bearer ${getApiKey()}`,
      },
    });

    if (!response.ok) {
      const error = await response.text();
      console.error(formatError(`Failed to fetch schema: ${error}`));
      process.exit(1);
    }

    const data = await response.json();

    console.log("\nCurrent Schema:");
    console.log(`  Table: ${table}`);
    console.log(`  Descriptor ID: ${data.descriptorId}`);
    console.log("  Columns:");

    for (const col of data.columns as SchemaColumn[]) {
      const nullable = col.nullable ? " (nullable)" : "";
      console.log(`    - ${col.name}: ${col.columnType}${nullable}`);
    }

    if (data.parentDescriptors && data.parentDescriptors.length > 0) {
      console.log(`  Parent Descriptors: ${data.parentDescriptors.join(", ")}`);
    }
  } catch (error) {
    console.error(formatError(`Error: ${error}`));
    process.exit(1);
  }
}

/**
 * Preview schema changes
 */
export async function migrateDiff(
  table: string,
  options: { server: string; file?: string },
): Promise<void> {
  console.log(formatInfo(`Computing diff for table: ${table}`));

  if (!options.file) {
    console.error(formatError("Please specify a schema file with --file"));
    process.exit(1);
  }

  try {
    const newSchemaContent = readFileSync(options.file, "utf-8");
    const newSchema = JSON.parse(newSchemaContent);

    const response = await fetch(`${options.server}/api/schema/${table}/diff`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${getApiKey()}`,
      },
      body: JSON.stringify({ newSchema }),
    });

    if (!response.ok) {
      const error = await response.text();
      console.error(formatError(`Failed to compute diff: ${error}`));
      process.exit(1);
    }

    const diff: SchemaDiff = await response.json();

    console.log("\nSchema Diff:");

    if (diff.addedColumns.length > 0) {
      console.log(formatSuccess("\n  Added Columns:"));
      for (const col of diff.addedColumns) {
        console.log(`    + ${col}`);
      }
    }

    if (diff.removedColumns.length > 0) {
      console.log(formatWarning("\n  Removed Columns:"));
      for (const col of diff.removedColumns) {
        console.log(`    - ${col}`);
      }
    }

    if (diff.potentialRenames.length > 0) {
      console.log(formatInfo("\n  Potential Renames:"));
      for (const rename of diff.potentialRenames) {
        console.log(
          `    ${rename.oldName} -> ${rename.newName} (${rename.confidence})`,
        );
      }
    }

    if (diff.typeChanges.length > 0) {
      console.log(formatWarning("\n  Type Changes:"));
      for (const change of diff.typeChanges) {
        console.log(
          `    ${change.column}: ${change.oldType} -> ${change.newType}`,
        );
      }
    }

    if (
      diff.addedColumns.length === 0 &&
      diff.removedColumns.length === 0 &&
      diff.potentialRenames.length === 0 &&
      diff.typeChanges.length === 0
    ) {
      console.log(formatInfo("  No changes detected"));
    }
  } catch (error) {
    console.error(formatError(`Error: ${error}`));
    process.exit(1);
  }
}

/**
 * Deploy schema migration
 */
export async function migratePush(
  table: string,
  options: { server: string; file?: string; env: string; yes: boolean },
): Promise<void> {
  console.log(formatInfo(`Deploying schema migration for table: ${table}`));
  console.log(`  Environment: ${options.env}`);

  if (!options.file) {
    console.error(formatError("Please specify a schema file with --file"));
    process.exit(1);
  }

  const apiKey = getApiKey();
  if (!apiKey) {
    console.error(formatError("JAZZ_API_KEY environment variable not set"));
    process.exit(1);
  }

  try {
    const newSchemaContent = readFileSync(options.file, "utf-8");
    const newSchema = JSON.parse(newSchemaContent);

    // First, preview the migration
    console.log(formatInfo("\nPreviewing migration..."));

    const previewResponse = await fetch(
      `${options.server}/api/schema/${table}/preview`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify({
          newSchema,
          environment: options.env,
        }),
      },
    );

    if (!previewResponse.ok) {
      const error = await previewResponse.text();
      console.error(formatError(`Failed to preview migration: ${error}`));
      process.exit(1);
    }

    const preview = await previewResponse.json();

    // Show warnings
    if (preview.warnings && preview.warnings.length > 0) {
      console.log(formatWarning("\nWarnings:"));
      for (const warning of preview.warnings as LensWarning[]) {
        const column = warning.column ? ` (${warning.column})` : "";
        console.log(`  - ${warning.message}${column}`);
      }
    }

    // Show lens transforms
    if (preview.lens) {
      console.log(formatInfo("\nGenerated Lens:"));
      console.log("  Forward transforms:");
      for (const transform of preview.lens.forward) {
        console.log(
          `    - ${transform.transformType}: ${JSON.stringify(transform)}`,
        );
      }
    }

    // Confirm unless --yes flag
    if (!options.yes) {
      console.log(
        formatWarning("\nThis will deploy the migration to the server."),
      );
      console.log("Use --yes flag to skip this confirmation.\n");

      // In a real CLI we would use readline for confirmation
      // For now, we'll just show a message
      console.log(
        "(Confirmation prompt would appear here - use --yes to proceed)",
      );
      process.exit(0);
    }

    // Execute migration
    console.log(formatInfo("\nExecuting migration..."));

    const deployResponse = await fetch(
      `${options.server}/api/schema/${table}/deploy`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify({
          newSchema,
          environment: options.env,
        }),
      },
    );

    if (!deployResponse.ok) {
      const error = await deployResponse.text();
      console.error(formatError(`Failed to deploy migration: ${error}`));
      process.exit(1);
    }

    const result: MigrationResult = await deployResponse.json();

    console.log(formatSuccess("\nMigration deployed successfully!"));
    console.log(`  New Descriptor ID: ${result.newDescriptorId}`);
    console.log(`  Rows Migrated: ${result.rowsMigrated}`);

    if (result.warnings && result.warnings.length > 0) {
      console.log(formatWarning("\n  Warnings:"));
      for (const warning of result.warnings) {
        console.log(`    - ${warning.message}`);
      }
    }
  } catch (error) {
    console.error(formatError(`Error: ${error}`));
    process.exit(1);
  }
}
