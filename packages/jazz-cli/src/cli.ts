#!/usr/bin/env node

/**
 * Jazz CLI - Schema migration deployment tool
 *
 * Commands:
 *   jazz migrate status   - Show current schema version
 *   jazz migrate diff     - Preview schema changes
 *   jazz migrate push     - Deploy schema migration
 */

import { Command } from "commander";
import { migrateDiff, migratePush, migrateStatus } from "./commands/migrate.js";

const program = new Command();

program
  .name("jazz")
  .description("CLI for Jazz schema migrations")
  .version("0.1.0");

// Migrate command group
const migrate = program
  .command("migrate")
  .description("Schema migration commands");

migrate
  .command("status")
  .description("Show current schema version for a table")
  .argument("<table>", "Table name")
  .option("-s, --server <url>", "Server URL", "http://localhost:3000")
  .action(async (table: string, options: { server: string }) => {
    await migrateStatus(table, options);
  });

migrate
  .command("diff")
  .description("Preview schema changes between current and new schema")
  .argument("<table>", "Table name")
  .option("-s, --server <url>", "Server URL", "http://localhost:3000")
  .option("-f, --file <path>", "Path to new schema file")
  .action(async (table: string, options: { server: string; file?: string }) => {
    await migrateDiff(table, options);
  });

migrate
  .command("push")
  .description("Deploy schema migration to server")
  .argument("<table>", "Table name")
  .option("-s, --server <url>", "Server URL", "http://localhost:3000")
  .option("-f, --file <path>", "Path to new schema file")
  .option("-e, --env <environment>", "Target environment", "dev")
  .option("-y, --yes", "Skip confirmation prompt", false)
  .action(
    async (
      table: string,
      options: { server: string; file?: string; env: string; yes: boolean },
    ) => {
      await migratePush(table, options);
    },
  );

program.parse();
