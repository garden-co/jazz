/**
 * Jazz CLI - Schema migration deployment tool
 *
 * This package provides a CLI for managing Jazz schema migrations.
 *
 * Usage:
 *   jazz migrate status <table>  - Show current schema version
 *   jazz migrate diff <table>    - Preview schema changes
 *   jazz migrate push <table>    - Deploy schema migration
 *
 * Environment Variables:
 *   JAZZ_API_KEY - API key for authenticating with the server
 *
 * Example:
 *   export JAZZ_API_KEY=your-api-key
 *   jazz migrate push users --file schema/users.json --env dev
 */

export { migrateDiff, migratePush, migrateStatus } from "./commands/migrate.js";
export {
  formatError,
  formatInfo,
  formatSuccess,
  formatWarning,
  getApiKey,
} from "./utils.js";
