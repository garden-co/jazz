import type { DataCommandMode } from "./command.js";

const EXIT_CODES = `Exit codes:
  0  success
  1  unexpected internal failure
  2  usage or invalid input (bad flags, invalid SQL, unknown table/column)
  3  missing or ambiguous credentials
  4  denied by the server
  5  timeout (a write may already have committed)
  6  the row already exists (a previous attempt committed)`;

const SCHEMA_SOURCES = `Schema source (choose at most one):
  (default)                     the schema named by the deployed permissions head
  --schema-dir <path>           a local schema.ts (executes local TypeScript; no server needed)
  --schema-hash <hash|current>  a stored schema version; current is the permissions head`;

const CREDENTIALS = `Data credentials (exactly one for row access; flags override environment):
  --backend-secret <s> / JAZZ_BACKEND_SECRET   unrestricted access
  --jwt <token> / JAZZ_JWT_TOKEN               an already-registered user's permissions
  An admin secret grants catalogue access only and never row access.
  Blank values count as unset. Prefer environment variables for secrets: argv
  is visible to other processes.`;

const STATEMENT_IDENTITY = `\`sql\` never guesses an application. SHOW TABLES and DESCRIBE use the same
catalogue as \`schema\` and need no data credential, but an application must
still be identified with --app-id/JAZZ_APP_ID and a server URL. To inspect a
schema with neither, use \`jazz-tools schema tables|describe --schema-dir\`.`;

const SQL_OPTIONS = `Options:
  --app-id <id> / JAZZ_APP_ID
  --server-url <url> / JAZZ_SERVER_URL
  --admin-secret <s> / JAZZ_ADMIN_SECRET
  --sql <statement> | --file <path|->   alternative to positional SQL
  --write                allow INSERT/UPDATE/DELETE
  --explain              print the compiled plan and exit; validates against the
                         schema but never connects or executes
  --id-seed <seed>       deterministic id for INSERT; a retry then fails with
                         ALREADY_EXISTS instead of writing a duplicate row
  --capabilities         print the supported dialect, operators, and limits
  --format <table|json|jsonl>   default: json when stdout is not a TTY, else table
  --max-cell-width <n>   truncate table cells longer than n (default 80; 0 disables)
  --timeout <ms>         whole-command deadline (default 30000)
  --verbose              print the resolved context as one JSON line on stderr
  --env-file <path>      load env vars (repeatable; existing env wins)`;

const DIALECT = `Dialect: one statement per invocation. SELECT with projection, AND
comparisons, IS [NOT] NULL, ORDER BY, LIMIT, OFFSET; one VALUES row per INSERT;
exact WHERE id = 'uuid' for UPDATE/DELETE. No joins, OR, parentheses, aliases,
aggregates, functions, parameters, multi-row writes, or transactions.
Run \`jazz-tools sql --capabilities\` for the machine-readable list.`;

export const SQL_HELP = `Usage:
  jazz-tools sql '<statement>' [options]
  jazz-tools sql --capabilities [--format json]

Read application rows through the Jazz SDK. SQL is never executed by the
database engine. Reads are the default; mutations require --write.

Examples:
  jazz-tools sql 'SHOW TABLES'
  jazz-tools sql 'DESCRIBE todos' --schema-dir ./my-app
  jazz-tools sql 'SELECT id, title FROM todos WHERE done = FALSE ORDER BY title LIMIT 20'
  printf '%s' 'SELECT title FROM todos' | jazz-tools sql --file - --format jsonl
  jazz-tools sql "INSERT INTO todos (title) VALUES ('Draft')" --write --format json
  jazz-tools sql "DELETE FROM todos WHERE id = '00000000-0000-0000-0000-000000000001'" --write --explain

${SCHEMA_SOURCES}

${CREDENTIALS}

${STATEMENT_IDENTITY}

${SQL_OPTIONS}

${DIALECT}

${EXIT_CODES}
`;

export const CAPABILITIES_HELP = `Usage:
  jazz-tools sql --capabilities [--format json]

Print the supported dialect, the where operators accepted per column type, and
the current limits. Needs no statement, schema, or credentials, so it is the
cheapest way to discover what this build accepts.

${EXIT_CODES}
`;

export const TABLES_HELP = `Usage:
  jazz-tools schema tables [options]

List tables from the local schema (default: ./schema.ts in the current
directory) or from a stored schema version. Needs no server or credentials in
the default local form.

Examples:
  jazz-tools schema tables
  jazz-tools schema tables --schema-dir ./my-app --format json
  jazz-tools schema tables --schema-hash current --app-id my-app --admin-secret "$JAZZ_ADMIN_SECRET"
  jazz-tools sql 'SHOW TABLES'    # the same list read from the deployed schema

Options:
  --schema-dir <path>           local app root containing schema.ts (default: .)
  --schema-hash <hash|current>  stored schema version; requires app, server, and admin secret
  --app-id <id> / JAZZ_APP_ID
  --server-url <url> / JAZZ_SERVER_URL
  --admin-secret <s> / JAZZ_ADMIN_SECRET
  --format <table|json|jsonl>   default: json when stdout is not a TTY, else table
  --max-cell-width <n>          truncate table cells longer than n (default 80; 0 disables)
  --env-file <path>             load env vars (repeatable)

${EXIT_CODES}
`;

export const DESCRIBE_HELP = `Usage:
  jazz-tools schema describe <table> [options]

Show a table's columns, types, nullability, defaults, references, indexes, and
branch keys. Reads the local schema by default; --schema-hash reads a stored
version. JSON and JSONL also carry hasDefault, sparse, mergeStrategy, and the
full structured typeDefinition.

Examples:
  jazz-tools schema describe todos
  jazz-tools schema describe todos --schema-dir ./my-app --format json
  jazz-tools schema describe todos --schema-hash current --app-id my-app --admin-secret "$JAZZ_ADMIN_SECRET"

Options:
  --schema-dir <path>           local app root containing schema.ts (default: .)
  --schema-hash <hash|current>  stored schema version; requires app, server, and admin secret
  --app-id <id> / JAZZ_APP_ID
  --server-url <url> / JAZZ_SERVER_URL
  --admin-secret <s> / JAZZ_ADMIN_SECRET
  --format <table|json|jsonl>   default: json when stdout is not a TTY, else table
  --max-cell-width <n>          truncate table cells longer than n (default 80; 0 disables)
  --env-file <path>             load env vars (repeatable)

${EXIT_CODES}
`;

export function dataHelp(mode: DataCommandMode, capabilities = false): string {
  if (capabilities) return CAPABILITIES_HELP;
  if (mode === "tables") return TABLES_HELP;
  if (mode === "describe") return DESCRIBE_HELP;
  return SQL_HELP;
}
