import type { ParsedArgs } from "./utils/args.ts";
import {
  getFlagNumber,
  getFlagString,
  getStorageEngine,
} from "./utils/args.ts";
import { readAllCoValues } from "./utils/sqliteCoValues.ts";

function assertNonEmptyString(
  value: string | undefined,
  label: string,
): string {
  if (!value || value.trim() === "") {
    throw new Error(`Missing required ${label}`);
  }
  return value;
}

export async function queryCoValues(args: ParsedArgs): Promise<void> {
  const dbPath = assertNonEmptyString(
    getFlagString(args, "db") ?? "./seed.db",
    "--db",
  );
  const storageEngine = getStorageEngine(args);
  const limit = getFlagNumber(args, "limit");

  if (storageEngine === "fjall") {
    console.error(
      "Warning: The query command currently only supports direct SQLite inspection.",
    );
    console.error(
      "For fjall databases, use the seed config ID to load CoValues through the sync server.",
    );
    process.exit(1);
  }

  const rows = readAllCoValues(dbPath);
  const out =
    typeof limit === "number" && limit >= 0 ? rows.slice(0, limit) : rows;

  console.log(
    JSON.stringify(
      {
        db: dbPath,
        total: rows.length,
        returned: out.length,
        rows: out.map((r) => ({ id: r.id, header: r.rawHeader })),
      },
      null,
      2,
    ),
  );
}
