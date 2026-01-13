import Database from "better-sqlite3";

import type { CoValueHeader } from "cojson";

export type CoValueHeaderRow = {
  id: string;
  header: CoValueHeader;
  rawHeader: string;
};

export function readAllCoValues(dbPath: string): CoValueHeaderRow[] {
  const db = new Database(dbPath, { readonly: true });
  try {
    const rows = db
      .prepare("SELECT id, header FROM coValues ORDER BY rowID ASC")
      .all() as { id: string; header: string }[];

    const out: CoValueHeaderRow[] = [];
    for (const row of rows) {
      try {
        const parsed = JSON.parse(row.header) as CoValueHeader;
        out.push({ id: row.id, header: parsed, rawHeader: row.header });
      } catch {
        // Skip invalid headers; queryCoValues still shows raw header if needed.
      }
    }
    return out;
  } finally {
    db.close();
  }
}
