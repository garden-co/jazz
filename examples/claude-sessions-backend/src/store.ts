import Database from "better-sqlite3";
import * as fs from "node:fs";
import * as path from "node:path";
import type { ClaudeSessionSummary } from "./parser.js";

export class ClaudeSessionStore {
  private db: Database.Database;

  constructor(dbPath: string) {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    this.db = new Database(dbPath);
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("synchronous = NORMAL");
    this.init();
  }

  private init(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS sessions (
        session_id TEXT PRIMARY KEY,
        transcript_path TEXT NOT NULL,
        cwd TEXT,
        project_root TEXT,
        git_branch TEXT,
        entrypoint TEXT,
        version TEXT,
        updated_at_unix_ms INTEGER NOT NULL,
        first_user_message TEXT,
        latest_user_message TEXT,
        latest_assistant_message TEXT,
        user_turn_count INTEGER NOT NULL DEFAULT 0,
        assistant_turn_count INTEGER NOT NULL DEFAULT 0,
        total_entries INTEGER NOT NULL DEFAULT 0
      );
      CREATE INDEX IF NOT EXISTS idx_sessions_project_root
        ON sessions(project_root, updated_at_unix_ms DESC);
      CREATE INDEX IF NOT EXISTS idx_sessions_updated
        ON sessions(updated_at_unix_ms DESC);
    `);
  }

  upsert(summary: ClaudeSessionSummary): void {
    this.db
      .prepare(
        `INSERT INTO sessions
          (session_id, transcript_path, cwd, project_root, git_branch, entrypoint, version,
           updated_at_unix_ms, first_user_message, latest_user_message, latest_assistant_message,
           user_turn_count, assistant_turn_count, total_entries)
         VALUES
          (@sessionId, @transcriptPath, @cwd, @projectRoot, @gitBranch, @entrypoint, @version,
           @updatedAtUnixMs, @firstUserMessage, @latestUserMessage, @latestAssistantMessage,
           @userTurnCount, @assistantTurnCount, @totalEntries)
         ON CONFLICT(session_id) DO UPDATE SET
           transcript_path = excluded.transcript_path,
           cwd = excluded.cwd,
           project_root = excluded.project_root,
           git_branch = excluded.git_branch,
           entrypoint = excluded.entrypoint,
           version = excluded.version,
           updated_at_unix_ms = excluded.updated_at_unix_ms,
           first_user_message = excluded.first_user_message,
           latest_user_message = excluded.latest_user_message,
           latest_assistant_message = excluded.latest_assistant_message,
           user_turn_count = excluded.user_turn_count,
           assistant_turn_count = excluded.assistant_turn_count,
           total_entries = excluded.total_entries`,
      )
      .run(summary);
  }

  getSession(sessionId: string): ClaudeSessionSummary | null {
    const row = this.db
      .prepare(`SELECT * FROM sessions WHERE session_id = ?`)
      .get(sessionId) as Record<string, unknown> | undefined;
    return row ? rowToSummary(row) : null;
  }

  listForProjectRoot(projectRoot: string, limit: number): ClaudeSessionSummary[] {
    const rows = this.db
      .prepare(
        `SELECT * FROM sessions
         WHERE project_root = ?
         ORDER BY updated_at_unix_ms DESC
         LIMIT ?`,
      )
      .all(projectRoot, limit) as Record<string, unknown>[];
    return rows.map(rowToSummary);
  }

  search(query: string, limit: number): ClaudeSessionSummary[] {
    const pattern = `%${query}%`;
    const rows = this.db
      .prepare(
        `SELECT * FROM sessions
         WHERE session_id LIKE ?
            OR latest_user_message LIKE ?
            OR latest_assistant_message LIKE ?
            OR first_user_message LIKE ?
         ORDER BY updated_at_unix_ms DESC
         LIMIT ?`,
      )
      .all(pattern, pattern, pattern, pattern, limit) as Record<string, unknown>[];
    return rows.map(rowToSummary);
  }

  listRecent(limit: number): ClaudeSessionSummary[] {
    const rows = this.db
      .prepare(`SELECT * FROM sessions ORDER BY updated_at_unix_ms DESC LIMIT ?`)
      .all(limit) as Record<string, unknown>[];
    return rows.map(rowToSummary);
  }

  count(): number {
    const row = this.db.prepare(`SELECT COUNT(*) AS n FROM sessions`).get() as { n: number };
    return row.n;
  }

  close(): void {
    this.db.close();
  }
}

function rowToSummary(row: Record<string, unknown>): ClaudeSessionSummary {
  return {
    sessionId: row.session_id as string,
    transcriptPath: row.transcript_path as string,
    cwd: (row.cwd as string | null) ?? null,
    projectRoot: (row.project_root as string | null) ?? null,
    gitBranch: (row.git_branch as string | null) ?? null,
    entrypoint: (row.entrypoint as string | null) ?? null,
    version: (row.version as string | null) ?? null,
    updatedAtUnixMs: row.updated_at_unix_ms as number,
    firstUserMessage: (row.first_user_message as string | null) ?? null,
    latestUserMessage: (row.latest_user_message as string | null) ?? null,
    latestAssistantMessage: (row.latest_assistant_message as string | null) ?? null,
    userTurnCount: row.user_turn_count as number,
    assistantTurnCount: row.assistant_turn_count as number,
    totalEntries: row.total_entries as number,
  };
}
