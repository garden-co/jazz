import * as fs from "node:fs";
import * as path from "node:path";
import { parseClaudeTranscript } from "./parser.js";
import type { ClaudeSessionStore } from "./store.js";

/**
 * Scan the Claude Code projects directory and upsert every transcript we find.
 * Returns the number of sessions ingested.
 */
export function scanClaudeProjects(
  projectsRoot: string,
  store: ClaudeSessionStore,
): { ingested: number; skipped: number } {
  if (!fs.existsSync(projectsRoot)) return { ingested: 0, skipped: 0 };

  let ingested = 0;
  let skipped = 0;
  const projectDirs = fs.readdirSync(projectsRoot, { withFileTypes: true });
  for (const projectDir of projectDirs) {
    if (!projectDir.isDirectory()) continue;
    const projectPath = path.join(projectsRoot, projectDir.name);
    let transcripts: fs.Dirent[];
    try {
      transcripts = fs.readdirSync(projectPath, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const t of transcripts) {
      if (!t.isFile() || !t.name.endsWith(".jsonl")) continue;
      const full = path.join(projectPath, t.name);
      const summary = parseClaudeTranscript(full);
      if (summary) {
        store.upsert(summary);
        ingested += 1;
      } else {
        skipped += 1;
      }
    }
  }
  return { ingested, skipped };
}

/**
 * Watch the Claude Code projects directory. On any JSONL change or rename,
 * re-parse that transcript and upsert it. Debounces per-file updates so we
 * don't thrash the sqlite write path during mid-turn streaming.
 */
export class ClaudeTranscriptWatcher {
  private watchers: fs.FSWatcher[] = [];
  private pending = new Map<string, NodeJS.Timeout>();
  private debounceMs: number;

  constructor(
    private projectsRoot: string,
    private store: ClaudeSessionStore,
    options?: { debounceMs?: number },
  ) {
    this.debounceMs = options?.debounceMs ?? 250;
  }

  start(): void {
    if (!fs.existsSync(this.projectsRoot)) {
      fs.mkdirSync(this.projectsRoot, { recursive: true });
    }

    // Watch the top-level projects dir for new project folders.
    const topWatcher = fs.watch(this.projectsRoot, (eventType, filename) => {
      if (!filename) return;
      const projectPath = path.join(this.projectsRoot, filename);
      try {
        if (fs.statSync(projectPath).isDirectory()) {
          this.watchProjectDir(projectPath);
        }
      } catch {
        /* removed or transient */
      }
    });
    this.watchers.push(topWatcher);

    // Watch every existing project dir.
    for (const entry of fs.readdirSync(this.projectsRoot, { withFileTypes: true })) {
      if (entry.isDirectory()) {
        this.watchProjectDir(path.join(this.projectsRoot, entry.name));
      }
    }
  }

  private watchProjectDir(projectPath: string): void {
    try {
      const watcher = fs.watch(projectPath, (_eventType, filename) => {
        if (!filename || !filename.endsWith(".jsonl")) return;
        const full = path.join(projectPath, filename);
        this.queueIngest(full);
      });
      this.watchers.push(watcher);
    } catch {
      /* dir removed */
    }
  }

  private queueIngest(transcriptPath: string): void {
    const existing = this.pending.get(transcriptPath);
    if (existing) clearTimeout(existing);
    const timer = setTimeout(() => {
      this.pending.delete(transcriptPath);
      try {
        if (!fs.existsSync(transcriptPath)) return;
        const summary = parseClaudeTranscript(transcriptPath);
        if (summary) this.store.upsert(summary);
      } catch (error) {
        console.error(`[watcher] ingest failed for ${transcriptPath}:`, error);
      }
    }, this.debounceMs);
    this.pending.set(transcriptPath, timer);
  }

  stop(): void {
    for (const timer of this.pending.values()) clearTimeout(timer);
    this.pending.clear();
    for (const w of this.watchers) {
      try {
        w.close();
      } catch {
        /* already closed */
      }
    }
    this.watchers = [];
  }
}
