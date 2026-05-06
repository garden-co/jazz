import * as fs from "node:fs";
import * as path from "node:path";

export interface ClaudeTurnSummary {
  uuid: string;
  parentUuid: string | null;
  role: "user" | "assistant";
  text: string;
  timestamp: string | null;
}

export interface ClaudeSessionSummary {
  sessionId: string;
  transcriptPath: string;
  cwd: string | null;
  projectRoot: string | null;
  gitBranch: string | null;
  entrypoint: string | null;
  version: string | null;
  updatedAtUnixMs: number;
  firstUserMessage: string | null;
  latestUserMessage: string | null;
  latestAssistantMessage: string | null;
  userTurnCount: number;
  assistantTurnCount: number;
  totalEntries: number;
}

const PREVIEW_MAX_CHARS = 400;

function extractText(message: unknown): string {
  if (!message || typeof message !== "object") return "";
  const m = message as Record<string, unknown>;
  const content = m.content;
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  const parts: string[] = [];
  for (const part of content) {
    if (!part || typeof part !== "object") continue;
    const p = part as Record<string, unknown>;
    const t = p.type;
    if ((t === "text" || t === "input_text" || t === "output_text") && typeof p.text === "string") {
      parts.push(p.text);
    } else if (t === "tool_use" && typeof p.name === "string") {
      parts.push(`[tool_use ${p.name}]`);
    } else if (t === "tool_result") {
      parts.push("[tool_result]");
    }
  }
  return parts.join("\n").trim();
}

function clipText(text: string, limit = PREVIEW_MAX_CHARS): string {
  const trimmed = text.trim();
  if (trimmed.length <= limit) return trimmed;
  return trimmed.slice(0, limit) + "…";
}

function parseTimestampToUnixMs(value: unknown): number | null {
  if (typeof value !== "string") return null;
  const t = Date.parse(value);
  return Number.isNaN(t) ? null : t;
}

/**
 * Parse a Claude Code transcript JSONL file into a session summary.
 * Returns null if the file is empty or has no user/assistant entries.
 */
export function parseClaudeTranscript(filePath: string): ClaudeSessionSummary | null {
  let raw: string;
  try {
    raw = fs.readFileSync(filePath, "utf-8");
  } catch {
    return null;
  }

  const lines = raw.split("\n");
  let sessionId: string | null = null;
  let cwd: string | null = null;
  let gitBranch: string | null = null;
  let entrypoint: string | null = null;
  let version: string | null = null;
  let firstUserText: string | null = null;
  let latestUserText: string | null = null;
  let latestAssistantText: string | null = null;
  let userCount = 0;
  let assistantCount = 0;
  let totalEntries = 0;
  let lastTimestampMs: number | null = null;

  for (const line of lines) {
    if (!line.trim()) continue;
    let entry: Record<string, unknown>;
    try {
      entry = JSON.parse(line) as Record<string, unknown>;
    } catch {
      continue;
    }
    totalEntries += 1;

    if (!sessionId && typeof entry.sessionId === "string") sessionId = entry.sessionId;
    if (!cwd && typeof entry.cwd === "string") cwd = entry.cwd;
    if (!gitBranch && typeof entry.gitBranch === "string") gitBranch = entry.gitBranch;
    if (!entrypoint && typeof entry.entrypoint === "string") entrypoint = entry.entrypoint;
    if (!version && typeof entry.version === "string") version = entry.version;

    const ts = parseTimestampToUnixMs(entry.timestamp);
    if (ts !== null && (lastTimestampMs === null || ts > lastTimestampMs)) {
      lastTimestampMs = ts;
    }

    const type = entry.type;
    if (type === "user") {
      userCount += 1;
      // Skip tool-result-only user entries for preview purposes, but still
      // count them as transcript turns so follow cursors match Claude JSONL.
      const toolUseResult = entry.toolUseResult;
      if (toolUseResult !== undefined) continue;
      const text = extractText(entry.message).trim();
      if (!text) continue;
      if (firstUserText === null) firstUserText = text;
      latestUserText = text;
    } else if (type === "assistant") {
      const text = extractText(entry.message).trim();
      if (!text) continue;
      assistantCount += 1;
      latestAssistantText = text;
    }
  }

  if (!sessionId) {
    // Fall back to filename: <uuid>.jsonl
    const base = path.basename(filePath, ".jsonl");
    if (/^[0-9a-f-]{36}$/i.test(base)) sessionId = base;
  }
  if (!sessionId) return null;

  let updatedAtUnixMs = lastTimestampMs;
  if (updatedAtUnixMs === null) {
    try {
      updatedAtUnixMs = fs.statSync(filePath).mtimeMs;
    } catch {
      updatedAtUnixMs = Date.now();
    }
  }

  // Derive projectRoot from cwd if present, else from the parent directory slug
  let projectRoot = cwd;
  if (!projectRoot) {
    const parent = path.basename(path.dirname(filePath));
    if (parent.startsWith("-Users-")) {
      projectRoot = "/" + parent.slice(1).split("-").join("/");
    }
  }

  return {
    sessionId,
    transcriptPath: filePath,
    cwd,
    projectRoot,
    gitBranch,
    entrypoint,
    version,
    updatedAtUnixMs,
    firstUserMessage: firstUserText ? clipText(firstUserText) : null,
    latestUserMessage: latestUserText ? clipText(latestUserText) : null,
    latestAssistantMessage: latestAssistantText ? clipText(latestAssistantText) : null,
    userTurnCount: userCount,
    assistantTurnCount: assistantCount,
    totalEntries,
  };
}
