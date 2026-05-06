import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ClaudeTranscriptWatcher } from "../src/watcher.js";
import type { ClaudeSessionStore } from "../src/store.js";

describe("ClaudeTranscriptWatcher", () => {
  let tmpDir: string;

  beforeEach(() => {
    vi.useFakeTimers();
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "claude-sessions-watcher-"));
  });

  afterEach(() => {
    vi.useRealTimers();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("delegates transcript ingestion without parsing in the socket process", async () => {
    const transcriptPath = path.join(tmpDir, "session.jsonl");
    fs.writeFileSync(transcriptPath, "");
    const upsert = vi.fn();
    const ingestTranscript = vi.fn();
    const watcher = new ClaudeTranscriptWatcher(
      tmpDir,
      { upsert } as unknown as ClaudeSessionStore,
      { debounceMs: 5, ingestTranscript },
    );
    const queueIngest = (watcher as unknown as { queueIngest(transcriptPath: string): void }).queueIngest.bind(watcher);

    queueIngest(transcriptPath);
    await vi.runAllTimersAsync();

    expect(ingestTranscript).toHaveBeenCalledWith(transcriptPath);
    expect(upsert).not.toHaveBeenCalled();
  });
});
