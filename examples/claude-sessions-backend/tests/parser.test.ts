import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { parseClaudeTranscript } from "../src/parser.js";

describe("parseClaudeTranscript", () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "claude-sessions-parser-"));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("returns null for empty files", () => {
    const file = path.join(tmpDir, "empty.jsonl");
    fs.writeFileSync(file, "");
    expect(parseClaudeTranscript(file)).toBeNull();
  });

  it("parses user and assistant turns with metadata", () => {
    const file = path.join(tmpDir, "abc.jsonl");
    fs.writeFileSync(
      file,
      [
        JSON.stringify({
          type: "user",
          sessionId: "abc",
          cwd: "/repo",
          gitBranch: "main",
          entrypoint: "cli",
          version: "1.0",
          uuid: "u1",
          parentUuid: null,
          message: { content: [{ type: "text", text: "hello" }] },
          timestamp: "2026-04-20T03:00:00Z",
        }),
        JSON.stringify({
          type: "assistant",
          sessionId: "abc",
          uuid: "a1",
          parentUuid: "u1",
          message: { id: "msg_1", content: [{ type: "text", text: "hi back" }] },
          timestamp: "2026-04-20T03:00:05Z",
        }),
        JSON.stringify({
          type: "user",
          sessionId: "abc",
          uuid: "u2",
          parentUuid: "a1",
          message: { content: [{ type: "text", text: "follow up" }] },
          timestamp: "2026-04-20T03:01:00Z",
        }),
      ].join("\n") + "\n",
    );

    const summary = parseClaudeTranscript(file);
    expect(summary).not.toBeNull();
    expect(summary?.sessionId).toBe("abc");
    expect(summary?.cwd).toBe("/repo");
    expect(summary?.gitBranch).toBe("main");
    expect(summary?.firstUserMessage).toBe("hello");
    expect(summary?.latestUserMessage).toBe("follow up");
    expect(summary?.latestAssistantMessage).toBe("hi back");
    expect(summary?.userTurnCount).toBe(2);
    expect(summary?.assistantTurnCount).toBe(1);
    expect(summary?.updatedAtUnixMs).toBe(Date.parse("2026-04-20T03:01:00Z"));
  });

  it("skips tool-result-only user entries for previews", () => {
    const file = path.join(tmpDir, "tool.jsonl");
    fs.writeFileSync(
      file,
      [
        JSON.stringify({
          type: "user",
          sessionId: "tool",
          uuid: "u1",
          message: { content: [{ type: "text", text: "real prompt" }] },
        }),
        JSON.stringify({
          type: "user",
          sessionId: "tool",
          uuid: "u2",
          toolUseResult: { ok: true },
          message: { content: [{ type: "tool_result", content: [] }] },
        }),
      ].join("\n") + "\n",
    );

    const summary = parseClaudeTranscript(file);
    expect(summary?.userTurnCount).toBe(1);
    expect(summary?.latestUserMessage).toBe("real prompt");
  });

  it("derives sessionId from filename when missing from entries", () => {
    const uuid = "12345678-1234-1234-1234-1234567890ab";
    const file = path.join(tmpDir, `${uuid}.jsonl`);
    fs.writeFileSync(
      file,
      JSON.stringify({
        type: "system",
        subtype: "compact",
      }) + "\n",
    );
    const summary = parseClaudeTranscript(file);
    expect(summary?.sessionId).toBe(uuid);
  });
});
