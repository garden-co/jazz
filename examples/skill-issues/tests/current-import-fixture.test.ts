import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { importMarkdownTodo } from "../src/domain/markdown.js";

describe("current tracked markdown import fixture", () => {
  it("imports representative pre-cutover todo ideas and issues", async () => {
    const root = await mkdtemp(join(tmpdir(), "skill-issues-current-import-"));
    await mkdir(join(root, "todo/ideas/1_mvp"), { recursive: true });
    await mkdir(join(root, "todo/issues"), { recursive: true });
    await writeFile(
      join(root, "todo/ideas/1_mvp/explicit-indices.md"),
      [
        "# Explicit Indices",
        "",
        "## What",
        "",
        "Developer-declared indices in the schema language, replacing auto-index-all-columns.",
        "",
        "## Notes",
        "",
      ].join("\n"),
    );
    await writeFile(
      join(root, "todo/issues/reconnect-outbox-dedup.md"),
      [
        "# Reconnect outbox dedup is not implemented",
        "",
        "## What",
        "",
        "After a reconnect, the client replays in-flight outbox entries. If the server received the payload but the ack did not reach the client, the server re-applies it on reconnect.",
        "",
        "## Priority",
        "",
        "unknown",
        "",
        "## Notes",
        "",
      ].join("\n"),
    );
    await writeFile(
      join(root, "todo/issues/test_multi-server-sync.md"),
      [
        "# Multi-server sync integration tests",
        "",
        "## What",
        "",
        "Missing integration tests simulating client -> edge -> server communication topology.",
        "",
        "## Priority",
        "",
        "unknown",
        "",
        "## Notes",
        "",
      ].join("\n"),
    );

    const items = await importMarkdownTodo(join(root, "todo"));

    expect(items).toHaveLength(3);
    expect(items.filter((item) => item.kind === "idea")).toHaveLength(1);
    expect(items.filter((item) => item.kind === "issue")).toHaveLength(2);
    const bySlug = new Map(items.map((item) => [item.slug, item]));
    expect(bySlug.get("explicit-indices")).toMatchObject({
      kind: "idea",
      title: "Explicit Indices",
    });
    expect(bySlug.get("explicit-indices")?.description).toContain("Developer-declared indices");
    expect(bySlug.get("reconnect-outbox-dedup")).toMatchObject({
      kind: "issue",
    });
    expect(bySlug.get("reconnect-outbox-dedup")?.description).toContain("server re-applies");
    expect(bySlug.get("test_multi-server-sync")).toMatchObject({
      kind: "issue",
    });
    expect(bySlug.get("test_multi-server-sync")?.description).toContain("client -> edge -> server");
  });
});
