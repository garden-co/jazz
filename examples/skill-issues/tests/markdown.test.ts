import { mkdtemp, mkdir, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { exportMarkdownTodo, importMarkdownTodo } from "../src/domain/markdown.js";

async function tempRoot() {
  return mkdtemp(join(tmpdir(), "skill-issues-markdown-"));
}

describe("markdown compatibility", () => {
  it("imports existing ideas and issues using filename slugs and What sections", async () => {
    const root = await tempRoot();
    await mkdir(join(root, "todo/ideas/1_mvp"), { recursive: true });
    await mkdir(join(root, "todo/issues"), { recursive: true });
    await writeFile(
      join(root, "todo/ideas/1_mvp/explicit-indices.md"),
      "# Explicit Indices\n\n## What\n\nDeveloper-declared indices in the schema language.\n\n## Notes\n\nIgnored note.\n",
    );
    await writeFile(
      join(root, "todo/issues/reconnect-outbox-dedup.md"),
      "# Reconnect outbox dedup\n\n## What\n\nReconnect should not replay duplicate frames.\n\n## Priority\n\nhigh\n\n## Notes\n\nIgnored issue note.\n",
    );

    const items = await importMarkdownTodo(join(root, "todo"));

    expect(items).toEqual([
      {
        kind: "idea",
        slug: "explicit-indices",
        title: "Explicit Indices",
        description: "Developer-declared indices in the schema language.",
      },
      {
        kind: "issue",
        slug: "reconnect-outbox-dedup",
        title: "Reconnect outbox dedup",
        description: "Reconnect should not replay duplicate frames.",
      },
    ]);
  });

  it("exports ideas and issues to the current Markdown shape", async () => {
    const root = await tempRoot();

    await exportMarkdownTodo(join(root, "todo"), [
      {
        kind: "idea",
        slug: "explicit-indices",
        title: "Explicit Indices",
        description: "Developer-declared indices in the schema language.",
      },
      {
        kind: "issue",
        slug: "reconnect-outbox-dedup",
        title: "Reconnect outbox dedup",
        description: "Reconnect should not replay duplicate frames.",
      },
    ]);

    await expect(
      readFile(join(root, "todo/ideas/1_mvp/explicit-indices.md"), "utf8"),
    ).resolves.toBe(
      "# Explicit Indices\n\n## What\n\nDeveloper-declared indices in the schema language.\n\n## Notes\n\n",
    );
    await expect(
      readFile(join(root, "todo/issues/reconnect-outbox-dedup.md"), "utf8"),
    ).resolves.toBe(
      "# Reconnect outbox dedup\n\n## What\n\nReconnect should not replay duplicate frames.\n\n## Priority\n\nunknown\n\n## Notes\n\n",
    );
  });

  it("rejects duplicate slugs across ideas and issues", async () => {
    const root = await tempRoot();
    await mkdir(join(root, "todo/ideas/1_mvp"), { recursive: true });
    await mkdir(join(root, "todo/issues"), { recursive: true });
    await writeFile(join(root, "todo/ideas/1_mvp/same-slug.md"), "# Idea\n\n## What\n\nOne\n");
    await writeFile(join(root, "todo/issues/same-slug.md"), "# Issue\n\n## What\n\nTwo\n");

    await expect(importMarkdownTodo(join(root, "todo"))).rejects.toThrow(
      "Duplicate item slug: same-slug",
    );
  });
});
