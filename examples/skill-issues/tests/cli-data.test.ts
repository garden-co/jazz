import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { runCli } from "../src/cli.js";
import type { IssueItem, ItemStatus, ListedItem, ListFilters } from "../src/repository.js";

type MemoryRepo = {
  upsertItem(item: IssueItem): Promise<ListedItem>;
  listItems(filters?: ListFilters): Promise<ListedItem[]>;
  getItem(slug: string): Promise<ListedItem | null>;
  assignMe(slug: string): Promise<ListedItem>;
  setStatus(slug: string, status: ItemStatus): Promise<ListedItem>;
};

function asListed(item: IssueItem, status: ItemStatus = "open"): ListedItem {
  return {
    ...item,
    state: {
      itemSlug: item.slug,
      status,
    },
  };
}

function createMemoryRepo(): MemoryRepo {
  const items = new Map<string, ListedItem>();

  async function getExisting(slug: string): Promise<ListedItem> {
    const item = items.get(slug);
    if (!item) {
      throw new Error(`Item not found: ${slug}`);
    }
    return item;
  }

  return {
    async upsertItem(item) {
      const saved = asListed(item, items.get(item.slug)?.state.status ?? "open");
      items.set(item.slug, saved);
      return saved;
    },

    async listItems(filters = {}) {
      return [...items.values()]
        .filter((item) => !filters.kind || item.kind === filters.kind)
        .filter((item) => !filters.status || item.state.status === filters.status)
        .sort((left, right) => left.slug.localeCompare(right.slug));
    },

    async getItem(slug) {
      return items.get(slug) ?? null;
    },

    async assignMe(slug) {
      return getExisting(slug);
    },

    async setStatus(slug, status) {
      const item = await getExisting(slug);
      const saved = asListed(item, status);
      items.set(slug, saved);
      return saved;
    },
  };
}

async function tempRoot() {
  return mkdtemp(join(tmpdir(), "skill-issues-cli-data-"));
}

describe("skill issues CLI data commands", () => {
  it("rejects list filters with missing values before opening the repository", async () => {
    const root = await tempRoot();
    const openRepository = vi.fn(async () => {
      throw new Error("repository should not open");
    });

    const result = await runCli(["list", "--kind"], { cwd: root, env: {} }, { openRepository });

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("Usage: issues list");
    expect(openRepository).not.toHaveBeenCalled();
  });

  it("rejects add with a missing slug before opening the repository", async () => {
    const root = await tempRoot();
    const openRepository = vi.fn(async () => {
      throw new Error("repository should not open");
    });

    const result = await runCli(
      ["add", "issue", "--title", "Policy error reasons", "--description", "Explain rejections."],
      { cwd: root, env: {} },
      { openRepository },
    );

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("Usage: issues add");
    expect(openRepository).not.toHaveBeenCalled();
  });

  it("rejects assign with a missing slug before opening the repository", async () => {
    const root = await tempRoot();
    const openRepository = vi.fn(async () => {
      throw new Error("repository should not open");
    });

    const result = await runCli(["assign", "--me"], { cwd: root, env: {} }, { openRepository });

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("Usage: issues assign");
    expect(openRepository).not.toHaveBeenCalled();
  });

  it("adds and lists issues through an injected repository", async () => {
    const root = await tempRoot();
    const repo = createMemoryRepo();
    const deps = { openRepository: async () => repo };

    await expect(
      runCli(
        [
          "add",
          "issue",
          "policy-error-reasons",
          "--title",
          "Policy error reasons",
          "--description",
          "Show useful reasons when policies reject a write.",
        ],
        { cwd: root, env: {} },
        deps,
      ),
    ).resolves.toMatchObject({
      exitCode: 0,
      stdout: "Saved issue policy-error-reasons.\n",
    });

    const result = await runCli(["list"], { cwd: root, env: {} }, deps);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("issue policy-error-reasons open Policy error reasons");
  });

  it("imports and exports Markdown todo items through an injected repository", async () => {
    const root = await tempRoot();
    const repo = createMemoryRepo();
    await mkdir(join(root, "todo/issues"), { recursive: true });
    await writeFile(
      join(root, "todo/issues/reconnect-outbox-dedup.md"),
      "# Reconnect outbox dedup\n\n## What\n\nReconnect should not replay duplicate frames.\n\n## Priority\n\nhigh\n\n## Notes\n\n",
    );

    const deps = { openRepository: async () => repo };
    await expect(runCli(["import", "todo"], { cwd: root, env: {} }, deps)).resolves.toMatchObject({
      exitCode: 0,
      stdout: "Imported 1 items.\n",
    });

    await expect(
      runCli(["export", "exported"], { cwd: root, env: {} }, deps),
    ).resolves.toMatchObject({
      exitCode: 0,
      stdout: "Exported 1 items.\n",
    });

    await expect(
      readFile(join(root, "exported/issues/reconnect-outbox-dedup.md"), "utf8"),
    ).resolves.toContain("Reconnect should not replay duplicate frames.");
  });
});
