import { describe, expect, it, vi } from "vitest";
import { createSkillIssuesServer } from "../src/server/server.js";
import { requestJson } from "./support/http.js";
import type { IssueItem, ItemStatus, ListedItem } from "../src/repository.js";
import type { Express } from "express";

function listedItem(overrides: Partial<ListedItem> = {}): ListedItem {
  return {
    kind: "issue",
    slug: "sync-status",
    title: "Sync status",
    description: "Show repository sync failures in the local dashboard.",
    state: {
      itemSlug: "sync-status",
      status: "open",
    },
    ...overrides,
  };
}

async function requestRawJson(
  app: Express,
  method: string,
  path: string,
  body: string,
): Promise<{ statusCode: number; body: unknown }> {
  const server = app.listen(0);

  try {
    await new Promise<void>((resolve) => server.once("listening", resolve));
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("Could not determine test server address.");
    }

    const response = await fetch(`http://127.0.0.1:${address.port}${path}`, {
      method,
      headers: {
        "content-type": "application/json",
      },
      body,
    });

    return {
      statusCode: response.status,
      body: await response.json(),
    };
  } finally {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error) reject(error);
        else resolve();
      });
    });
  }
}

describe("skill issues server API", () => {
  it("returns repository items", async () => {
    const items = [
      listedItem(),
      listedItem({
        kind: "idea",
        slug: "guided-triage",
        title: "Guided triage",
        description: "Group open reports into a short morning review.",
        state: {
          itemSlug: "guided-triage",
          status: "in_progress",
        },
      }),
    ];
    const fakeRepo = {
      listItems: vi.fn(async () => items),
      upsertItem: vi.fn(),
      assignMe: vi.fn(),
      setStatus: vi.fn(),
    };
    const app = createSkillIssuesServer({ openRepository: async () => fakeRepo });

    const response = await requestJson(app, "GET", "/api/items");

    expect(response).toEqual({
      statusCode: 200,
      body: items,
    });
    expect(fakeRepo.listItems).toHaveBeenCalledWith({});
  });

  it("creates repository items", async () => {
    const fakeRepo = {
      listItems: vi.fn(),
      upsertItem: vi.fn(async (item: IssueItem) => listedItem(item)),
      assignMe: vi.fn(),
      setStatus: vi.fn(),
    };
    const app = createSkillIssuesServer({ openRepository: async () => fakeRepo });
    const item: IssueItem = {
      kind: "issue",
      slug: "stale-preview",
      title: "Stale preview",
      description: "The browser preview keeps showing an old item list.",
    };

    const response = await requestJson(app, "POST", "/api/items", item);

    expect(response).toEqual({
      statusCode: 201,
      body: { ok: true },
    });
    expect(fakeRepo.upsertItem).toHaveBeenCalledWith(item);
  });

  it("returns a skill issues client error for malformed API JSON", async () => {
    const fakeRepo = {
      listItems: vi.fn(),
      upsertItem: vi.fn(),
      assignMe: vi.fn(),
      setStatus: vi.fn(),
    };
    const app = createSkillIssuesServer({ openRepository: async () => fakeRepo });

    const response = await requestRawJson(app, "POST", "/api/items", "{");

    expect(response).toEqual({
      statusCode: 400,
      body: { error: "Malformed JSON" },
    });
    expect(fakeRepo.upsertItem).not.toHaveBeenCalled();
  });

  it("assigns an item to the verified user", async () => {
    const fakeRepo = {
      listItems: vi.fn(),
      upsertItem: vi.fn(),
      assignMe: vi.fn(async () => listedItem()),
      setStatus: vi.fn(),
    };
    const app = createSkillIssuesServer({ openRepository: async () => fakeRepo });

    const response = await requestJson(app, "POST", "/api/items/sync-status/assign-me");

    expect(response).toEqual({
      statusCode: 200,
      body: { ok: true },
    });
    expect(fakeRepo.assignMe).toHaveBeenCalledWith("sync-status");
  });

  it("updates item status", async () => {
    const fakeRepo = {
      listItems: vi.fn(),
      upsertItem: vi.fn(),
      assignMe: vi.fn(),
      setStatus: vi.fn(async (_slug: string, status: ItemStatus) =>
        listedItem({
          state: {
            itemSlug: "sync-status",
            status,
          },
        }),
      ),
    };
    const app = createSkillIssuesServer({ openRepository: async () => fakeRepo });

    const response = await requestJson(app, "POST", "/api/items/sync-status/status", {
      status: "done",
    });

    expect(response).toEqual({
      statusCode: 200,
      body: { ok: true },
    });
    expect(fakeRepo.setStatus).toHaveBeenCalledWith("sync-status", "done");
  });

  it("exports repository items to markdown todo files", async () => {
    const items = [listedItem()];
    const fakeRepo = {
      listItems: vi.fn(async () => items),
      upsertItem: vi.fn(),
      assignMe: vi.fn(),
      setStatus: vi.fn(),
    };
    const exportMarkdownTodo = vi.fn(async () => undefined);
    const app = createSkillIssuesServer({
      openRepository: async () => fakeRepo,
      exportMarkdownTodo,
      cwd: "/repo",
    });

    const response = await requestJson(app, "POST", "/api/export");

    expect(response).toEqual({
      statusCode: 200,
      body: { ok: true },
    });
    expect(fakeRepo.listItems).toHaveBeenCalledWith({});
    expect(exportMarkdownTodo).toHaveBeenCalledWith("/repo/todo", [
      {
        kind: "issue",
        slug: "sync-status",
        title: "Sync status",
        description: "Show repository sync failures in the local dashboard.",
      },
    ]);
  });
});
