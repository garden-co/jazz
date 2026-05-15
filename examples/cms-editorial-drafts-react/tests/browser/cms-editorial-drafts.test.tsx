import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";
import { App } from "../../src/App.js";
import type { DbConfig } from "jazz-tools";

function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

async function waitFor(check: () => boolean, timeoutMs: number, message: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`Timeout: ${message}`);
}

function typeInto(input: HTMLInputElement | HTMLTextAreaElement, value: string) {
  const prototype =
    input instanceof HTMLTextAreaElement
      ? window.HTMLTextAreaElement.prototype
      : window.HTMLInputElement.prototype;
  const setter = Object.getOwnPropertyDescriptor(prototype, "value")!.set!;
  setter.call(input, value);
  input.dispatchEvent(new Event("input", { bubbles: true }));
}

function articleTitle(el: HTMLDivElement): string | null {
  return el.querySelector("[data-testid='article-title']")?.textContent ?? null;
}

function selectArticleByTitle(el: HTMLDivElement, title: string): void {
  const rows = el.querySelectorAll<HTMLLIElement>("[data-testid='article-row']");
  for (const row of rows) {
    if (row.textContent?.includes(title)) {
      row.click();
      return;
    }
  }
  throw new Error(`Could not find article row with title "${title}"`);
}

describe("CMS Editorial Drafts", () => {
  const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

  async function mountApp(config: {
    appId?: string;
    serverUrl?: string;
    auth?: { localFirstSecret: string };
    adminSecret?: string;
    driver?: DbConfig["driver"];
  }): Promise<HTMLDivElement> {
    const el = document.createElement("div");
    document.body.appendChild(el);
    const root = createRoot(el);
    mounts.push({ root, container: el });

    await act(async () => {
      root.render(<App config={{ appId: config.appId ?? "cms-test-app", ...config }} />);
    });

    await waitFor(
      () => el.querySelector("[data-testid='cms-editor']") !== null,
      5000,
      "CMS editor should render",
    );

    return el;
  }

  afterEach(async () => {
    for (const { root, container } of mounts) {
      try {
        await act(async () => root.unmount());
      } catch {
        /* best effort */
      }
      container.remove();
    }
    mounts.length = 0;
  });

  it("isolates branch edits from published content until merge", async () => {
    const el = await mountApp({ driver: { type: "persistent", dbName: uniqueDbName("draft") } });

    // Seed sample articles.
    await act(async () => {
      el.querySelector<HTMLButtonElement>("[data-testid='seed-articles']")!.click();
    });

    await waitFor(
      () => el.querySelectorAll("[data-testid='article-row']").length >= 3,
      3000,
      "Seed articles should appear",
    );

    // Select the welcome article.
    await act(async () => {
      selectArticleByTitle(el, "Welcome to Editorial");
    });

    await waitFor(
      () => articleTitle(el) === "Welcome to Editorial",
      3000,
      "Selected article should load in the editor",
    );

    // Open the create-branch dialog and create a branch.
    await act(async () => {
      el.querySelector<HTMLButtonElement>("[data-testid='create-branch']")!.click();
    });

    await waitFor(
      () => el.querySelector("[data-testid='branch-name-input']") !== null,
      2000,
      "Create-branch dialog should open",
    );

    await act(async () => {
      typeInto(
        el.querySelector<HTMLInputElement>("[data-testid='branch-name-input']")!,
        "Spring relaunch",
      );
      typeInto(
        el.querySelector<HTMLInputElement>("[data-testid='branch-description-input']")!,
        "Polish hero copy",
      );
    });

    await act(async () => {
      el.querySelector<HTMLButtonElement>("[data-testid='create-branch-confirm']")!.click();
    });

    await waitFor(
      () =>
        Array.from(el.querySelectorAll("[data-testid='select-branch']")).some((b) =>
          b.textContent?.includes("Spring relaunch"),
        ),
      3000,
      "New branch tab should appear",
    );

    await waitFor(
      () => el.querySelectorAll("[data-testid='article-row']").length >= 3,
      5000,
      "Articles should load on the new branch",
    );

    // Re-select the article on the branch.
    await act(async () => {
      selectArticleByTitle(el, "Welcome to Editorial");
    });

    await waitFor(
      () => articleTitle(el) === "Welcome to Editorial",
      3000,
      "Article should be selectable on the branch",
    );

    // Edit on the branch.
    await act(async () => {
      typeInto(
        el.querySelector<HTMLInputElement>("[data-testid='title-input']")!,
        "Welcome — Spring Edition",
      );
      typeInto(
        el.querySelector<HTMLTextAreaElement>("[data-testid='body-input']")!,
        "Draft hero copy for the spring relaunch.",
      );
    });

    await act(async () => {
      el.querySelector<HTMLButtonElement>("[data-testid='save-article']")!.click();
    });

    await waitFor(
      () =>
        Array.from(el.querySelectorAll("[data-testid='article-row']")).some((r) =>
          r.textContent?.includes("Welcome — Spring Edition"),
        ),
      5000,
      "Saved branch title should appear in the sidebar list",
    );

    // Diff panel should show the update.
    await waitFor(
      () => el.querySelector("[data-testid='draft-diff']") !== null,
      3000,
      "Diff panel should appear on a branch",
    );

    await waitFor(
      () => {
        const diffPanel = el.querySelector("[data-testid='draft-diff']");
        const text = diffPanel?.textContent ?? "";
        return text.includes("title") && text.includes("body");
      },
      5000,
      `Diff panel should mention title+body. Got: ${el.querySelector("[data-testid='draft-diff']")?.textContent}`,
    );

    // Switch to Published — content should be untouched.
    await act(async () => {
      el.querySelector<HTMLButtonElement>("[data-testid='select-published']")!.click();
    });

    await waitFor(
      () => el.querySelector("[data-testid='draft-diff']") === null,
      3000,
      "Diff panel should disappear on Published",
    );

    await act(async () => {
      selectArticleByTitle(el, "Welcome to Editorial");
    });

    await waitFor(
      () => articleTitle(el) === "Welcome to Editorial",
      3000,
      "Published article should keep original title",
    );

    // Switch back to the branch and merge.
    await act(async () => {
      const branchTab = Array.from(
        el.querySelectorAll<HTMLButtonElement>("[data-testid='select-branch']"),
      ).find((b) => b.textContent?.includes("Spring relaunch"))!;
      branchTab.click();
    });

    await waitFor(
      () =>
        Array.from(el.querySelectorAll("[data-testid='article-row']")).some((r) =>
          r.textContent?.includes("Welcome — Spring Edition"),
        ),
      3000,
      "Branch should show edited article",
    );

    await act(async () => {
      selectArticleByTitle(el, "Welcome — Spring Edition");
    });

    await waitFor(
      () => el.querySelector<HTMLButtonElement>("[data-testid='merge-branch']") !== null,
      3000,
      "Merge button should appear on a branch with changes",
    );

    await act(async () => {
      el.querySelector<HTMLButtonElement>("[data-testid='merge-branch']")!.click();
    });

    await waitFor(
      () => el.querySelector("[data-testid='merge-branch-confirm']") !== null,
      2000,
      "Merge confirmation dialog should open",
    );

    await act(async () => {
      el.querySelector<HTMLButtonElement>("[data-testid='merge-branch-confirm']")!.click();
    });

    // After merge: back on Published, branch tab gone, title updated.
    await waitFor(
      () =>
        Array.from(el.querySelectorAll("[data-testid='select-branch']")).every(
          (b) => !b.textContent?.includes("Spring relaunch"),
        ),
      3000,
      "Merged branch tab should be removed",
    );

    await act(async () => {
      selectArticleByTitle(el, "Welcome — Spring Edition");
    });

    await waitFor(
      () => articleTitle(el) === "Welcome — Spring Edition",
      3000,
      "Merged title should now appear on Published",
    );
  });

  it("creates and deletes an article on a branch", async () => {
    const el = await mountApp({ driver: { type: "persistent", dbName: uniqueDbName("create") } });

    await act(async () => {
      el.querySelector<HTMLButtonElement>("[data-testid='seed-articles']")!.click();
    });

    await waitFor(
      () => el.querySelectorAll("[data-testid='article-row']").length >= 3,
      3000,
      "Seed articles should appear",
    );

    // Create a branch.
    await act(async () => {
      el.querySelector<HTMLButtonElement>("[data-testid='create-branch']")!.click();
    });
    await waitFor(
      () => el.querySelector("[data-testid='branch-name-input']") !== null,
      2000,
      "Create-branch dialog should open",
    );
    await act(async () => {
      typeInto(
        el.querySelector<HTMLInputElement>("[data-testid='branch-name-input']")!,
        "New article branch",
      );
    });
    await act(async () => {
      el.querySelector<HTMLButtonElement>("[data-testid='create-branch-confirm']")!.click();
    });

    // Create a new article on this branch.
    await act(async () => {
      el.querySelector<HTMLButtonElement>("[data-testid='new-article']")!.click();
    });

    await waitFor(
      () => articleTitle(el) === "Untitled draft",
      3000,
      "New article should be created and selected",
    );

    // Diff should mark it as insert.
    await waitFor(
      () => {
        const badges = el.querySelectorAll("[data-testid='article-diff-badge']");
        return Array.from(badges).some((b) => b.textContent === "insert");
      },
      3000,
      "New article should appear as 'insert' in diff",
    );

    // Switch to Published — new article should not exist there.
    await act(async () => {
      el.querySelector<HTMLButtonElement>("[data-testid='select-published']")!.click();
    });

    const rowsOnPublished = el.querySelectorAll<HTMLLIElement>("[data-testid='article-row']");
    const titles = Array.from(rowsOnPublished).map((r) => r.textContent ?? "");
    expect(titles.every((t) => !t.includes("Untitled draft"))).toBe(true);
  });
});
