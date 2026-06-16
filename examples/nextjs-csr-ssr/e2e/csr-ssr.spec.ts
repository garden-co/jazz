import { expect, test, type Locator, type Page } from "@playwright/test";

function todoSection(page: Page, label: string): Locator {
  return page.locator("section", {
    has: page.getByText(label, { exact: true }),
  });
}

async function addTodo(section: Locator, title: string) {
  await section.locator('input[name="titleField"]').fill(title);
  await section.locator('button[type="submit"]').click();
}

async function reloadUntilSectionContains(page: Page, label: string, title: string) {
  const deadline = Date.now() + 15_000;

  while (Date.now() < deadline) {
    await page.reload();

    if ((await todoSection(page, label).textContent())?.includes(title)) {
      return;
    }

    await page.waitForTimeout(250);
  }

  throw new Error(`Timed out waiting for ${label} to render "${title}" after reload`);
}

test.describe("Next.js CSR / SSR todos", () => {
  test("client and server panes round-trip todos through the shared Jazz backend", async ({
    page,
  }) => {
    // Flow:
    // browser client submit -> sync server -> reload -> Next RSC render
    // Next server action submit -> sync server -> live client subscription update
    const clientTitle = `alice books train tickets ${Date.now()}`;
    const serverTitle = `bob files travel reimbursement ${Date.now()}`;

    await page.goto("/");

    const clientPane = todoSection(page, "Client-side (React)");

    await expect(page.getByRole("heading", { name: "jazz — nextjs CSR / SSR" })).toBeVisible();
    // No empty-state precondition: the sync server is shared and persistent, so
    // the panes may already hold rows. The round-trip below asserts on unique,
    // freshly-added titles instead.

    await addTodo(clientPane, clientTitle);
    await expect(clientPane).toContainText(clientTitle);

    await reloadUntilSectionContains(page, "Server-side (RSC)", clientTitle);

    const reloadedClientPane = todoSection(page, "Client-side (React)");
    const reloadedServerPane = todoSection(page, "Server-side (RSC)");
    await expect(reloadedServerPane).toContainText(clientTitle);

    await addTodo(reloadedServerPane, serverTitle);
    await expect(reloadedServerPane).toContainText(serverTitle);
    await expect(reloadedClientPane).toContainText(serverTitle, { timeout: 15_000 });
  });

  test("the prefetch+hydrate pane is server-rendered with its rows — no cold-load flash", async ({
    page,
    request,
  }) => {
    const title = `prefetched and hydrated ${Date.now()}`;
    const paneLabel = "Server prefetch + client hydrate";

    // Seed a row so the server prefetch has data, then wait until the
    // prefetch+hydrate pane reflects it.
    await page.goto("/");
    await addTodo(todoSection(page, "Server-side (RSC)"), title);
    await reloadUntilSectionContains(page, paneLabel, title);

    // The RAW server-rendered HTML (no client JS) must already carry the row in
    // the prefetch+hydrate pane: the snapshot seeded it on the server, so the
    // browser's first paint is not empty and won't flash to "No todos yet" when
    // the live store connects.
    const html = await (await request.get("/")).text();
    const paneStart = html.indexOf(paneLabel);
    expect(paneStart).toBeGreaterThan(-1);
    const paneHtml = html.slice(paneStart);
    expect(paneHtml).toContain(title);
    expect(paneHtml).not.toContain("No todos yet.");

    // After hydration the same row is still shown — SSR first paint matched the
    // live client, with no flash in between.
    await expect(todoSection(page, paneLabel)).toContainText(title);
  });
});
