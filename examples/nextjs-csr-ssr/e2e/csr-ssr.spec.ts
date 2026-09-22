import { randomUUID } from "node:crypto";
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

async function reloadAndWaitForServerRow(page: Page, label: string, title: string) {
  // Exercise reload persistence once, then leave the foreground alive long
  // enough to reconnect and upload its durable outbox. Repeated reloads during
  // "Loading account…" can indefinitely restart that required work.
  await page.reload();
  await expect(todoSection(page, "Client-side (React)")).toContainText(title, {
    timeout: 15_000,
  });

  // Each request performs a fresh server render without tearing down the
  // browser that is synchronizing the write. The final reload still verifies
  // that the visible RSC pane receives the server's persisted row.
  await expect
    .poll(
      async () => {
        const response = await page.request.get("/");
        expect(response.ok()).toBe(true);
        return (await response.text()).includes(title);
      },
      { timeout: 15_000 },
    )
    .toBe(true);
  await page.reload();
  await expect(todoSection(page, label)).toContainText(title);
}

test.describe("Next.js CSR / SSR todos", () => {
  test("client and server panes round-trip todos through the shared Jazz backend", async ({
    page,
  }, testInfo) => {
    // Flow:
    // browser client submit -> sync server -> reload -> Next RSC render
    // Next server action submit -> sync server -> live client subscription update
    const identity = `${testInfo.retry}-${randomUUID()}`;
    const clientTitle = `alice books train tickets ${identity}`;
    const serverTitle = `bob files travel reimbursement ${identity}`;

    await page.goto("/");

    const clientPane = todoSection(page, "Client-side (React)");

    await expect(page.getByRole("heading", { name: "jazz — nextjs CSR / SSR" })).toBeVisible();

    await addTodo(clientPane, clientTitle);
    await expect(clientPane).toContainText(clientTitle);
    await expect(clientPane.getByRole("status")).toHaveText("Saved locally");

    await reloadAndWaitForServerRow(page, "Server-side (RSC)", clientTitle);

    const reloadedClientPane = todoSection(page, "Client-side (React)");
    const reloadedServerPane = todoSection(page, "Server-side (RSC)");
    await expect(reloadedServerPane).toContainText(clientTitle);

    await addTodo(reloadedServerPane, serverTitle);
    await expect(reloadedServerPane).toContainText(serverTitle);
    await expect(reloadedClientPane).toContainText(serverTitle, { timeout: 15_000 });
  });
});
