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
  }, testInfo) => {
    // Flow:
    // browser client submit -> sync server -> reload -> Next RSC render
    // Next server action submit -> sync server -> live client subscription update
    const identity = `${testInfo.retry}-${randomUUID()}`;
    const clientTitle = `alice books train tickets ${identity}`;
    const serverTitle = `bob files travel reimbursement ${identity}`;

    await page.goto("/");

    const clientPane = todoSection(page, "Client-side (React)");
    const serverPane = todoSection(page, "Server-side (RSC)");

    await expect(page.getByRole("heading", { name: "jazz — nextjs CSR / SSR" })).toBeVisible();

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
});
