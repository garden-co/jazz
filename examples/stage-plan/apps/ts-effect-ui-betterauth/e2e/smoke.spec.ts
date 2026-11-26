import { expect, test } from "@playwright/test";

const TIMEOUT = 60_000;

test("a crew chief plans a show, the server adds the checklist, and it survives a reload", async ({
  page,
}) => {
  const run = Date.now().toString(36);
  await page.goto("/");
  await page.getByRole("button", { name: "Create an account" }).click();
  await page.getByLabel("Name", { exact: true }).fill("Ada");
  await page.getByLabel("Email", { exact: true }).fill(`ada-${run}@stage-plan.test`);
  await page.getByLabel("Password", { exact: true }).fill("correct horse battery");
  await page.getByRole("button", { name: "Create account" }).click();

  await page.getByLabel("Your name", { exact: true }).fill("Ada");
  await page.getByRole("button", { name: "Continue" }).click();
  await page.getByLabel("Show", { exact: true }).fill(`Album launch ${run}`);
  await page.getByLabel("Venue", { exact: true }).fill("The Lantern");
  await page.getByLabel("Date", { exact: true }).fill("2026-12-04");
  await page.getByLabel("Doors", { exact: true }).fill("19:30");
  await page.getByRole("button", { name: "Add show" }).click();

  // Written in the browser through Effect.
  await page.getByLabel("New task", { exact: true }).fill("Book backline");
  await page.getByRole("button", { name: "Add task" }).click();
  const todo = page.getByRole("region", { name: "To do" });
  await expect(todo.getByText("Book backline")).toBeVisible({ timeout: TIMEOUT });

  // Written on the server, as this user, through Effect.
  await page.getByRole("button", { name: "Add standard checklist" }).click();
  await expect(todo.getByText("Soundcheck")).toBeVisible({ timeout: TIMEOUT });
  await page.getByLabel("Status of Soundcheck").selectOption("doing");
  const inProgress = page.getByRole("region", { name: "In progress" });
  await expect(inProgress.getByText("Soundcheck")).toBeVisible({ timeout: TIMEOUT });

  await page.reload();
  await expect(
    page.getByRole("region", { name: "In progress" }).getByText("Soundcheck"),
  ).toBeVisible({
    timeout: TIMEOUT,
  });
  await expect(page.getByRole("region", { name: "To do" }).getByText("Book backline")).toBeVisible({
    timeout: TIMEOUT,
  });
  await expect(page.getByRole("region", { name: "To do" }).getByText("Line check")).toBeVisible();
});
