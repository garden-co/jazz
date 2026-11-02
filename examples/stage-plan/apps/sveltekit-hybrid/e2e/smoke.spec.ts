import { expect, test } from "@playwright/test";

const TIMEOUT = 60_000;

test("a guest plans a show, signs up without losing it, and it survives a reload", async ({
  page,
}) => {
  const run = Date.now().toString(36);
  await page.goto("/");

  // A local-first guest: no sign-up needed to start planning.
  await page.getByLabel("Your name", { exact: true }).fill("Ada");
  await page.getByRole("button", { name: "Continue" }).click();
  await page.getByLabel("Show", { exact: true }).fill(`Album launch ${run}`);
  await page.getByLabel("Venue", { exact: true }).fill("The Lantern");
  await page.getByLabel("Date", { exact: true }).fill("2026-12-04");
  await page.getByLabel("Doors", { exact: true }).fill("19:30");
  await page.getByRole("button", { name: "Add show" }).click();
  await page.getByLabel("New task", { exact: true }).fill("Line check");
  await page.getByRole("button", { name: "Add task" }).click();
  await page.getByLabel("Status of Line check").selectOption("doing");
  const inProgress = page.getByRole("region", { name: "In progress" });
  await expect(inProgress.getByText("Line check")).toBeVisible({ timeout: TIMEOUT });

  // Creating an account keeps the guest's board.
  await page.getByRole("button", { name: "Create an account" }).click();
  await page.getByLabel("Name", { exact: true }).fill("Ada");
  await page.getByLabel("Email", { exact: true }).fill(`ada-${run}@stage-plan.test`);
  await page.getByLabel("Password", { exact: true }).fill("correct horse battery");
  await page.getByRole("button", { name: "Create account" }).click();
  await expect(page.getByText(`Signed in as ada-${run}@stage-plan.test`)).toBeVisible({
    timeout: TIMEOUT,
  });
  await page.getByLabel("New task", { exact: true }).fill("Soundcheck");
  await page.getByRole("button", { name: "Add task" }).click();
  await expect(page.getByRole("region", { name: "To do" }).getByText("Soundcheck")).toBeVisible({
    timeout: TIMEOUT,
  });

  // After a reload the account signs back in through the backend.
  await page.reload();
  await expect(page.getByText(`Signed in as ada-${run}@stage-plan.test`)).toBeVisible({
    timeout: TIMEOUT,
  });
  await expect(
    page.getByRole("region", { name: "In progress" }).getByText("Line check"),
  ).toBeVisible({ timeout: TIMEOUT });
  await expect(page.getByRole("region", { name: "To do" }).getByText("Soundcheck")).toBeVisible({
    timeout: TIMEOUT,
  });
});
