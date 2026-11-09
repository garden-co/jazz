import { expect, test, type Browser } from "@playwright/test";

const TIMEOUT = 60_000;
const run = Date.now().toString(36);

async function signUp(browser: Browser, name: string) {
  const context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  const page = await context.newPage();
  page.setDefaultTimeout(TIMEOUT);
  await page.goto("/");
  await page.getByRole("button", { name: "Create an account" }).click();
  await page.getByLabel("Name").fill(name);
  await page.getByLabel("Email").fill(`${name.toLowerCase()}-${run}@band-book.test`);
  await page.getByLabel("Password").fill("correct horse battery");
  await page.getByRole("button", { name: "Create account" }).click();
  await page.waitForURL(/\/workspace/);
  await page.getByRole("button", { name: "Setlist: spring tour" }).first().waitFor();
  return page;
}

test("the band's seeded pages, a shared song and a live edit reach a guest", async ({
  browser,
}) => {
  const owner = await signUp(browser, "Ada");
  const sidebarItem = (name: string) => owner.getByRole("button", { name, exact: true }).first();

  await sidebarItem("Setlist: spring tour").click();
  await expect(owner.getByLabel("To-do", { exact: true }).first()).toBeVisible();

  await sidebarItem("Songs").click();
  await sidebarItem("Harbour lights").click();
  const title = owner.getByRole("textbox", { name: "Page title" });
  await expect(title).toHaveValue("Harbour lights");
  await owner.getByRole("button", { name: "Add block" }).click();
  await owner.getByRole("menuitem", { name: "Text" }).click();
  await owner.keyboard.type("Bridge: stay on the IV chord");

  await sidebarItem("Issues").click();
  await expect(owner.getByRole("table")).toBeVisible();
  await expect(owner.getByText("Confirm backline with Porto venue")).toBeVisible();

  await sidebarItem("Harbour lights").click();
  await owner.getByRole("button", { name: "Share" }).click();
  await owner.getByRole("button", { name: "Create link" }).click();
  const linkInput = owner.getByRole("dialog").getByRole("textbox").last();
  await expect(linkInput).toHaveValue(/\/invite\//);
  const inviteUrl = await linkInput.inputValue();
  await owner.keyboard.press("Escape");

  // The guest redeems the link and sees the shared song only.
  const guest = await signUp(browser, "Bo");
  await guest.goto(inviteUrl);
  await guest.waitForURL(/p=/);
  await expect(guest.getByRole("textbox", { name: "Page title" })).toHaveValue("Harbour lights");
  await expect(
    guest
      .getByText("Bridge: stay on the IV chord")
      .or(guest.locator("textarea", { hasText: "Bridge" }))
      .first(),
  ).toBeVisible();
  const guestNav = guest.getByRole("navigation", { name: "Side navigation" });
  await expect(guestNav).toContainText("Harbour lights");
  await expect(guestNav).not.toContainText("Tour notes");

  // The owner's edit reaches the guest without a reload.
  await title.fill("Harbour lights (final)");
  await expect(guest.getByRole("textbox", { name: "Page title" })).toHaveValue(
    "Harbour lights (final)",
    { timeout: TIMEOUT },
  );
});
