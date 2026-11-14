import { expect, test, type Page } from "@playwright/test";

const TIMEOUT = 60_000;

async function signUp(page: Page, run: string) {
  await page.goto("/");
  await page.getByRole("button", { name: "Create an account instead" }).click();
  await page.getByLabel("Your name").fill("Ada");
  await page.getByLabel("Email").fill(`ada-${run}@big-label.test`);
  await page.getByLabel("Password").fill("correct horse battery");
  await page.getByRole("button", { name: "Create account" }).click();

  // The backend bootstraps a personal label with this account as its admin.
  const nav = page.getByRole("navigation");
  await expect(nav.getByRole("link", { name: "Artists" })).toBeVisible({ timeout: TIMEOUT });
  return nav;
}

async function addArtist(page: Page, name: string) {
  await page.getByRole("navigation").getByRole("link", { name: "Artists" }).click();
  await page.getByRole("button", { name: "Add artist" }).first().click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("Name").fill(name);
  await dialog.getByRole("button", { name: "Add artist" }).click();
  await expect(page.getByText(name).first()).toBeVisible({ timeout: TIMEOUT });
}

test("a new account gets a personal label, adds an artist and it survives a reload", async ({
  page,
}) => {
  const run = Date.now().toString(36);
  await signUp(page, run);
  const artist = `The Analytical Engines ${run}`;
  await addArtist(page, artist);

  await page.reload();
  await expect(page.getByText(artist).first()).toBeVisible({ timeout: TIMEOUT });
});

test("the release dialog refuses a catalogue number that is already in use", async ({ page }) => {
  const run = Date.now().toString(36);
  await signUp(page, run);
  const artist = `The Difference Engines ${run}`;
  await addArtist(page, artist);
  const artistUrl = page.url();

  const addRelease = async (title: string) => {
    await page.goto(artistUrl);
    await page.getByRole("button", { name: "Add release" }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByLabel("Title").fill(title);
    await dialog.getByLabel("Catalogue number").fill("ENG-001");
    await dialog.getByRole("button", { name: "Add release" }).click();
    return dialog;
  };

  // The first release takes the number and the dialog closes.
  const first = await addRelease("First light");
  await expect(first).toBeHidden({ timeout: TIMEOUT });
  await expect(page.getByRole("heading", { name: "First light" })).toBeVisible({
    timeout: TIMEOUT,
  });

  // A second one gets the error on the field, from saveRelease, and stays open.
  const second = await addRelease("Second light");
  await expect(second.getByText("ENG-001 is already in use")).toBeVisible({ timeout: TIMEOUT });
  await expect(second).toBeVisible();
  await second.getByLabel("Catalogue number").fill("ENG-002");
  await second.getByRole("button", { name: "Add release" }).click();
  await expect(second).toBeHidden({ timeout: TIMEOUT });
});
