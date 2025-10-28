import { type BrowserContext, expect, test } from "@playwright/test";

test("create a new organization and share", async ({
  page: marioPage,
  browser,
}) => {
  await marioPage.goto("/");

  const luigiContext = await browser.newContext();
  const luigiPage = await luigiContext.newPage();
  await luigiPage.goto("/");

  await test.step("Set the profile names", async () => {
    await marioPage
      .getByRole("textbox", { name: "Profile name" })
      .fill("Mario");
    await luigiPage
      .getByRole("textbox", { name: "Profile name" })
      .fill("Luigi");
  });

  await test.step("Create a new organization", async () => {
    await marioPage
      .getByRole("textbox", { name: "Organization name" })
      .fill("Mario's organization");
    await marioPage.getByRole("button", { name: "Create" }).click();

    await expect(
      marioPage.getByRole("heading", { name: "Mario's organization" }),
    ).toBeVisible();
  });

  await test.step("Invite Luigi to the organization", async () => {
    await marioPage.getByRole("button", { name: "Copy invite link" }).click();

    const inviteUrl = await marioPage.evaluate(() =>
      navigator.clipboard.readText(),
    );

    await luigiPage.goto(inviteUrl);

    await expect(
      luigiPage.getByRole("heading", { name: "Mario's organization" }),
    ).toBeVisible();
    await expect(marioPage.getByText("Luigi")).toBeVisible();
  });

  await test.step("Kick out Luigi from the organization", async () => {
    await marioPage.getByRole("button", { name: "Remove" }).click();

    await expect(marioPage.getByText("Luigi")).not.toBeVisible();

    await expect(
      luigiPage.getByRole("heading", {
        name: "You don't have access to this organization",
      }),
    ).toBeVisible();

    await luigiPage.getByRole("link", { name: "Go back to home" }).click();

    await expect(
      luigiPage.getByRole("heading", { name: "Organizations example app" }),
    ).toBeVisible();
  });
});
