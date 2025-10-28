import { expect, test } from "@playwright/test";

test("start a new game and play", async ({ page: marioPage, browser }) => {
  await marioPage.goto("/");

  await marioPage.getByRole("button", { name: /Start New Game/ }).click();

  await expect(marioPage.getByText("Waiting for opponent")).toBeVisible();

  const url = await marioPage.url();

  const luigiContext = await browser.newContext();
  const luigiPage = await luigiContext.newPage();
  await luigiPage.goto(url);

  await expect(marioPage.getByText("Waiting for selection")).toBeVisible();
  await expect(luigiPage.getByText("Waiting for selection")).toBeVisible();
  await expect(luigiPage.getByText("Waiting for opponent")).toBeVisible();

  await marioPage.getByLabel("Select Rock").click();
  await marioPage.getByRole("button", { name: /Make your move!/i }).click();

  await expect(
    luigiPage.getByText("The opponent has made their move"),
  ).toBeVisible();

  await luigiPage.getByLabel("Select Paper").click();
  await luigiPage.getByRole("button", { name: /Make your move!/i }).click();

  await expect(luigiPage.getByText(/🎉 You Win! 🎉/)).toBeVisible();
  await expect(marioPage.getByText(/😔 You Lose! 😔/)).toBeVisible();

  await marioPage.getByRole("button", { name: /Start New Game/ }).click();

  await expect(marioPage.getByText("Waiting for opponent")).toBeVisible();
  await expect(luigiPage.getByText("Waiting for opponent")).toBeVisible();
});
