import { expect, test } from "@playwright/test";

test.describe("Sharing", () => {
  test("should share simple coValues", async ({ page, browser }) => {
    await page.goto("/sharing");

    await page.getByRole("button", { name: "Create the root" }).click();
    await expect(page.getByTestId("id")).not.toHaveText("", {
      timeout: 20_000,
    });

    const id = await page.getByTestId("id").textContent();
    const inviteLink = await page
      .getByTestId("invite-link-reader")
      .textContent();

    // Create a new incognito instance and accept the invite
    const newUserPage = await (await browser.newContext()).newPage();
    await newUserPage.goto(inviteLink!);

    await expect(newUserPage.getByTestId("id")).toHaveText(id ?? "", {
      timeout: 20_000,
    });
  });

  test("should reveal internal values on group extension", async ({
    page,
    browser,
  }) => {
    await page.goto("/sharing");

    await page.getByRole("button", { name: "Create the root" }).click();
    await expect(page.getByTestId("id")).not.toHaveText("", {
      timeout: 20_000,
    });

    await page.getByRole("button", { name: "Add a child" }).click();

    await expect(page.getByTestId("values")).toContainText(
      "CoValue root ---> CoValue child 1",
    );

    const inviteLink = await page
      .getByTestId("invite-link-reader")
      .textContent();

    // Create a new incognito instance and accept the invite
    const newUserPage = await (await browser.newContext()).newPage();
    await newUserPage.goto(inviteLink!);

    // The user should not have access to the internal values
    // because they are part of a different group
    await expect(newUserPage.getByTestId("values")).toContainText(
      "CoValue root",
      {
        timeout: 20_000,
      },
    );
    await expect(newUserPage.getByTestId("values")).not.toContainText(
      "CoValue root ---> CoValue child 1",
    );

    // Extend the coMaps group with the coList group
    await page.getByRole("button", { name: "Share the children" }).click();

    // The user should now have access to the internal values
    await expect(newUserPage.getByTestId("values")).toContainText(
      "CoValue root ---> CoValue child 1",
    );
  });

  test("should reveal internal values if another user extends the group", async ({
    page,
    browser,
  }) => {
    await page.goto("/sharing");

    await page.getByRole("button", { name: "Create the root" }).click();
    await expect(page.getByTestId("id")).not.toHaveText("", {
      timeout: 20_000,
    });

    const inviteLink = await page
      .getByTestId("invite-link-admin")
      .textContent();

    // Create a new incognito instance and accept the invite
    const newUserPage = await (await browser.newContext()).newPage();
    await newUserPage.goto(inviteLink!);

    await expect(newUserPage.getByTestId("values")).toContainText(
      "CoValue root",
      {
        timeout: 20_000,
      },
    );

    await newUserPage.getByRole("button", { name: "Add a child" }).click();
    await newUserPage.getByRole("button", { name: "Add a child" }).click();
    await newUserPage
      .getByRole("button", { name: "Reveal next level" })
      .click();
    await newUserPage
      .getByRole("button", { name: "Share the children" })
      .click();

    await page.getByRole("button", { name: "Reveal next level" }).click();

    // The user should now have access to the internal values
    await expect(page.getByTestId("values")).toContainText(
      "CoValue root ---> CoValue child 1 ---> CoValue child 2",
    );
  });
});
