import { expect, test } from "@playwright/test";

test.describe("Concurrent Changes", () => {
  test("should complete the task without incurring on InvalidSignature errors", async ({
    page,
    context,
  }) => {
    await page.goto("/concurrent-changes");
    const newPage = await context.newPage();

    await page.getByRole("button", { name: "Create a new value!" }).click();

    await newPage.goto(page.url());

    await page.getByTestId("done").waitFor();

    await newPage.close();

    const errorLogs: string[] = [];

    page.on("console", (message) => {
      if (message.type() === "error") {
        errorLogs.push(message.text());
      }
    });

    await page.reload();

    await expect(page.getByTestId("done")).toBeVisible();

    expect(
      errorLogs.find((log) => log.includes("InvalidSignature")),
    ).toBeUndefined();
  });
});
