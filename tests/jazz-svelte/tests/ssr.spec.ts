import { expect, test } from "@playwright/test";

test("SSR should show the updated profile name after navigation", async ({
  page,
}) => {
  await page.goto("/ssr");
  const input = page.getByTestId("name-input");
  await expect(input).toBeVisible({ timeout: 15000 });
  // Throttle the network to ensure sync message is slow
  const cdpSession = await page.context().newCDPSession(page);
  await cdpSession.send("Network.emulateNetworkConditions", {
    offline: false,
    downloadThroughput: -1,
    uploadThroughput: 1024,
    latency: 500,
  });

  await input.fill("TestUpdatedName");
  await page.getByTestId("navigate").click();

  await expect(page.getByTestId("ssr-profile-name")).not.toHaveText(
    "Anonymous user",
  );

  await expect(page.getByTestId("ssr-profile-name")).toHaveText(
    "TestUpdatedName",
  );
});
