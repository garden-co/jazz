import { expect, test } from "@playwright/test";

const TIMEOUT = 20_000;

test("revoked session cookies cannot reopen the dashboard", async ({ browser, page }) => {
  const runId = Date.now();
  const credentials = {
    name: "Session Validation User",
    email: `session-validation-${runId}@example.com`,
    password: "testpassword",
  };

  await page.goto("/", { waitUntil: "networkidle" });
  const signUpResponse = await page.request.post("/api/auth/sign-up/email", {
    data: credentials,
  });
  expect(signUpResponse.ok()).toBe(true);

  const sessionCookie = (await page.context().cookies()).find((cookie) =>
    cookie.name.includes("session_token"),
  );
  if (!sessionCookie) {
    throw new Error("Better Auth did not issue a session cookie");
  }

  const origin = new URL(page.url()).origin;
  const dashboardResponse = await page.goto(`${origin}/dashboard`, {
    waitUntil: "networkidle",
  });
  expect(dashboardResponse?.ok()).toBe(true);
  await expect(page).toHaveURL(`${origin}/dashboard`, { timeout: TIMEOUT });
  await expect(page.getByRole("button", { name: "Sign out" })).toBeVisible({
    timeout: TIMEOUT,
  });

  const signOutResponse = await page.request.post("/api/auth/sign-out");
  expect(signOutResponse.ok()).toBe(true);

  const replayContext = await browser.newContext();
  try {
    await replayContext.addCookies([sessionCookie]);
    const replayPage = await replayContext.newPage();
    await replayPage.goto(`${origin}/dashboard`, { waitUntil: "networkidle" });

    await expect(replayPage).toHaveURL(`${origin}/`, { timeout: TIMEOUT });
    await expect(replayPage.getByRole("heading", { name: "Sign in" })).toBeVisible({
      timeout: TIMEOUT,
    });
  } finally {
    await replayContext.close();
  }
});
