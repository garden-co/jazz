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
  const dashboardResponse = await page.request.get(`${origin}/dashboard`, {
    maxRedirects: 0,
  });
  expect(dashboardResponse.status()).toBe(200);

  const signOutResponse = await page.request.post("/api/auth/sign-out", {
    headers: { Origin: origin },
  });
  expect(signOutResponse.ok()).toBe(true);

  const replayContext = await browser.newContext();
  try {
    await replayContext.addCookies([sessionCookie]);
    const replayResponse = await replayContext.request.get(`${origin}/dashboard`, {
      maxRedirects: 0,
    });

    expect(replayResponse.status()).toBe(303);
    expect(replayResponse.headers().location).toBe("/");
  } finally {
    await replayContext.close();
  }
});
