import { expect, test } from "@playwright/test";

test("clerk integration - complete sign in flow", async ({ page }) => {
  await page.goto("/");

  // Wait for page to load
  await page.waitForTimeout(3000);

  console.log("=== INITIAL PAGE STATE ===");
  const pageText = await page.textContent("body");
  console.log("Full page text:", pageText);
  console.log(
    'Page contains "You\'re not logged in":',
    pageText?.includes("You're not logged in"),
  );

  // Check if Clerk is loaded
  const clerkLoaded = await page.evaluate(() => {
    return typeof (window as any).Clerk !== "undefined";
  });
  console.log("Clerk loaded:", clerkLoaded);

  // Check if Clerk publishable key is available in the page
  const hasClerkKey = await page.evaluate(() => {
    return document.querySelector('script[src*="clerk"]') !== null;
  });
  console.log("Clerk scripts loaded:", hasClerkKey);

  // Check for any Vue app errors
  const vueErrors = await page.evaluate(() => {
    return (
      (window as any).__VUE_DEVTOOLS_GLOBAL_HOOK__?.Vue?.config?.errorHandler ||
      null
    );
  });
  console.log("Vue errors:", vueErrors);

  console.log("=== CLICKING SIGN IN ===");
  const signInButton = page.getByRole("button", { name: "Sign in" });
  const signInExists = await signInButton.isVisible();
  console.log("Sign in button exists:", signInExists);

  if (!signInExists) {
    console.log("Sign in button not found, taking screenshot");
    await page.screenshot({ path: "debug-no-signin-button.png" });
    return;
  }

  await signInButton.click();
  console.log("Clicked sign in button");

  // Wait and check what happens
  await page.waitForTimeout(1000);

  // Check for any modals or overlays
  const modals = await page
    .locator(
      '[role="dialog"], .cl-modal, .clerk-modal, [data-testid*="modal"], [class*="modal"]',
    )
    .all();
  console.log("Found modals/dialogs:", modals.length);

  for (let i = 0; i < modals.length; i++) {
    const modal = modals[i];
    const isVisible = await modal.isVisible();
    const className = await modal.getAttribute("class");
    console.log(`Modal ${i}: visible=${isVisible}, class="${className}"`);
  }

  // Check for any iframes (Clerk might use iframes)
  const iframes = await page.locator("iframe").all();
  console.log("Found iframes:", iframes.length);

  for (let i = 0; i < iframes.length; i++) {
    const iframe = iframes[i];
    const src = await iframe.getAttribute("src");
    const isVisible = await iframe.isVisible();
    console.log(`Iframe ${i}: visible=${isVisible}, src="${src}"`);
  }

  // Check for any new elements that appeared
  await page.waitForTimeout(2000);
  const allInputs = await page.locator("input").all();
  console.log("Total inputs on page:", allInputs.length);

  for (let i = 0; i < allInputs.length; i++) {
    const input = allInputs[i];
    const type = await input.getAttribute("type");
    const name = await input.getAttribute("name");
    const placeholder = await input.getAttribute("placeholder");
    const isVisible = await input.isVisible();
    console.log(
      `Input ${i}: type="${type}" name="${name}" placeholder="${placeholder}" visible=${isVisible}`,
    );
  }

  // Check for any error messages
  const errorElements = await page
    .locator('[class*="error"], [role="alert"], .cl-formFieldErrorText')
    .all();
  console.log("Found error elements:", errorElements.length);

  for (let i = 0; i < errorElements.length; i++) {
    const error = errorElements[i];
    const text = await error.textContent();
    const isVisible = await error.isVisible();
    console.log(`Error ${i}: visible=${isVisible}, text="${text}"`);
  }

  // Check console errors
  const logs: string[] = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") {
      logs.push(msg.text());
    }
  });

  await page.waitForTimeout(1000);
  if (logs.length > 0) {
    console.log("Console errors:", logs);
  }
});
