import { randomBytes } from "node:crypto";
import { test } from "@playwright/test";
import { HomePage } from "./pages/HomePage";

test("should sign up, sign in, and logout", async ({ page }) => {
  const username = randomBytes(4).toString("hex");
  const email = `${username}@example.com`;
  const password = randomBytes(8).toString("hex");

  // Sign up
  await page.goto("/");
  const homePage = new HomePage(page);
  await homePage.expectLoggedOut();
  await homePage.signUpLink.click();
  await homePage.signUpEmail(username, email, password);
  await homePage.expectLoggedIn(username);

  // Log out & sign in
  await homePage.logout();
  await homePage.expectLoggedOut();
  await homePage.signInLink.click();
  await homePage.signInEmail(email, password);
  await homePage.expectLoggedIn(username);

  // Logout
  await homePage.logout();
  await homePage.expectLoggedOut();
});
