import { test } from "@playwright/test";
import { HomePage } from "./pages/HomePage";

test("should sign up, sign in, and logout", async ({ page }) => {
  // Sign up
  await page.goto("/");
  const homePage = new HomePage(page);
  await homePage.expectLoggedOut();
  await homePage.signUp("UserA", "a@a.com", "12345678");
  await homePage.expectLoggedIn("UserA");

  // Log out & sign in
  await homePage.logout();
  await homePage.expectLoggedOut();
  await homePage.signIn("a@a.com", "12345678");
  await homePage.expectLoggedIn("UserA");

  // Logout
  await homePage.logout();
  await homePage.expectLoggedOut();
});
