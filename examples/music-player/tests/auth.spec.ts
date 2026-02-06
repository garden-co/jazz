import { type BrowserContext, test, expect } from "@playwright/test";
import { HomePage } from "./pages/HomePage";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function mockAuthenticator(context: BrowserContext) {
  await context.addInitScript(() => {
    Object.defineProperty(window.navigator, "credentials", {
      value: {
        ...window.navigator.credentials,
        create: async () => ({
          type: "public-key",
          id: new Uint8Array([1, 2, 3, 4]),
          rawId: new Uint8Array([1, 2, 3, 4]),
          response: {
            clientDataJSON: new Uint8Array([1]),
            attestationObject: new Uint8Array([2]),
          },
        }),
        get: async () => ({
          type: "public-key",
          id: new Uint8Array([1, 2, 3, 4]),
          rawId: new Uint8Array([1, 2, 3, 4]),
          response: {
            authenticatorData: new Uint8Array([1]),
            clientDataJSON: new Uint8Array([2]),
            signature: new Uint8Array([3]),
          },
        }),
      },
      configurable: true,
    });
  });
}

// Configure the authenticator
test.beforeEach(async ({ context }) => {
  // Enable virtual authenticator environment
  await mockAuthenticator(context);
});

test("sign up and log out", async ({ page: marioPage }) => {
  await marioPage.goto("/");

  const marioHome = new HomePage(marioPage);

  await marioHome.fillUsername("Mario");
  await marioPage.keyboard.press("Enter");

  await marioHome.signUp();

  await marioHome.logoutButton.waitFor({
    state: "visible",
  });

  await marioHome.logOut();
});

test("deleted account shows error in other logged-in session", async ({
  page: marioPage,
  browser,
}) => {
  // Mario enters and signs up
  await marioPage.goto("/");
  const marioHome = new HomePage(marioPage);

  await marioHome.fillUsername("Mario");
  await marioPage.keyboard.press("Enter");

  await marioHome.signUp();

  // Navigate to settings to get the passphrase
  await marioHome.navigateToSettings();

  const passphrase = await marioHome.getPassphrase();
  expect(passphrase).toBeTruthy();

  await sleep(4000); // Wait for the sync to complete

  // Create a new browser context and log in with the same passphrase
  const marioContext2 = await browser.newContext();
  await mockAuthenticator(marioContext2);
  const marioPage2 = await marioContext2.newPage();
  await marioPage2.goto("/");

  const marioHome2 = new HomePage(marioPage2);

  // Log in with the passphrase in the second context
  await marioHome2.loginWithPassphrase(passphrase);

  // Verify we're logged in by checking for the music track
  await marioHome2.expectMusicTrack("Example song");

  // Delete the account from the first context
  await marioHome.navigateToSettings();
  await marioHome.deleteAccount();

  await sleep(4000); // Wait for the sync to complete

  // The second context should now show the account deleted error screen
  await marioHome2.expectAccountDeletedScreen();
});
