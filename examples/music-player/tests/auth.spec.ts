import { BrowserContext, test } from "@playwright/test";
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

  await marioHome.signUp("Mario");

  await marioHome.logoutButton.waitFor({
    state: "visible",
  });

  await marioHome.logOut();
});
