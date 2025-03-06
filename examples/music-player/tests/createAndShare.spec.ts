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

test("create a new playlist and share", async ({
  page: marioPage,
  browser,
}) => {
  await marioPage.goto("/");

  const marioHome = new HomePage(marioPage);

  // The example song should be loaded
  await marioHome.expectMusicTrack("Example song");
  await marioHome.editTrackTitle("Example song", "Super Mario World");

  await marioHome.createPlaylist();
  await marioHome.editPlaylistTitle("Save the princess");

  await marioHome.navigateToPlaylist("All tracks");
  await marioHome.addTrackToPlaylist("Super Mario World", "Save the princess");

  await marioHome.navigateToPlaylist("Save the princess");
  await marioHome.expectMusicTrack("Super Mario World");

  await marioHome.signUp("Mario");

  const url = await marioHome.getShareLink();

  await sleep(4000); // Wait for the sync to complete

  const luigiContext = await browser.newContext();
  await mockAuthenticator(luigiContext);

  const luigiPage = await luigiContext.newPage();
  await luigiPage.goto("/");

  const luigiHome = new HomePage(luigiPage);

  await luigiHome.signUp("Luigi");

  await luigiPage.goto(url);

  await luigiHome.expectMusicTrack("Super Mario World");
  await luigiHome.playMusicTrack("Super Mario World");
  await luigiHome.expectActiveTrackPlaying();
});

test("create a new playlist, share, then remove track", async ({
  page: marioPage,
  browser,
}) => {
  // Create playlist with a song and share
  await marioPage.goto("/");
  const marioHome = new HomePage(marioPage);
  await marioHome.expectMusicTrack("Example song");
  await marioHome.editTrackTitle("Example song", "Super Mario World");
  await marioHome.createPlaylist();
  await marioHome.editPlaylistTitle("Save the princess");
  await marioHome.navigateToPlaylist("All tracks");
  await marioHome.addTrackToPlaylist("Super Mario World", "Save the princess");
  await marioHome.navigateToPlaylist("Save the princess");
  await marioHome.expectMusicTrack("Super Mario World");
  await marioHome.signUp("Mario");
  const url = await marioHome.getShareLink();

  await sleep(4000); // Wait for the sync to complete

  // Retrieve shared playlist
  const luigiContext = await browser.newContext();
  await mockAuthenticator(luigiContext);
  const luigiPage = await luigiContext.newPage();
  await luigiPage.goto("/");
  const luigiHome = new HomePage(luigiPage);
  await luigiHome.signUp("Luigi");
  await luigiPage.goto(url);
  await luigiHome.expectMusicTrack("Super Mario World");

  // Remove track from playlist
  await marioHome.navigateToHome();
  await marioHome.removeTrackFromPlaylist(
    "Super Mario World",
    "Save the princess",
  );
  await sleep(4000); // Wait for the sync to complete

  // Expect that the track is removed from the playlist
  await marioHome.navigateToPlaylist("Save the princess");
  await marioHome.notExpectMusicTrack("Super Mario World");
  await luigiHome.notExpectMusicTrack("Super Mario World");
});
