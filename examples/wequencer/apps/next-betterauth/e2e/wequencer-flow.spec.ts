import { expect, test, type BrowserContext, type Page } from "@playwright/test";

const TIMEOUT = 30_000;

type Credentials = { name: string; email: string; password: string };

const TRACKS = ["Kick", "Snare", "Closed hat", "Bass"];
const STEPS_PER_TRACK = 16;

function expectedPattern() {
  return TRACKS.flatMap((name, track) =>
    Array.from({ length: STEPS_PER_TRACK }, (_, step) => ({
      label: `${name}, step ${step + 1}`,
      // The three edits below start on disabled pads, so the final state is
      // fixed even though their delivery timing is not.
      pressed: step % (track + 2) === 0 || (step === 1 && track < 3),
    })),
  );
}

function expectedPatternAfterEditorBurst() {
  return expectedPattern().map((pad, index) => ({
    ...pad,
    // The editor changes the first eight pads in every lane. This is a fixed
    // 32-row fixture, so a dropped write cannot hide behind a row-count check.
    pressed: index % STEPS_PER_TRACK < 8 ? !pad.pressed : pad.pressed,
  }));
}

async function patternOn(page: Page) {
  return page.locator(".track-lane .pad").evaluateAll((pads) =>
    pads.map((pad) => ({
      label: pad.getAttribute("aria-label"),
      pressed: pad.getAttribute("aria-pressed") === "true",
    })),
  );
}

async function signUp(page: Page, credentials: Credentials) {
  await page.goto("http://localhost:3000/");
  await page.getByRole("button", { name: "Create an account" }).click();
  await page.getByLabel("Name").fill(credentials.name);
  await page.getByLabel("Email").fill(credentials.email);
  await page.getByLabel("Password").fill(credentials.password);
  await page.getByRole("button", { name: "Create account" }).click();
  await expect(page).toHaveURL("/dashboard", { timeout: TIMEOUT });
}

async function invite(page: Page, userId: string, role: "editor" | "viewer") {
  await page.getByLabel("Collaborator user ID").fill(userId);
  await page.getByLabel("Role").selectOption(role);
  await page.getByRole("button", { name: "Add collaborator" }).click();
}

async function makeClient(context: BrowserContext, credentials: Credentials) {
  const page = await context.newPage();
  await signUp(page, credentials);
  const memberId = await page.getByTestId("member-id").textContent();
  if (!memberId) throw new Error("signed-in member id was not rendered");
  return { page, memberId: memberId.replace("Your member ID: ", "") };
}

test("two clients converge ordered pads and transport after a bounded offline phase", async ({
  browser,
}) => {
  const run = Date.now();
  const ownerContext = await browser.newContext();
  const editorContext = await browser.newContext();
  try {
    const owner = await makeClient(ownerContext, {
      name: "Owner",
      email: `owner-${run}@example.com`,
      password: "testpassword",
    });
    const editor = await makeClient(editorContext, {
      name: "Editor",
      email: `editor-${run}@example.com`,
      password: "testpassword",
    });

    await owner.page.getByRole("button", { name: "Create a 4-track session" }).click();
    await expect(owner.page.getByRole("heading", { name: "Late-night rehearsal" })).toBeVisible({
      timeout: TIMEOUT,
    });
    await invite(owner.page, editor.memberId, "editor");
    await editor.page.reload();
    await expect(editor.page.getByRole("heading", { name: "Late-night rehearsal" })).toBeVisible({
      timeout: TIMEOUT,
    });

    // Phase 1: concurrent online writes to independent ordered pads.
    await Promise.all([
      owner.page.getByRole("button", { name: "Kick, step 2" }).click(),
      editor.page.getByRole("button", { name: "Snare, step 2" }).click(),
    ]);
    await expect(owner.page.getByRole("button", { name: "Snare, step 2" })).toHaveAttribute(
      "aria-pressed",
      "true",
      { timeout: TIMEOUT },
    );
    await expect(editor.page.getByRole("button", { name: "Kick, step 2" })).toHaveAttribute(
      "aria-pressed",
      "true",
      { timeout: TIMEOUT },
    );

    // Phase 2: a bounded partition. Only the disjoint editor pad is asserted;
    // same-field conflict resolution remains Jazz's documented merge behavior.
    await editorContext.setOffline(true);
    await editor.page.getByRole("button", { name: "Closed hat, step 2" }).click();
    await expect(editor.page.getByRole("button", { name: "Closed hat, step 2" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    await editorContext.setOffline(false);
    await expect(owner.page.getByRole("button", { name: "Closed hat, step 2" })).toHaveAttribute(
      "aria-pressed",
      "true",
      { timeout: TIMEOUT },
    );
    await expect.poll(() => patternOn(owner.page), { timeout: TIMEOUT }).toEqual(expectedPattern());
    await expect
      .poll(() => patternOn(editor.page), { timeout: TIMEOUT })
      .toEqual(expectedPattern());

    // Phase 3: playback is an ordinary transport receipt, visible through the
    // same ordered query on the second client.
    await owner.page.getByRole("button", { name: "Play" }).click();
    await expect(editor.page.getByRole("button", { name: "Pause" })).toBeVisible({
      timeout: TIMEOUT,
    });
  } finally {
    await ownerContext.close();
    await editorContext.close();
  }
});

test("viewer pad writes receive an authorization rejection receipt", async ({ browser }) => {
  const run = Date.now();
  const ownerContext = await browser.newContext();
  const viewerContext = await browser.newContext();
  try {
    const owner = await makeClient(ownerContext, {
      name: "Owner",
      email: `owner-viewer-${run}@example.com`,
      password: "testpassword",
    });
    const viewer = await makeClient(viewerContext, {
      name: "Viewer",
      email: `viewer-${run}@example.com`,
      password: "testpassword",
    });
    await owner.page.getByRole("button", { name: "Create a 4-track session" }).click();
    await expect(owner.page.getByRole("heading", { name: "Late-night rehearsal" })).toBeVisible({
      timeout: TIMEOUT,
    });
    await invite(owner.page, viewer.memberId, "viewer");
    await viewer.page.reload();
    await expect(viewer.page.getByRole("heading", { name: "Late-night rehearsal" })).toBeVisible({
      timeout: TIMEOUT,
    });
    await viewer.page.getByRole("button", { name: "Kick, step 2" }).click();
    await expect(viewer.page.getByRole("status")).toContainText(
      "Pad update was rejected by session permissions.",
      { timeout: TIMEOUT },
    );
    await expect(viewer.page.getByRole("button", { name: "Kick, step 2" })).toHaveAttribute(
      "aria-pressed",
      "false",
      { timeout: TIMEOUT },
    );
  } finally {
    await ownerContext.close();
    await viewerContext.close();
  }
});

test("editor edit burst preserves a readable pattern", async ({ browser }) => {
  const run = Date.now();
  const ownerContext = await browser.newContext();
  const editorContext = await browser.newContext();
  try {
    const owner = await makeClient(ownerContext, {
      name: "Owner",
      email: `owner-burst-${run}@example.com`,
      password: "testpassword",
    });
    const editor = await makeClient(editorContext, {
      name: "Editor",
      email: `editor-burst-${run}@example.com`,
      password: "testpassword",
    });
    await owner.page.getByRole("button", { name: "Create a 4-track session" }).click();
    await expect(owner.page.getByRole("heading", { name: "Late-night rehearsal" })).toBeVisible({
      timeout: TIMEOUT,
    });
    await invite(owner.page, editor.memberId, "editor");
    await editor.page.reload();
    await expect(editor.page.getByRole("heading", { name: "Late-night rehearsal" })).toBeVisible({
      timeout: TIMEOUT,
    });

    const edits = TRACKS.flatMap((name) =>
      Array.from({ length: 8 }, (_, step) =>
        editor.page.getByRole("button", { name: `${name}, step ${step + 1}` }),
      ),
    );
    await Promise.all(edits.map((pad) => pad.click()));

    const expected = expectedPatternAfterEditorBurst();
    await expect.poll(() => patternOn(editor.page), { timeout: TIMEOUT }).toEqual(expected);
    await expect.poll(() => patternOn(owner.page), { timeout: TIMEOUT }).toEqual(expected);
  } finally {
    await ownerContext.close();
    await editorContext.close();
  }
});
