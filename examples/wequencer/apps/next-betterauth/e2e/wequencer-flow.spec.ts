import { expect, test, type BrowserContext, type Page } from "@playwright/test";
import { INSTRUMENTS, starterStep } from "../lib/instruments";

const TIMEOUT = 30_000;

type Credentials = { name: string; email: string; password: string };

// A new session defaults to eight tracks, one per instrument, of 16 steps.
const TRACKS = INSTRUMENTS;
const STEPS_PER_TRACK = 16;
// The three edits below start on disabled pads, so the final state is fixed
// even though their delivery timing is not.
const EDITED = ["Kick", "Snare", "Closed hat"];

function expectedPattern() {
  return TRACKS.flatMap(({ label, value }) =>
    Array.from({ length: STEPS_PER_TRACK }, (_, step) => ({
      label: `${label}, step ${step + 1}`,
      pressed: starterStep(value, step) || (step === 1 && EDITED.includes(label)),
    })),
  );
}

/** The starter groove every new session is created with, before any edit. */
function starterPattern() {
  return TRACKS.flatMap(({ label, value }) =>
    Array.from({ length: STEPS_PER_TRACK }, (_, step) => ({
      label: `${label}, step ${step + 1}`,
      pressed: starterStep(value, step),
    })),
  );
}

/** The burst test makes no other edits, so it starts from the starter groove. */
function expectedPatternAfterEditorBurst() {
  return starterPattern().map((pad, index) => ({
    ...pad,
    // The editor changes the first eight pads in every lane. This is a fixed
    // 64-row fixture, so a dropped write cannot hide behind a row-count check.
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

async function createSession(page: Page) {
  await page.getByRole("button", { name: "New session" }).first().click();
  await page.getByRole("button", { name: "Create session" }).click();
  await expect(page.getByRole("heading", { name: "Late-night rehearsal" })).toBeVisible({
    timeout: TIMEOUT,
  });
}

async function invite(page: Page, accountId: string, role: "editor" | "viewer") {
  await page.getByRole("button", { name: "Members" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("Collaborator account ID").fill(accountId);
  if (role === "viewer") {
    await dialog.getByRole("combobox", { name: "Role" }).click();
    await page.getByRole("option", { name: /Viewer/ }).click();
  }
  await dialog.getByRole("button", { name: "Add collaborator" }).click();
  // Exact: the member row's "Remove" button carries the same name in its tooltip.
  await expect(dialog.getByText(`Account ${accountId.slice(0, 8)}`, { exact: true })).toBeVisible({
    timeout: TIMEOUT,
  });
  await page.keyboard.press("Escape");
}

async function openSession(page: Page, via: "keyboard" | "mouse" = "keyboard") {
  await page.reload();
  if (via === "keyboard") {
    // ClickableCard's link is a visually hidden 1px element (the card surface
    // handles pointer clicks), so it never receives a pointer hit and a mouse
    // click on it cannot land. Activate it the way keyboard and screen reader
    // users do.
    await page.getByRole("link", { name: "Late-night rehearsal" }).press("Enter");
  } else {
    // A pointer user clicks the card surface, here its title.
    await page.getByRole("heading", { name: "Late-night rehearsal", level: 2 }).click();
  }
  await expect(page).toHaveURL(/\/dashboard\/[^/]+$/, { timeout: TIMEOUT });
  await expect(page.getByRole("heading", { name: "Late-night rehearsal" })).toBeVisible({
    timeout: TIMEOUT,
  });
}

async function makeClient(context: BrowserContext, credentials: Credentials) {
  const page = await context.newPage();
  await signUp(page, credentials);
  const memberId = await page.getByTestId("member-id").textContent({ timeout: TIMEOUT });
  if (!memberId) throw new Error("signed-in account id was not rendered");
  return { page, memberId: memberId.trim() };
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

    await createSession(owner.page);
    await invite(owner.page, editor.memberId, "editor");
    await openSession(editor.page);

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
    await expect(editor.page.getByRole("button", { name: "Stop" })).toBeVisible({
      timeout: TIMEOUT,
    });
    await expect(editor.page.getByTestId("transport-position")).toContainText("Step", {
      timeout: TIMEOUT,
    });
    await editor.page.getByRole("button", { name: "Stop" }).click();
    await expect(owner.page.getByRole("button", { name: "Play" })).toBeVisible({
      timeout: TIMEOUT,
    });
  } finally {
    await ownerContext.close();
    await editorContext.close();
  }
});

test("viewers see a read-only session", async ({ browser }) => {
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
    await createSession(owner.page);
    await invite(owner.page, viewer.memberId, "viewer");
    await openSession(viewer.page, "mouse");
    // The UI is read-only; the sync server rejecting a viewer's writes is
    // covered by the topology test's revoked-editor phase.
    await expect(viewer.page.getByText("You're viewing this session")).toBeVisible();
    await expect(viewer.page.getByRole("button", { name: "Kick, step 2" })).toBeDisabled();
    await expect(viewer.page.getByRole("button", { name: "Play" })).toBeDisabled();
    await owner.page.getByRole("button", { name: "Kick, step 2" }).click();
    await expect(viewer.page.getByRole("button", { name: "Kick, step 2" })).toHaveAttribute(
      "aria-pressed",
      "true",
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
    await createSession(owner.page);
    await invite(owner.page, editor.memberId, "editor");
    await openSession(editor.page);

    const edits = TRACKS.flatMap(({ label: name }) =>
      Array.from({ length: 8 }, (_, step) =>
        editor.page.getByRole("button", { name: `${name}, step ${step + 1}`, exact: true }),
      ),
    );
    // Parallel locator clicks share one mouse, so they land on each other's
    // pads. Wait until every pad can be toggled, then fire all 64 clicks at
    // once through the pads' own handlers.
    for (const pad of edits) await expect(pad).toBeEnabled({ timeout: TIMEOUT });
    await Promise.all(edits.map((pad) => pad.dispatchEvent("click")));

    const expected = expectedPatternAfterEditorBurst();
    await expect.poll(() => patternOn(editor.page), { timeout: TIMEOUT }).toEqual(expected);
    await expect.poll(() => patternOn(owner.page), { timeout: TIMEOUT }).toEqual(expected);
  } finally {
    await ownerContext.close();
    await editorContext.close();
  }
});
