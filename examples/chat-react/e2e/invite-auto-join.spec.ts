/**
 * Regression test for the auto-join race condition.
 *
 * ChatView performs a fire-and-forget db.insert(chatMembers) when a user lands
 * on a chat they can see but are not yet a member of.  The MessageComposer
 * enables as soon as userId + myProfile are available — independently of
 * membership — so the user can send a message before the server has
 * acknowledged the chatMembers insert.  The server then rejects the message
 * insert with a permissions error because the sender is not yet a member.
 *
 * Fix: gate the composer on membershipReady (true only after chatMembers insert
 * is acknowledged at edge-tier), not just userId + myProfile.
 *
 * Test contract: when Bob lands on a chat he is not yet a member of, the
 * composer MUST be disabled until auto-join completes server-side.  Sending
 * immediately after the composer first enables must always deliver the message.
 */
import { expect, test, type Page } from "@playwright/test";
import { newUserContext, createChatAndGetId, waitForComposer, waitForMessage } from "./helpers";

test.describe("auto-join race on first message send", () => {
  test("composer is disabled until membership is confirmed, then message delivers", async ({
    browser,
  }) => {
    const runId = Date.now();
    const bobMessage = `first-send-${runId}`;

    // ── Alice: create a public chat ───────────────────────────────────────────
    const { page: alice } = await newUserContext(browser, "alice");
    const chatId = await createChatAndGetId(alice);

    // ── Bob: open the chat directly (he is not yet a member) ─────────────────
    const { page: bob } = await newUserContext(browser, "bob");

    const consoleErrors: string[] = [];
    bob.on("console", (msg) => {
      if (msg.type() === "error") consoleErrors.push(msg.text());
    });

    await bob.goto(`http://127.0.0.1:5183/#/chat/${chatId}`);

    // ── Assert 1: composer must be initially disabled ─────────────────────────
    //
    // With the buggy code composerReady = !!userId && !!myProfile — both are
    // available almost immediately from local state — so the composer is enabled
    // before the chatMembers insert reaches the server.  The fix adds a
    // membershipReady gate, so the composer starts disabled.
    //
    // We check within a short window (500 ms) immediately after navigation.
    // If the composer is already enabled at this point, the test fails: the UI
    // is allowing sends before membership is confirmed.
    const composerEnabledImmediately = await isComposerEnabled(bob);
    expect(
      composerEnabledImmediately,
      "composer must not be enabled immediately on landing — it should wait for membership confirmation",
    ).toBe(false);

    // ── Assert 2: composer eventually becomes enabled ─────────────────────────
    //
    // After the chatMembers insert is acknowledged at the server, membership is
    // confirmed and the composer should unlock.
    await waitForComposer(bob, 20_000);

    // ── Assert 3: sending the first message succeeds ──────────────────────────
    //
    // Now that the composer is enabled (membership confirmed), a send must
    // succeed.  Bob's message must appear in Alice's tab.
    await sendImmediately(bob, bobMessage);
    await waitForMessage(alice, bobMessage, 20_000);

    // ── Assert 4: no permission errors ───────────────────────────────────────
    await bob.waitForTimeout(1_000);
    const permissionErrors = consoleErrors.filter(
      (e) =>
        e.toLowerCase().includes("policy denied") ||
        e.toLowerCase().includes("writeerror") ||
        e.toLowerCase().includes("permission"),
    );
    expect(permissionErrors, "expected no permission errors on Bob's page").toHaveLength(0);
  });
});

/**
 * Returns true if the ProseMirror editor is currently editable (not disabled).
 *
 * Waits for the editor element to mount first, then reads its initial
 * contenteditable state.  This avoids a false-negative when the editor hasn't
 * rendered yet.
 */
async function isComposerEnabled(page: Page): Promise<boolean> {
  const editorLocator = page.locator("#messageEditor .ProseMirror");
  // Wait for the element to be in the DOM before reading its attribute.
  await editorLocator.waitFor({ state: "attached", timeout: 10_000 }).catch(() => {});
  const attr = await editorLocator
    .getAttribute("contenteditable", { timeout: 1_000 })
    .catch(() => null);
  return attr === "true";
}

/**
 * Send a message without waiting for any network idle — as fast as possible.
 * Uses the __editorHandle exposed on the editor container.
 */
async function sendImmediately(page: Page, text: string): Promise<void> {
  await page.evaluate((msg) => {
    const el = document.getElementById("messageEditor") as
      | (HTMLElement & {
          __editorHandle?: { insertText: (t: string) => void; send: () => void };
        })
      | null;
    if (!el?.__editorHandle) throw new Error("Editor handle not found on #messageEditor");
    el.__editorHandle.insertText(msg);
    el.__editorHandle.send();
  }, text);
}
