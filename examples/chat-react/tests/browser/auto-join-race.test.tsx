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
 * Test contract:
 *   A. The composer MUST transition through a disabled state before enabling
 *      (i.e. it must not start enabled immediately).
 *   B. Sending a message the moment the composer first enables must always
 *      deliver the message to other participants (no permission error).
 *
 * Adapted from Jazz 1 Playwright invite-auto-join.spec.ts to Jazz 2 vitest
 * browser tests.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { App } from "../../src/App.js";
import { TEST_SERVER_URL, APP_ID, testSecret } from "./test-constants.js";
import { resetProfileGuard } from "../../src/hooks/useMyProfile.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

async function waitFor(check: () => boolean, timeoutMs: number, message: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`Timeout: ${message}`);
}

interface EditorHandle {
  insertText: (text: string) => void;
  send: () => void;
}

function getEditorHandle(container: ParentNode): EditorHandle {
  const el = container.querySelector("#messageEditor") as
    | (HTMLElement & { __editorHandle?: EditorHandle })
    | null;
  if (!el?.__editorHandle) throw new Error("Editor handle not found on #messageEditor");
  return el.__editorHandle;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("auto-join race on first message send", () => {
  const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

  function makeMount(): { container: HTMLDivElement; root: Root } {
    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);
    mounts.push({ root, container });
    return { container, root };
  }

  async function mountApp(
    config: {
      appId: string;
      dbName: string;
      driver?: { type: "memory" };
      serverUrl: string;
      secret: string;
    },
    initialPath?: string,
  ): Promise<{ container: HTMLDivElement; root: Root }> {
    const mounted = makeMount();
    mounted.root.render(<App config={config} initialPath={initialPath} />);
    await waitFor(
      () => mounted.container.childNodes.length > 0,
      10_000,
      `App should commit initial DOM; hash=${window.location.hash}`,
    );
    return mounted;
  }

  async function unmountApp(container: HTMLDivElement): Promise<void> {
    const idx = mounts.findIndex((mount) => mount.container === container);
    if (idx === -1) return;
    const [{ root }] = mounts.splice(idx, 1);
    root.unmount();
    container.remove();
    await new Promise((r) => setTimeout(r, 200));
  }

  afterEach(async () => {
    resetProfileGuard();
    for (const { root, container } of mounts) {
      try {
        root.unmount();
      } catch {
        /* best effort */
      }
      container.remove();
    }
    mounts.length = 0;
    window.location.hash = "";
    // Allow JazzProvider's async shutdown (worker termination, OPFS lock
    // release) to complete before the next test starts.
    await window.__jazz?.shutdown();
  });

  it("composer is disabled until membership is confirmed, then message delivers", async () => {
    const serverUrl = TEST_SERVER_URL;
    const runId = Date.now();
    const bobMessage = `first-send-${runId}`;

    // ── Capture console errors for the duration of this test ────────────────
    const consoleErrors: string[] = [];
    const originalError = console.error;
    console.error = (...args: unknown[]) => {
      consoleErrors.push(args.map((a) => String(a)).join(" "));
      originalError(...args);
    };

    try {
      // ── Alice: create a public chat ────────────────────────────────────────
      const aliceConfig = {
        appId: APP_ID,
        dbName: uniqueDbName("autojoin-alice"),
        driver: { type: "memory" },
        serverUrl,
        secret: await testSecret(`autojoin-alice-${runId}`),
      };
      let { container: aliceContainer } = await mountApp(aliceConfig);

      // The app redirects from / to /#/chat/:id once it has created the seed
      // public chat.  Wait for the hash to settle.
      await waitFor(
        () => /\/#\/chat\//.test(window.location.href),
        20_000,
        `Alice should land on a chat URL after the seed chat is created; text=${aliceContainer.textContent?.slice(
          0,
          500,
        )}; consoleErrors=${JSON.stringify(consoleErrors)}`,
      );

      // Wait for Alice's editor to be enabled, so we know the chat is fully
      // ready and the chatId is stable.
      await waitFor(
        () => {
          const editor = aliceContainer.querySelector("#messageEditor .ProseMirror");
          return !!editor && editor.getAttribute("contenteditable") !== "false";
        },
        20_000,
        "Alice's composer should become enabled",
      );

      const aliceHash = window.location.hash;
      const match = aliceHash.match(/\/chat\/([^/]+)/);
      if (!match) throw new Error(`Could not extract chatId from hash: ${aliceHash}`);
      const chatId = match[1];

      await unmountApp(aliceContainer);

      // ── Bob: install observer BEFORE mount, then render at the chat URL ───
      //
      // The observer captures the editor's contenteditable history as it goes
      // through its initial mount and any subsequent transitions.  Installing
      // before render is essential — by the time render() returns, the editor
      // is mounted asynchronously after data loads, so a polling installer
      // catches the very first contenteditable value.
      const { container: bobContainer } = makeMount();

      const editorHistory: string[] = [];
      const poll = setInterval(() => {
        const pm = bobContainer.querySelector("#messageEditor .ProseMirror");
        if (!pm) return;
        clearInterval(poll);

        editorHistory.push(`initial:${pm.getAttribute("contenteditable") ?? "missing"}`);

        new MutationObserver((mutations) => {
          for (const m of mutations) {
            if (m.attributeName === "contenteditable") {
              const val = (m.target as Element).getAttribute("contenteditable") ?? "missing";
              editorHistory.push(`change:${val}`);
            }
          }
        }).observe(pm, { attributes: true });
      }, 5);

      const bobConfig = {
        appId: APP_ID,
        dbName: uniqueDbName("autojoin-bob"),
        driver: { type: "memory" },
        serverUrl,
        secret: await testSecret(`autojoin-bob-${runId}`),
      };
      mounts[mounts.length - 1]?.root.render(
        <App config={bobConfig} initialPath={`/chat/${chatId}`} />,
      );
      await waitFor(
        () => bobContainer.childNodes.length > 0,
        10_000,
        `Bob app should commit initial DOM; hash=${window.location.hash}; consoleErrors=${JSON.stringify(
          consoleErrors,
        )}`,
      );

      // ── Assert A: composer must transition through disabled before enabling ─
      //
      // With the fix, contenteditable starts as "false" and flips to "true"
      // after the chatMembers insert is server-acknowledged.
      //
      // With the buggy code, composerReady = !!userId && !!myProfile skips the
      // membership gate, so the editor starts as "true" directly.
      await waitFor(
        () => {
          const editor = bobContainer.querySelector("#messageEditor .ProseMirror");
          return !!editor && editor.getAttribute("contenteditable") !== "false";
        },
        20_000,
        `Bob's composer should eventually become enabled; hash=${window.location.hash}; text=${bobContainer.textContent?.slice(
          0,
          500,
        )}; editorHistory=${JSON.stringify(editorHistory)}; consoleErrors=${JSON.stringify(consoleErrors)}`,
      );

      clearInterval(poll); // belt-and-braces; the polling installer already cleared it

      const wasEverDisabled = editorHistory.some(
        (entry) => entry === "initial:false" || entry === "change:false",
      );
      expect(
        wasEverDisabled,
        `composer must have been disabled before enabling — history: ${JSON.stringify(editorHistory)}`,
      ).toBe(true);

      // ── Assert B: message sent the moment composer enables is delivered ────
      const bobEditor = getEditorHandle(bobContainer);
      bobEditor.insertText(bobMessage);
      bobEditor.send();

      ({ container: aliceContainer } = await mountApp(aliceConfig, `/chat/${chatId}`));

      await waitFor(
        () =>
          [...aliceContainer.querySelectorAll("article")].some((a) =>
            a.textContent?.includes(bobMessage),
          ),
        20_000,
        `Alice should receive Bob's message "${bobMessage}"; consoleErrors=${JSON.stringify(
          consoleErrors,
        )}; bobText=${bobContainer.textContent?.slice(0, 500)}`,
      );

      // ── Assert C: no permission errors during/after send ──────────────────
      await new Promise((r) => setTimeout(r, 1_000));
      const permissionErrors = consoleErrors.filter(
        (e) =>
          e.toLowerCase().includes("policy denied") ||
          e.toLowerCase().includes("writeerror") ||
          e.toLowerCase().includes("permission"),
      );
      expect(permissionErrors, "expected no permission errors during Bob's send").toHaveLength(0);
    } finally {
      console.error = originalError;
    }
  });
});
