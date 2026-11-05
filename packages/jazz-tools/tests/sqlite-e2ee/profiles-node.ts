import type { BrowserContext, Page } from "playwright";
import { evaluateHarnessOperation } from "../browser/evaluate-harness.mjs";
import type { ChatMessage, ProfileInfo, ProfileInput } from "./profile-harness.js";

export interface ProfileScenarioResult {
  profiles: ProfileInfo[];
  bobReopened: ProfileInfo;
  first: ChatMessage;
  reply: ChatMessage;
  bobBeforeGrant: unknown;
  bobAfterGrant: ChatMessage[];
  aliceSubscription: ChatMessage[];
  bobAfterReopen: ChatMessage[];
  malloryBeforeReply: unknown;
  malloryAfterReply: unknown;
}

const harnessModule = "/tests/sqlite-e2ee/profile-harness.ts";
const call = <T>(page: Page, method: string, args?: unknown) =>
  evaluateHarnessOperation<T>(page, harnessModule, method, args);

// Use the existing empty harness page. It only supports Vite's dynamic imports;
// account creation, crypto, worker startup and all database calls are production.
export async function profileScenario(
  context: BrowserContext,
  page: Page,
  input: Omit<ProfileInput, "label">,
): Promise<ProfileScenarioResult> {
  const browser = context.browser();
  if (!browser) throw new Error("Independent profiles require an attached browser");
  const harnessUrl = new URL("/tests/browser/remote-db-harness.html", page.url()).href;
  const contexts: BrowserContext[] = [];
  const pages: Page[] = [];
  const profiles: ProfileInfo[] = [];
  try {
    for (const label of ["Alice", "Bob", "Mallory"]) {
      const isolated = await browser.newContext();
      contexts.push(isolated);
      if (browser.browserType().name() === "webkit") {
        // Keep the private session open while its last application page closes.
        // This opaque-origin anchor owns neither the Jazz worker nor its keys.
        const anchor = await isolated.newPage();
        await anchor.goto("data:text/html,<title>Open private session</title>");
      }
      const isolatedPage = await isolated.newPage();
      pages.push(isolatedPage);
      await isolatedPage.goto(harnessUrl, { waitUntil: "domcontentloaded" });
      profiles.push(await call<ProfileInfo>(isolatedPage, "open", { ...input, label }));
    }
    const [alice, bob, mallory] = pages as [Page, Page, Page];
    const chat = await call<{ id: string }>(alice, "createChat");
    const first = await call<ChatMessage>(alice, "send", {
      chatId: chat.id,
      text: "Hello from Alice's profile",
    });
    const bobBeforeGrant = await call<unknown>(bob, "readRefusal", chat.id);
    await call(alice, "grant", { chatId: chat.id, accountId: profiles[1]!.accountId });
    const bobAfterGrant = await call<ChatMessage[]>(bob, "read", chat.id);
    const malloryBeforeReply = await call<unknown>(mallory, "readRefusal", chat.id);
    await call(alice, "watch", chat.id);
    await call(alice, "waitForMessage", first.id);
    const reply = await call<ChatMessage>(bob, "send", {
      chatId: chat.id,
      text: "Reply from Bob's private profile",
    });
    const aliceSubscription = await call<ChatMessage[]>(alice, "waitForMessage", reply.id);

    // Discard Bob's JS realm and every owning page. Retain only the browser
    // context's normal localStorage/IndexedDB; never transfer account secrets.
    await call(bob, "close");
    await bob.close();
    const reopenedPage = await contexts[1]!.newPage();
    pages[1] = reopenedPage;
    await reopenedPage.goto(harnessUrl, { waitUntil: "domcontentloaded" });
    const bobReopened = await call<ProfileInfo>(reopenedPage, "open", { ...input, label: "Bob" });
    const bobAfterReopen = await call<ChatMessage[]>(reopenedPage, "read", chat.id);
    const malloryAfterReply = await call<unknown>(mallory, "readRefusal", chat.id);
    return {
      profiles,
      bobReopened,
      first,
      reply,
      bobBeforeGrant,
      bobAfterGrant,
      aliceSubscription,
      bobAfterReopen,
      malloryBeforeReply,
      malloryAfterReply,
    };
  } finally {
    await Promise.allSettled(
      pages.map((p) => (p.isClosed() ? Promise.resolve() : call(p, "close"))),
    );
    await Promise.all(contexts.map((c) => c.close()));
  }
}
