import type { BrowserContext, Page } from "playwright";
import { evaluateHarnessOperation } from "../browser/evaluate-harness.mjs";
import type { QuickstartInput } from "./quickstart-harness.js";

const harnessModule = "/tests/sqlite-e2ee/quickstart-harness.tsx";
const call = <T>(page: Page, method: string, args?: unknown) =>
  evaluateHarnessOperation<T>(page, harnessModule, method, args);

export async function quickstartScenario(
  context: BrowserContext,
  page: Page,
  input: QuickstartInput,
  reloadCycles = 0,
) {
  const browser = context.browser();
  if (!browser) throw new Error("Quickstart profiles require an attached browser");
  const harnessUrl = new URL("/tests/browser/remote-db-harness.html", page.url()).href;
  const contexts: BrowserContext[] = [];
  const pages: Page[] = [];
  const browserErrors: string[] = [];
  async function open(p: Page, url: string) {
    await p.goto(url, { waitUntil: "domcontentloaded" });
    await call(p, "mount", input);
    await p.getByRole("button", { name: "New chat", exact: true }).waitFor();
    await p
      .locator("main code")
      .filter({ hasText: /[0-9a-f-]{36}/ })
      .waitFor();
    return (await p.locator("main code").innerText()).trim();
  }
  async function send(p: Page, text: string) {
    await p.getByLabel("Message", { exact: true }).fill(text);
    await p.getByRole("button", { name: "Send", exact: true }).click();
    await p.waitForFunction(() => {
      const input = document.querySelector<HTMLInputElement>("form input");
      return input?.value === "";
    });
    await p.getByRole("listitem").filter({ hasText: text }).waitFor();
  }
  async function messages(p: Page, expected: string[]): Promise<string[]> {
    // Capture the qualifying UI state in the same browser evaluation. A
    // pending permission rollback may briefly restart the live subscription.
    const snapshot = await p.waitForFunction((expected) => {
      const rows = Array.from(
        document.querySelectorAll("main li"),
        (item) => item.textContent ?? "",
      );
      return rows.length === expected.length &&
        rows.every((text, index) => text === expected[index])
        ? rows
        : false;
    }, expected);
    try {
      const rows = await snapshot.jsonValue();
      if (!Array.isArray(rows)) throw new Error("Quickstart message snapshot did not settle");
      return rows;
    } finally {
      await snapshot.dispose();
    }
  }
  try {
    const accounts: string[] = [];
    for (let index = 0; index < 3; index++) {
      const isolated = await browser.newContext();
      isolated.on("console", (message) => {
        if (message.type() === "error" || message.type() === "warning")
          browserErrors.push(message.text());
      });
      isolated.setDefaultTimeout(15_000);
      contexts.push(isolated);
      const p = await isolated.newPage();
      pages.push(p);
      p.on("pageerror", (error) => browserErrors.push(error.message));
      accounts.push(await open(p, harnessUrl));
    }
    const [alice, bob, mallory] = pages as [Page, Page, Page];
    await alice.getByRole("button", { name: "New chat", exact: true }).click();
    await alice.getByRole("link").waitFor();
    const chatUrl = await alice.getByRole("link").getAttribute("href");
    const chatId = new URL(chatUrl!).searchParams.get("chat");
    if (!chatId) throw new Error("Quickstart did not publish a chat link");
    const first = "Hello from the React quickstart";
    const reply = "Reply through the React hooks";
    await send(alice, first);
    await open(mallory, chatUrl!);
    const malloryBeforeGrant = await call<unknown>(mallory, "readRemote", chatId);

    await alice.getByLabel("Recipient account ID", { exact: true }).fill(accounts[1]!);
    await alice.getByRole("button", { name: "Share chat", exact: true }).click();
    await alice.getByRole("status").filter({ hasText: "Shared." }).waitFor();
    const bobOpened = await open(bob, chatUrl!);
    await bob.getByRole("listitem").filter({ hasText: first }).waitFor();
    const bobShareControls = await bob.getByRole("button", { name: "Share chat" }).count();
    await send(bob, reply);
    await alice.getByRole("listitem").filter({ hasText: reply }).waitFor();
    const unauthorizedGrant = await call<unknown>(bob, "tryGrant", {
      chatId,
      accountId: accounts[2],
    });
    const malloryAfterReply = await call<unknown>(mallory, "readRemote", chatId);
    const malloryMessages = await mallory.getByRole("listitem").allTextContents();
    const [aliceMessages, bobMessages] = await Promise.all([
      messages(alice, [first, reply]),
      messages(bob, [first, reply]),
    ]);
    const bobReopened = await open(bob, chatUrl!);
    await bob.getByRole("listitem").filter({ hasText: reply }).waitFor();
    const bobRestoredMessages = await messages(bob, [first, reply]);
    const aliceReopened = await open(alice, chatUrl!);
    await alice.getByRole("listitem").filter({ hasText: reply }).waitFor();
    const aliceRestoredMessages = await messages(alice, [first, reply]);
    const reloads: Array<Array<{ account: string; messages: string[] }>> = [];
    for (let cycle = 0; cycle < reloadCycles; cycle++) {
      // Reload both independent owners without draining their runtimes first:
      // browser eviction must preserve each account and its committed messages.
      reloads.push(
        await Promise.all(
          [alice, bob].map(async (p) => ({
            account: await open(p, chatUrl!),
            messages: await messages(p, [first, reply]),
          })),
        ),
      );
    }
    const alerts = await Promise.all(pages.map((p) => p.getByRole("alert").allTextContents()));
    return {
      accounts,
      bobOpened,
      bobReopened,
      aliceReopened,
      first,
      reply,
      aliceMessages,
      bobMessages,
      aliceRestoredMessages,
      bobRestoredMessages,
      bobShareControls,
      unauthorizedGrant,
      malloryBeforeGrant,
      malloryAfterReply,
      malloryMessages,
      alerts,
      browserErrors,
      reloads,
    };
  } catch (error) {
    const screens = await Promise.all(
      pages.map((p) =>
        p
          .locator("body")
          .innerText()
          .catch(() => "closed"),
      ),
    );
    throw new Error(
      `${String(error)}\nBrowser errors: ${JSON.stringify(browserErrors)}\nPages: ${JSON.stringify(screens)}`,
      { cause: error },
    );
  } finally {
    await Promise.allSettled(pages.map((p) => call(p, "unmount")));
    await Promise.all(contexts.map((c) => c.close()));
  }
}

export type QuickstartResult = Awaited<ReturnType<typeof quickstartScenario>>;
