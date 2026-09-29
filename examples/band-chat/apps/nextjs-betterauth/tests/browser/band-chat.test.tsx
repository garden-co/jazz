import { afterEach, it } from "vitest";
import { page, userEvent } from "vitest/browser";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { createAccountManager, type AccountStore, type DbConfig } from "jazz-tools";
import { deploy } from "../../../../../../packages/jazz-tools/src/dev/catalogue";
import {
  getJazzServerInfo,
  getJazzServerJwtForUser,
} from "../../../../../../packages/jazz-tools/tests/browser/testing-server";
import permissions from "../../permissions";
import { app } from "../../schema";
import { BandChatPreview } from "../../src/BandChat";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

type PreviewLabel = "owner" | "guest" | "local";
const mounts: Array<{ root: Root; element: HTMLDivElement; label: PreviewLabel }> = [];

function failureDiagnostics() {
  // Emit only synthetic labels, counts and categories. Never include DOM text,
  // input contents, canonical authors, tokens, server URLs or raw errors.
  const previews = mounts.map(({ element, label }) => ({
    label,
    connected: element.isConnected,
    rooms: element.querySelectorAll("nav a, nav button").length,
    messages: element.querySelectorAll("[data-message-id]").length,
    dialogs: document.querySelectorAll("dialog[open]").length,
    alerts: [...element.querySelectorAll("[role='alert']")].map((alert) => {
      const message = alert.textContent ?? "";
      if (/permission|unauthori[sz]ed|forbidden|denied/i.test(message)) return "permission";
      if (/connect|network|transport|socket/i.test(message)) return "connection";
      if (/storage|indexeddb|sqlite|quota/i.test(message)) return "storage";
      if (/query|subscription/i.test(message)) return "query";
      return "other";
    }),
  }));
  return JSON.stringify({ previews });
}

async function waitFor(check: () => boolean, message: string, timeoutMs = 5_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await act(async () => await new Promise((resolve) => setTimeout(resolve, 30)));
  }
  throw new Error(`${message}; diagnostics=${failureDiagnostics()}`);
}

function inMemoryAccountStore(): AccountStore {
  let selected: string | null = null;
  return {
    async read() {
      return selected;
    },
    async update(transform) {
      selected = transform(selected);
    },
  };
}

async function localPreviewConfig(): Promise<DbConfig> {
  const appId = "band-chat-browser-receipt";
  const accounts = await createAccountManager({
    appId,
    serverUrl: "https://band-chat-preview.example",
    store: inMemoryAccountStore(),
  });
  return {
    appId,
    driver: { type: "memory" },
    account: accounts.createLocalFirst(),
  };
}

async function mount(
  config: DbConfig | undefined = undefined,
  label: PreviewLabel = "local",
  initialParams?: Record<string, string>,
) {
  const selectedConfig = config ?? (await localPreviewConfig());
  const element = document.createElement("div");
  element.dataset.testid = `preview-${label}`;
  document.body.append(element);
  const root = createRoot(element);
  mounts.push({ root, element, label });
  await act(async () => {
    root.render(<BandChatPreview config={selectedConfig} initialParams={initialParams} />);
  });
  return element;
}

afterEach(async () => {
  for (const { root, element } of mounts.splice(0)) {
    await act(async () => root.unmount());
    element.remove();
  }
});

function preview(element: HTMLDivElement) {
  return page.getByTestId(element.dataset.testid!);
}

function openDialog() {
  return page.getByRole("dialog");
}

function hasText(element: HTMLElement, text: string) {
  return element.textContent?.includes(text) ?? false;
}

async function setUpProfile(element: HTMLDivElement, name: string) {
  await waitFor(
    () => hasText(element, "Set up your profile"),
    "profile setup should render",
    15_000,
  );
  await preview(element).getByLabelText("Display name").fill(name);
  await preview(element).getByRole("button", { name: "Continue", exact: true }).click();
  await waitFor(() => hasText(element, name), "profile should be saved", 15_000);
}

async function createRoom(element: HTMLDivElement, name: string) {
  await preview(element).getByRole("button", { name: "New room", exact: true }).first().click();
  await openDialog().getByLabelText("Room name").fill(name);
  await openDialog().getByRole("button", { name: "Create room", exact: true }).click();
  await waitFor(
    () => element.querySelector("h2")?.textContent === name,
    `${name} should open`,
    15_000,
  );
}

async function sendMessage(element: HTMLDivElement, text: string) {
  await preview(element).getByRole("textbox", { name: "Message input" }).fill(text);
  await preview(element).getByRole("button", { name: "Send", exact: true }).click();
  await waitFor(() => hasText(element, text), "sent message should render", 15_000);
}

it("negotiates persistent browser workers and renders the owner, join-request, message, and removal flow", async () => {
  // Regression: a persistent browser worker creates its NativeRuntimeAdapter
  // around WasmDb, then uses that artifact's feature mask for the server Hello.
  // Removing WasmDb.wireFeatures makes this first remote worker connection fail
  // before either mounted user can complete the shared room flow.
  //
  // Each mounted preview also registers several local subscriptions
  // (rooms, profiles, messages, and members) while its persistent worker is
  // still opening.  Admission may hold *delivery* until storage opens, but it
  // must not serially defer native registrations: that ordering used to leave
  // the policy-maintained member view without a Stream B witness.
  const server = await getJazzServerInfo(
    `019d4a17-4591-7c0a-a320-${crypto.randomUUID().slice(0, 12)}`,
  );
  await deploy({
    appId: server.appId,
    serverUrl: server.serverUrl,
    adminSecret: server.adminSecret,
    schema: app,
    permissions,
  });
  const ownerToken = await getJazzServerJwtForUser("browser-owner", undefined, server.appId);
  const guestToken = await getJazzServerJwtForUser("browser-guest", undefined, server.appId);
  const ownerAccount = await enrollTestAccount(server, ownerToken);
  const guestAccount = await enrollTestAccount(server, guestToken);
  const owner = await mount(
    {
      appId: server.appId,
      driver: { type: "persistent", dbName: `band-chat-owner-${crypto.randomUUID()}` },
      account: ownerAccount,
      serverUrl: server.serverUrl,
    },
    "owner",
  );
  await setUpProfile(owner, "Olive Owner");
  await createRoom(owner, "Owner room");

  // The owner shares the room link from the invite dialog.
  await preview(owner).getByRole("button", { name: "Invite", exact: true }).click();
  const linkInput = openDialog().getByLabelText("Room link");
  await waitFor(() => (linkInput.element() as HTMLInputElement).value.includes("join="), "link");
  const roomId = new URL((linkInput.element() as HTMLInputElement).value).searchParams.get("join")!;
  // Dialogs are modal; close it so the second preview can be used.
  await userEvent.keyboard("{Escape}");
  await waitFor(() => document.querySelector("dialog[open]") === null, "invite dialog closes");

  const guest = await mount(
    {
      appId: server.appId,
      driver: { type: "persistent", dbName: `band-chat-guest-${crypto.randomUUID()}` },
      account: guestAccount,
      serverUrl: server.serverUrl,
    },
    "guest",
    { join: roomId },
  );
  await setUpProfile(guest, "Gus Guest");
  // The guest cannot read the room yet; asking to join is all it can do.
  await preview(guest).getByRole("button", { name: "Ask to join", exact: true }).click();
  await waitFor(
    () => hasText(guest, "Waiting for the room creator"),
    "guest should see its pending request",
    15_000,
  );

  await preview(owner)
    .getByRole("button", { name: /^Invite( \d+)?$/ })
    .click();
  await waitFor(
    () => hasText(openDialog().element() as HTMLElement, "Gus Guest"),
    "owner should see the join request with the guest's name",
    15_000,
  );
  await openDialog().getByRole("button", { name: "Admit", exact: true }).click();
  await waitFor(
    () => !hasText(openDialog().element() as HTMLElement, "Asking to join"),
    "the admitted request should clear",
    15_000,
  );
  await userEvent.keyboard("{Escape}");
  await waitFor(() => document.querySelector("dialog[open]") === null, "members dialog closes");

  await waitFor(
    () => guest.querySelector("h2")?.textContent === "Owner room",
    "guest should open the room once admitted",
    15_000,
  );
  await sendMessage(guest, "Guest is on the setlist");
  await waitFor(
    () => hasText(owner, "Guest is on the setlist"),
    "owner should receive the guest message",
    15_000,
  );

  await preview(owner)
    .getByRole("button", { name: /^Invite( \d+)?$/ })
    .click();
  await waitFor(() => document.querySelector("dialog[open]") !== null, "members dialog opens");
  const dialog = openDialog().element() as HTMLElement;
  const guestRow = [...dialog.querySelectorAll("li")].find((row) => hasText(row, "Gus Guest"))!;
  await act(async () =>
    [...guestRow.querySelectorAll("button")].find((button) => hasText(button, "Remove"))!.click(),
  );
  // Once removed, the guest moves from the member list to "People you know".
  await waitFor(
    () =>
      ![...(openDialog().element() as HTMLElement).querySelectorAll("li")].some(
        (row) => hasText(row, "Gus Guest") && hasText(row, "Remove"),
      ),
    "owner should render the guest removal",
    15_000,
  );
  // Revocation is an authority boundary, not a promise to erase rows already
  // retained in the guest's local-first store. The permission receipt proves
  // that a post-removal write is rejected at the serving authority.
}, 90_000);

async function enrollTestAccount(server: { appId: string; serverUrl: string }, token: string) {
  const accounts = await createAccountManager({ appId: server.appId, serverUrl: server.serverUrl });
  return accounts.registerJWT({ getToken: async () => token });
}

it("creates a local room, sends and reacts to a message, and applies client-side picker validation", async () => {
  const element = await mount();
  await setUpProfile(element, "Lou Local");
  await createRoom(element, "Soundcheck");

  await sendMessage(element, "Amp warmed up");
  await preview(element).getByRole("button", { name: "React", exact: true }).click();
  await page.getByRole("button", { name: "🔥", exact: true }).click();
  await waitFor(
    () =>
      [...element.querySelectorAll("button[aria-pressed='true']")].some((button) =>
        hasText(button, "🔥 1"),
      ),
    "own reaction should render as pressed",
  );

  const attachment = element.querySelector<HTMLInputElement>("input[aria-label='Attachment']")!;
  Object.defineProperty(attachment, "files", {
    configurable: true,
    value: [new File([new Uint8Array(10 * 1024 * 1024 + 1)], "too-big.png", { type: "image/png" })],
  });
  await act(async () => attachment.dispatchEvent(new Event("change", { bubbles: true })));
  await waitFor(
    () =>
      [...element.querySelectorAll("[role='alert']")].some((alert) =>
        hasText(alert as HTMLElement, "client-side validation only"),
      ),
    "oversized attachment should be rejected by the picker",
  );

  // An accepted file streams into its own message and renders as a download chip.
  Object.defineProperty(attachment, "files", {
    configurable: true,
    value: [new File(["Opening: Blue in Green"], "setlist.txt", { type: "text/plain" })],
  });
  await act(async () => attachment.dispatchEvent(new Event("change", { bubbles: true })));
  await preview(element).getByRole("button", { name: "Send", exact: true }).click();
  await waitFor(
    () =>
      [...element.querySelectorAll("[data-message-id]")].some((message) =>
        hasText(message as HTMLElement, "setlist.txt"),
      ),
    "sent attachment should render in the timeline",
    15_000,
  );

  // A sketch is posted as its own message with a shared drawing surface.
  await preview(element).getByRole("button", { name: "Sketch", exact: true }).click();
  await waitFor(
    () => element.querySelector("[data-message-id] svg.sketch-surface") !== null,
    "sketch should render in the timeline",
    15_000,
  );
});
