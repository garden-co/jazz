import { afterEach, expect, it } from "vitest";
import { userEvent } from "vitest/browser";
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
let invitation:
  | {
      input: HTMLInputElement;
      form: HTMLFormElement;
      expectedValue: string;
      submitted: boolean;
      reachedRoot: boolean;
    }
  | undefined;
function failureDiagnostics() {
  // Emit only synthetic labels, counts and categories. Never include DOM text,
  // input contents, canonical authors, tokens, server URLs or raw errors.
  const previews = mounts.map(({ element, label }) => ({
    label,
    connected: element.isConnected,
    rooms: element.querySelectorAll("button.room").length,
    conversation: element.querySelector(".conversation") !== null,
    memberships: element.querySelectorAll("[aria-label='Room membership'] li").length,
    inviteInput: element.querySelector("input[aria-label='Invite account ID']") !== null,
    alerts: [...element.querySelectorAll("[role='alert']")].map((alert) => {
      const message = alert.textContent ?? "";
      if (/permission|unauthori[sz]ed|forbidden|denied/i.test(message)) return "permission";
      if (/connect|network|transport|socket/i.test(message)) return "connection";
      if (/storage|indexeddb|sqlite|quota/i.test(message)) return "storage";
      if (/query|subscription/i.test(message)) return "query";
      return "other";
    }),
  }));
  return JSON.stringify({
    previews,
    invitation: invitation
      ? {
          submitted: invitation.submitted,
          reachedRoot: invitation.reachedRoot,
          inputConnected: invitation.input.isConnected,
          formConnected: invitation.form.isConnected,
          inputMatchesExpected: invitation.input.value === invitation.expectedValue,
          inputCleared: invitation.input.value === "",
        }
      : null,
  });
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

async function mount(config: DbConfig | undefined = undefined, label: PreviewLabel = "local") {
  const selectedConfig = config ?? (await localPreviewConfig());
  const element = document.createElement("div");
  document.body.append(element);
  const root = createRoot(element);
  mounts.push({ root, element, label });
  await act(async () => {
    root.render(<BandChatPreview config={selectedConfig} />);
  });
  await waitFor(() => element.querySelector("#room-name") !== null, "room composer should render");
  return element;
}

afterEach(async () => {
  invitation = undefined;
  for (const { root, element } of mounts.splice(0)) {
    await act(async () => root.unmount());
    element.remove();
  }
});

it("negotiates persistent browser workers and renders the owner, guest-message, and removal flow", async () => {
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
  const ownerUserId = "browser-owner";
  const guestUserId = "browser-guest";
  const ownerToken = await getJazzServerJwtForUser(ownerUserId, undefined, server.appId);
  const guestToken = await getJazzServerJwtForUser(guestUserId, undefined, server.appId);
  const ownerAccount = await enrollTestAccount(server, ownerToken);
  const guestAccount = await enrollTestAccount(server, guestToken);
  const guestAuthor = guestAccount.id;
  const owner = await mount(
    {
      appId: server.appId,
      driver: { type: "persistent", dbName: `band-chat-owner-${crypto.randomUUID()}` },
      account: ownerAccount,
      serverUrl: server.serverUrl,
    },
    "owner",
  );
  await createRoom(owner, "Owner room");

  const guest = await mount(
    {
      appId: server.appId,
      driver: { type: "persistent", dbName: `band-chat-guest-${crypto.randomUUID()}` },
      account: guestAccount,
      serverUrl: server.serverUrl,
    },
    "guest",
  );
  await createRoom(guest, "Guest profile bootstrap");

  const invitee = currentInput(owner, "input[aria-label='Invite account ID']");
  await setInputValue(invitee, guestAuthor);
  // Creating the guest's bootstrap room can rerender the owner's membership
  // panel. Reacquire the controlled input after React commits the value so the
  // submit event reaches the currently connected form.
  const currentInvitee = currentInput(owner, "input[aria-label='Invite account ID']");
  const observation = {
    input: currentInvitee,
    form: currentInvitee.closest("form")!,
    expectedValue: guestAuthor,
    submitted: false,
    reachedRoot: false,
  };
  invitation = observation;
  owner.addEventListener(
    "submit",
    () => {
      observation.reachedRoot = true;
    },
    { once: true },
  );
  observation.submitted = true;
  await userEvent.click(currentInvitee.closest("form")!.querySelector("button[type='submit']")!);
  await waitFor(
    () => owner.textContent?.includes(guestAuthor) ?? false,
    "owner should render the invited guest",
    15_000,
  );
  await waitFor(
    () =>
      [...guest.querySelectorAll("button")].some((button) =>
        button.textContent?.includes("Owner room"),
      ),
    "guest should receive the invited room",
    15_000,
  );
  const ownerRoom = [...guest.querySelectorAll<HTMLButtonElement>("button")].find((button) =>
    button.textContent?.includes("Owner room"),
  )!;
  await act(async () => ownerRoom.click());
  const guestMessage = guest.querySelector<HTMLInputElement>("input[aria-label='Message']")!;
  await setInputValue(guestMessage, "Guest is on the setlist");
  await userEvent.click(guestMessage.closest("form")!.querySelector("button[type='submit']")!);
  await waitFor(
    () => owner.textContent?.includes("Guest is on the setlist") ?? false,
    "owner should receive the guest message",
    15_000,
  );

  const guestMembership = [...owner.querySelectorAll("li")].find((row) =>
    row.textContent?.includes(guestAuthor),
  )!;
  await act(async () => guestMembership.querySelector<HTMLButtonElement>("button")!.click());
  await waitFor(
    () => !owner.textContent?.includes(guestAuthor),
    "owner should render the guest removal",
    15_000,
  );
  // Revocation is an authority boundary, not a promise to erase rows already
  // retained in the guest's local-first store. The permission receipt proves
  // that a post-removal write is rejected at the serving authority.
});

async function enrollTestAccount(server: { appId: string; serverUrl: string }, token: string) {
  const accounts = await createAccountManager({ appId: server.appId, serverUrl: server.serverUrl });
  return accounts.registerJWT({ getToken: async () => token });
}

async function setInputValue(input: HTMLInputElement, value: string) {
  await userEvent.fill(input, value);
}

function currentInput(element: HTMLElement, selector: string): HTMLInputElement {
  const input = element.querySelector<HTMLInputElement>(selector);
  if (!input?.isConnected) {
    throw new Error(`expected a connected input for selector ${selector}`);
  }
  return input;
}

async function createRoom(element: HTMLDivElement, name: string) {
  const input = element.querySelector<HTMLInputElement>("#room-name")!;
  await setInputValue(input, name);
  await userEvent.click(input.closest("form")!.querySelector("button[type='submit']")!);
  await waitFor(
    () => element.textContent?.includes(`# ${name}`) ?? false,
    `${name} should be visible`,
    15_000,
  );
}

it("creates a local room, sends a message, and applies client-side picker validation", async () => {
  const element = await mount();
  const roomName = element.querySelector<HTMLInputElement>("#room-name")!;
  await setInputValue(roomName, "Soundcheck");
  await userEvent.click(element.querySelector("aside form button[type='submit']")!);
  await waitFor(
    () => element.textContent?.includes("# Soundcheck") ?? false,
    "room should be visible",
  );

  const invitee = element.querySelector<HTMLInputElement>("input[aria-label='Invite account ID']")!;
  const guestAccountId = crypto.randomUUID();
  await setInputValue(invitee, guestAccountId);
  await userEvent.click(invitee.closest("form")!.querySelector("button[type='submit']")!);
  await waitFor(
    () => element.textContent?.includes(guestAccountId) ?? false,
    "invited member should be visible",
  );
  const guestMembership = [...element.querySelectorAll("li")].find((row) =>
    row.textContent?.includes(guestAccountId),
  )!;
  await act(async () => guestMembership.querySelector<HTMLButtonElement>("button")!.click());
  await waitFor(
    () => !element.textContent?.includes(guestAccountId),
    "removed member should disappear",
  );

  const message = element.querySelector<HTMLInputElement>("input[aria-label='Message']")!;
  await setInputValue(message, "Amp warmed up");
  await userEvent.click(message.closest("form")!.querySelector("button[type='submit']")!);
  await waitFor(
    () => element.textContent?.includes("Amp warmed up") ?? false,
    "local message should render",
  );

  const attachment = element.querySelector<HTMLInputElement>("input[aria-label='Attachment']")!;
  Object.defineProperty(attachment, "files", {
    configurable: true,
    value: [new File([new Uint8Array(256 * 1024 + 1)], "too-big.png", { type: "image/png" })],
  });
  await act(async () => attachment.dispatchEvent(new Event("change", { bubbles: true })));
  expect(element.querySelector("[role='alert']")?.textContent).toContain(
    "256 KiB; this is client-side validation only",
  );
});
