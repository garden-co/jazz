import { afterEach, expect, it } from "vitest";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { DbConfig } from "jazz-tools";
import { deploy } from "../../../../../../packages/jazz-tools/src/dev/catalogue";
import {
  getJazzServerInfo,
  getJazzServerJwtForUser,
} from "../../../../../../packages/jazz-tools/tests/browser/testing-server";
import permissions from "../../permissions";
import { app } from "../../schema";
import { BandChatPreview } from "../../src/BandChat";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const mounts: Array<{ root: Root; element: HTMLDivElement }> = [];

async function waitFor(check: () => boolean, message: string, timeoutMs = 5_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await act(async () => await new Promise((resolve) => setTimeout(resolve, 30)));
  }
  throw new Error(message);
}

async function mount(
  config: DbConfig = {
    appId: "band-chat-browser-receipt",
    driver: { type: "memory" },
    secret: "jazz-auth-v1:Tb9eLjnS22z-_s9FK0EtiFIIRDe4EAygLAdni55RvAs",
  },
) {
  const element = document.createElement("div");
  document.body.append(element);
  const root = createRoot(element);
  mounts.push({ root, element });
  await act(async () => {
    root.render(<BandChatPreview config={config} />);
  });
  await waitFor(() => element.querySelector("#room-name") !== null, "room composer should render");
  return element;
}

afterEach(async () => {
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
  const guestTokenClaims = JSON.parse(atob(guestToken.split(".")[1]!)) as { iss: string };
  const guestAuthor = JSON.stringify([guestTokenClaims.iss, guestUserId]);
  const owner = await mount({
    appId: server.appId,
    driver: { type: "persistent", dbName: `band-chat-owner-${crypto.randomUUID()}` },
    jwtToken: ownerToken,
    serverUrl: server.serverUrl,
  });
  await createRoom(owner, "Owner room");

  const guest = await mount({
    appId: server.appId,
    driver: { type: "persistent", dbName: `band-chat-guest-${crypto.randomUUID()}` },
    jwtToken: guestToken,
    serverUrl: server.serverUrl,
  });
  await createRoom(guest, "Guest profile bootstrap");

  const invitee = owner.querySelector<HTMLInputElement>(
    "input[aria-label='Invite canonical author']",
  )!;
  await setInputValue(invitee, guestAuthor);
  await act(async () =>
    invitee
      .closest("form")!
      .dispatchEvent(new Event("submit", { bubbles: true, cancelable: true })),
  );
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
  await act(async () =>
    guestMessage
      .closest("form")!
      .dispatchEvent(new Event("submit", { bubbles: true, cancelable: true })),
  );
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

async function setInputValue(input: HTMLInputElement, value: string) {
  Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")!.set!.call(
    input,
    value,
  );
  await act(async () => input.dispatchEvent(new Event("input", { bubbles: true })));
}

async function createRoom(element: HTMLDivElement, name: string) {
  const input = element.querySelector<HTMLInputElement>("#room-name")!;
  await setInputValue(input, name);
  await act(async () =>
    input.closest("form")!.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true })),
  );
  await waitFor(
    () => element.textContent?.includes(`# ${name}`) ?? false,
    `${name} should be visible`,
    15_000,
  );
}

it("creates a local room, sends a message, and applies client-side picker validation", async () => {
  const element = await mount();
  const roomName = element.querySelector<HTMLInputElement>("#room-name")!;
  Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")!.set!.call(
    roomName,
    "Soundcheck",
  );
  await act(async () => {
    roomName.dispatchEvent(new Event("input", { bubbles: true }));
    element
      .querySelector("aside form")!
      .dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
  });
  await waitFor(
    () => element.textContent?.includes("# Soundcheck") ?? false,
    "room should be visible",
  );

  const invitee = element.querySelector<HTMLInputElement>(
    "input[aria-label='Invite canonical author']",
  )!;
  const guestAuthor = JSON.stringify(["https://guest.example", "guest-user"]);
  Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")!.set!.call(
    invitee,
    guestAuthor,
  );
  await act(async () => {
    invitee.dispatchEvent(new Event("input", { bubbles: true }));
    invitee
      .closest("form")!
      .dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
  });
  await waitFor(
    () => element.textContent?.includes(guestAuthor) ?? false,
    "invited member should be visible",
  );
  const guestMembership = [...element.querySelectorAll("li")].find((row) =>
    row.textContent?.includes(guestAuthor),
  )!;
  await act(async () => guestMembership.querySelector<HTMLButtonElement>("button")!.click());
  await waitFor(
    () => !element.textContent?.includes(guestAuthor),
    "removed member should disappear",
  );

  const message = element.querySelector<HTMLInputElement>("input[aria-label='Message']")!;
  Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")!.set!.call(
    message,
    "Amp warmed up",
  );
  await act(async () => {
    message.dispatchEvent(new Event("input", { bubbles: true }));
    message
      .closest("form")!
      .dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
  });
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
