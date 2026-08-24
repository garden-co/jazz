import { afterEach, describe, expect, it } from "vitest";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { App } from "../../src/App.js";
import { createSmokeScenario } from "../../src/scenario.js";
import { bandChatFixtureUsers } from "../../src/fixture.js";
import { createDb, type DbConfig } from "jazz-tools";
import { deploy } from "../../../../../../packages/jazz-tools/src/dev/catalogue.js";
import permissions from "../../permissions.js";
import { app } from "../../schema.js";
import {
  blockJazzServerNetwork,
  getJazzServerInfo,
  getJazzServerJwtForUser,
  unblockJazzServerNetwork,
} from "../../../../../../packages/jazz-tools/tests/browser/testing-server.js";
import { bandChatBrowserCommands } from "./browser-commands.js";

// A valid local-first identity seed; fixed so the persistence receipt reopens
// as the same member identity.
const secret = "Tb9eLjnS22z-_s9FK0EtiFIIRDe4EAygLAdni55RvAs";
const mounts: Array<{ root: Root; element: HTMLDivElement }> = [];

async function waitFor(check: () => boolean, message: string, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 50));
    });
  }
  throw new Error(message);
}
async function mount(driver: DbConfig["driver"], config: Partial<DbConfig> = {}) {
  const element = document.createElement("div");
  document.body.append(element);
  const root = createRoot(element);
  mounts.push({ root, element });
  const appConfig = (
    config.jwtToken
      ? { appId: "band-chat-local", driver, ...config }
      : { appId: "band-chat-local", secret, driver, ...config }
  ) as Partial<DbConfig>;
  await act(async () => {
    root.render(<App config={appConfig} />);
  });
  await waitFor(
    () => element.querySelector(".empty") !== null,
    "BandChat should render the demo state",
  );
  return element;
}
function clickDemo(element: HTMLDivElement) {
  element.querySelector<HTMLButtonElement>(".empty button")!.click();
}
function typeMessage(element: HTMLDivElement, value: string) {
  const input = element.querySelector<HTMLInputElement>("input[aria-label='Message']")!;
  Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")!.set!.call(
    input,
    value,
  );
  input.dispatchEvent(new Event("input", { bubbles: true }));
}
afterEach(async () => {
  for (const { root, element } of mounts.splice(0)) {
    await act(async () => root.unmount());
    element.remove();
  }
});

describe("BandChat browser smoke", () => {
  it("allows only the authenticated local-first subject to self-provision a profile", async () => {
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
    const local = await createDb({
      appId: server.appId,
      serverUrl: server.serverUrl,
      secret,
      driver: { type: "memory" },
    });
    try {
      const userId = local.getAuthState().session?.user_id;
      expect(userId).toBeTruthy();
      const profile = local.insert(app.profiles, {
        userId: userId!,
        displayName: "Local musician",
      });
      await profile.wait({ tier: "edge" });
      await expect(
        local
          .insert(app.profiles, { userId: "someone-else", displayName: "Forged" })
          .wait({ tier: "edge" }),
      ).rejects.toThrow(/permission_denied/i);
      await local
        .update(app.profiles, profile.value.id, { displayName: "Local musician renamed" })
        .wait({ tier: "edge" });
      await local.delete(app.profiles, profile.value.id).wait({ tier: "edge" });
    } finally {
      await local.shutdown();
    }
  });

  it("creates its synthetic member room and sends locally in memory", async () => {
    const scenario = createSmokeScenario();
    const element = await mount({ type: "memory" });
    await act(async () => clickDemo(element));
    await waitFor(
      () => element.textContent?.includes(scenario.assertion.visibleText) ?? false,
      "welcome message should appear",
    );
    await act(async () => {
      typeMessage(element, "Amp is warmed up");
      element
        .querySelector("form")!
        .dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    });
    await waitFor(
      () => element.textContent?.includes("Amp is warmed up") ?? false,
      "local message should appear",
    );
  });

  it("keeps the room and messages after a persistent browser reopen", async () => {
    const dbName = `band-chat-${crypto.randomUUID()}`;
    const first = await mount({ type: "persistent", dbName });
    await act(async () => clickDemo(first));
    await waitFor(
      () => first.textContent?.includes("Soundcheck at 19:00") ?? false,
      "first session should write room",
    );
    const prior = mounts.pop()!;
    await act(async () => prior.root.unmount());
    prior.element.remove();
    const reopened = await mount({ type: "persistent", dbName });
    await waitFor(
      () => reopened.textContent?.includes("Soundcheck at 19:00") ?? false,
      "persistent reopen should restore local room",
    );
    expect(reopened.textContent).toContain("Neon Soundcheck");
  });

  it("rejects oversized attachments and round-trips allowed bytes", async () => {
    const element = await mount({ type: "memory" });
    await act(async () => clickDemo(element));
    await waitFor(() => element.querySelector("form") !== null, "demo room should open");
    const attachment = element.querySelector<HTMLInputElement>("input[aria-label='Attachment']")!;
    const setFile = async (file: File) => {
      Object.defineProperty(attachment, "files", { configurable: true, value: [file] });
      await act(async () => attachment.dispatchEvent(new Event("change", { bubbles: true })));
    };
    await setFile(new File([new Uint8Array(256 * 1024 + 1)], "too-big.png", { type: "image/png" }));
    expect(element.querySelector("[role='alert']")?.textContent).toContain("256 KB");
    await setFile(new File([new Uint8Array([1, 2, 3])], "setlist.txt", { type: "text/plain" }));
    await act(async () =>
      element
        .querySelector("form")!
        .dispatchEvent(new Event("submit", { bubbles: true, cancelable: true })),
    );
    await waitFor(
      () => element.textContent?.includes("setlist.txt (3 bytes)") ?? false,
      "allowed attachment bytes should render after local write",
    );
  });

  it("delivers offline writes after reconnect to a fresh server-backed store", async () => {
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
    const jwtToken = await getJazzServerJwtForUser(
      bandChatFixtureUsers.owner,
      undefined,
      server.appId,
    );
    const dbName = `band-chat-offline-${crypto.randomUUID()}`;
    await bandChatBrowserCommands().jazzBandChatBootstrapProfile(
      server,
      bandChatFixtureUsers.owner,
      "Owner",
    );
    const primed = await mount(
      { type: "persistent", dbName },
      { appId: server.appId, jwtToken, serverUrl: server.serverUrl },
    );
    await waitFor(
      () => primed.textContent?.includes("Start the soundcheck") ?? false,
      "trusted profile bootstrap should reach the browser before it goes offline",
      10000,
    );
    const online = mounts.pop()!;
    await act(async () => online.root.unmount());
    online.element.remove();
    await blockJazzServerNetwork(server.serverUrl);
    const offline = await mount(
      { type: "persistent", dbName },
      { appId: server.appId, jwtToken, serverUrl: server.serverUrl },
    );
    await waitFor(
      () => offline.querySelector(".empty button") !== null,
      "cached trusted profile should materialize before offline provisioning",
    );
    await act(async () => clickDemo(offline));
    await waitFor(
      () => offline.textContent?.includes("Soundcheck at 19:00") ?? false,
      "offline room should write locally",
    );
    const prior = mounts.pop()!;
    await act(async () => prior.root.unmount());
    prior.element.remove();
    await unblockJazzServerNetwork(server.serverUrl);
    const reconnected = await mount(
      { type: "persistent", dbName },
      { appId: server.appId, jwtToken, serverUrl: server.serverUrl },
    );
    await waitFor(
      () => reconnected.textContent?.includes("Soundcheck at 19:00") ?? false,
      "reconnect should retain local room",
    );
    const freshStore = await mount(
      { type: "persistent", dbName: `band-chat-fresh-${crypto.randomUUID()}` },
      { appId: server.appId, jwtToken, serverUrl: server.serverUrl },
    );
    await waitFor(
      () => freshStore.textContent?.includes("Soundcheck at 19:00") ?? false,
      "fresh server-backed store should receive reconnected room",
      10000,
    );
  });
});
