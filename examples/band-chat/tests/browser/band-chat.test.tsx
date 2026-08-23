import { afterEach, describe, expect, inject, it } from "vitest";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { App } from "../../src/App.js";
import { createSmokeScenario } from "../../src/scenario.js";
import type { DbConfig } from "jazz-tools";
import { APP_ID } from "./test-constants.js";

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
async function mount(driver: DbConfig["driver"], serverUrl?: string) {
  const element = document.createElement("div");
  document.body.append(element);
  const root = createRoot(element);
  mounts.push({ root, element });
  await act(async () => {
    root.render(<App config={{ appId: APP_ID, secret, driver, serverUrl }} />);
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

  it("retains an offline local write while reconnecting to the deployed server", async () => {
    const serverUrl = inject("bandChatServerUrl");
    const dbName = `band-chat-offline-${crypto.randomUUID()}`;
    const offline = await mount({ type: "persistent", dbName }, "http://127.0.0.1:1");
    await act(async () => clickDemo(offline));
    await waitFor(
      () => offline.textContent?.includes("Soundcheck at 19:00") ?? false,
      "offline room should write locally",
    );
    const prior = mounts.pop()!;
    await act(async () => prior.root.unmount());
    prior.element.remove();
    const reconnected = await mount({ type: "persistent", dbName }, serverUrl);
    await waitFor(
      () => reconnected.textContent?.includes("Soundcheck at 19:00") ?? false,
      "reconnect should retain local room",
    );
  });
});
