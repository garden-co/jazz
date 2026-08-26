import { afterEach, expect, it } from "vitest";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { BandChatPreview } from "../../src/BandChat";

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const mounts: Array<{ root: Root; element: HTMLDivElement }> = [];

async function waitFor(check: () => boolean, message: string) {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    if (check()) return;
    await act(async () => await new Promise((resolve) => setTimeout(resolve, 30)));
  }
  throw new Error(message);
}

async function mount() {
  const element = document.createElement("div");
  document.body.append(element);
  const root = createRoot(element);
  mounts.push({ root, element });
  await act(async () => {
    root.render(
      <BandChatPreview
        config={{
          appId: "band-chat-browser-receipt",
          driver: { type: "memory" },
          secret: "Tb9eLjnS22z-_s9FK0EtiFIIRDe4EAygLAdni55RvAs",
        }}
      />,
    );
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

  const message = element.querySelector<HTMLInputElement>("input[aria-label='Message']")!;
  Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")!.set!.call(
    message,
    "Amp warmed up",
  );
  await act(async () => {
    message.dispatchEvent(new Event("input", { bubbles: true }));
    element
      .querySelector(".conversation form")!
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
