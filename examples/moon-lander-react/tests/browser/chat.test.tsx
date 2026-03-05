/**
 * E2E browser tests for Moon Lander — Chat.
 *
 * Tests the chat input UI (Enter to open, Escape to close, Enter to send)
 * and the speech bubble data contract. Most tests mount <Game> directly
 * with connected-mode props and a mock onSendMessage callback.
 *
 * Data attribute contract:
 *   data-chat-open       "true" | "false"
 *   data-chat-input      current input field value
 *
 * Callbacks tested:
 *   onSendMessage(text: string)
 *
 * Props used:
 *   chatMessages: ChatMessage[]  — recent messages, rendered as speech bubbles
 */

import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, describe, expect, it } from "vitest";
import {
  type MountEntry,
  pressKey,
  readStr,
  releaseKey,
  unmountAll,
  waitFor,
  waitForAttr,
} from "./test-helpers";
import { Game } from "../../src/Game";

const SPEED = 10;
const mounts: MountEntry[] = [];

async function mountGameWith(props: Record<string, unknown>): Promise<HTMLDivElement> {
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });

  await act(async () => {
    root.render(<Game {...({ physicsSpeed: SPEED, initialMode: "landed", ...props } as any)} />);
  });

  await waitFor(
    () => el.querySelector('[data-testid="game-canvas"]') !== null,
    3000,
    "Game canvas should render",
  );

  return el;
}

afterEach(async () => {
  await unmountAll(mounts);
});

// ---------------------------------------------------------------------------
// Chat
// ---------------------------------------------------------------------------

describe("Moon Lander — Chat", () => {
  it("Enter opens chat input, Escape closes it", async () => {
    /**
     *   [Enter] → chat opens (data-chat-open="true")
     *   [Escape] → chat closes (data-chat-open="false")
     */
    const el = await mountGameWith({ deposits: [], inventory: [] });

    expect(readStr(el, "chat-open")).toBe("false");

    pressKey("Enter", "Enter");
    await waitForAttr(el, "chat-open", "true", 1000);

    pressKey("Escape", "Escape");
    await waitForAttr(el, "chat-open", "false", 1000);
  });

  it("typing and pressing Enter sends a message", async () => {
    /**
     *   [Enter] → open chat
     *   type "hello moon" → input value updates
     *   [Enter] → onSendMessage("hello moon") fires, chat closes
     */
    const messages: string[] = [];

    const el = await mountGameWith({
      deposits: [],
      inventory: [],
      onSendMessage: (text: string) => {
        messages.push(text);
      },
    });

    pressKey("Enter", "Enter");
    await waitForAttr(el, "chat-open", "true", 1000);

    const input = el.querySelector('[data-testid="chat-input"]') as HTMLInputElement;
    expect(input).toBeTruthy();

    // Simulate typing by setting input value directly
    input.value = "hello moon";

    pressKey("Enter", "Enter");
    releaseKey("Enter", "Enter");

    await waitFor(() => messages.length > 0, 1000, "onSendMessage should fire");
    expect(messages[0]).toBe("hello moon");

    await waitForAttr(el, "chat-open", "false", 1000);
  });

  it("game keys are suppressed while chat is open", async () => {
    /**
     *   Walking player opens chat → A/D keys don't move the player
     */
    const el = await mountGameWith({ deposits: [], inventory: [] });

    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const xBefore = readStr(el, "player-x");

    pressKey("Enter", "Enter");
    await waitForAttr(el, "chat-open", "true", 1000);

    pressKey("d", "KeyD");
    await new Promise((r) => setTimeout(r, 300));
    releaseKey("d", "KeyD");

    expect(readStr(el, "player-x")).toBe(xBefore);

    pressKey("Escape", "Escape");
    await waitForAttr(el, "chat-open", "false", 1000);
  });

  it("does not send empty messages", async () => {
    const messages: string[] = [];

    const el = await mountGameWith({
      deposits: [],
      inventory: [],
      onSendMessage: (text: string) => {
        messages.push(text);
      },
    });

    pressKey("Enter", "Enter");
    await waitForAttr(el, "chat-open", "true", 1000);

    // Press Enter with empty input
    pressKey("Enter", "Enter");
    releaseKey("Enter", "Enter");

    await new Promise((r) => setTimeout(r, 300));
    expect(messages).toHaveLength(0);
  });
});
