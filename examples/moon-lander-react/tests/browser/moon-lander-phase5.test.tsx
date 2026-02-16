/**
 * E2E browser tests for Moon Lander — Phase 5: Chat.
 *
 * Tests the chat input UI (Enter to open, Escape to close, Enter to send)
 * and the speech bubble data contract. Most tests mount <Game> directly
 * with connected-mode props and a mock onSendMessage callback.
 *
 * Phase 5 data attribute contract (new additions):
 *   data-chat-open       "true" | "false"
 *   data-chat-input      current input field value
 *
 * New callback: onSendMessage(text: string)
 *   Fired when the player submits a chat message.
 *
 * New prop: chatMessages: ChatMessage[]
 *   Recent chat messages from Jazz, rendered as speech bubbles.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";
import { Game } from "../../src/Game.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const SPEED = 10;
const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

async function mountGameWith(props: Record<string, unknown>): Promise<HTMLDivElement> {
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });

  await act(async () => {
    root.render(<Game {...({ physicsSpeed: SPEED, ...props } as any)} />);
  });

  await waitFor(
    () => el.querySelector('[data-testid="game-canvas"]') !== null,
    3000,
    "Game canvas should render",
  );

  return el;
}

afterEach(async () => {
  for (const { root, container } of mounts) {
    try {
      await act(async () => root.unmount());
    } catch {
      /* best effort */
    }
    container.remove();
  }
  mounts.length = 0;
});

async function waitFor(
  check: () => boolean,
  timeoutMs: number,
  message: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`Timeout: ${message}`);
}

function readStr(el: HTMLDivElement, attr: string): string {
  const container = el.querySelector('[data-testid="game-container"]')!;
  const raw = container.getAttribute(`data-${attr}`);
  if (raw === null) throw new Error(`Missing data attribute: data-${attr}`);
  return raw;
}

async function waitForAttr(
  el: HTMLDivElement,
  attr: string,
  expected: string,
  timeoutMs = 5000,
): Promise<void> {
  const container = el.querySelector('[data-testid="game-container"]')!;
  await waitFor(
    () => container.getAttribute(`data-${attr}`) === expected,
    timeoutMs,
    `data-${attr} should become "${expected}" (got "${container.getAttribute(`data-${attr}`)}")`,
  );
}

function pressKey(key: string, code?: string) {
  document.dispatchEvent(
    new KeyboardEvent("keydown", { key, code: code ?? key, bubbles: true }),
  );
}

function releaseKey(key: string, code?: string) {
  document.dispatchEvent(
    new KeyboardEvent("keyup", { key, code: code ?? key, bubbles: true }),
  );
}

// ---------------------------------------------------------------------------
// Phase 5: Chat
// ---------------------------------------------------------------------------

describe("Moon Lander — Phase 5: Chat", () => {
  // =========================================================================
  // 1. Enter toggles chat input open/closed
  //
  //   [Enter] → chat opens (data-chat-open="true")
  //   [Escape] → chat closes (data-chat-open="false")
  // =========================================================================

  it("Enter opens chat input, Escape closes it", async () => {
    const el = await mountGameWith({ deposits: [], inventory: [] });

    // Land first
    await waitForAttr(el, "player-mode", "landed", 3000);

    // Chat should start closed
    expect(readStr(el, "chat-open")).toBe("false");

    // Press Enter → chat opens
    pressKey("Enter", "Enter");
    await waitForAttr(el, "chat-open", "true", 1000);

    // Press Escape → chat closes
    pressKey("Escape", "Escape");
    await waitForAttr(el, "chat-open", "false", 1000);
  });

  // =========================================================================
  // 2. Typing and pressing Enter sends a message
  //
  //   [Enter] → open chat
  //   type "hello" → input value updates
  //   [Enter] → onSendMessage("hello") fires, chat closes
  // =========================================================================

  it("typing and pressing Enter sends a message", async () => {
    const messages: string[] = [];

    const el = await mountGameWith({
      deposits: [],
      inventory: [],
      onSendMessage: (text: string) => {
        messages.push(text);
      },
    });

    await waitForAttr(el, "player-mode", "landed", 3000);

    // Open chat
    pressKey("Enter", "Enter");
    await waitForAttr(el, "chat-open", "true", 1000);

    // Find the input element and type into it
    const input = el.querySelector('[data-testid="chat-input"]') as HTMLInputElement;
    expect(input).toBeTruthy();

    // Simulate typing by setting input value directly
    input.value = "hello moon";

    // Submit by pressing Enter (document-level handler reads input value)
    pressKey("Enter", "Enter");
    releaseKey("Enter", "Enter");

    // onSendMessage should have fired
    await waitFor(
      () => messages.length > 0,
      1000,
      "onSendMessage should fire",
    );
    expect(messages[0]).toBe("hello moon");

    // Chat should close after sending
    await waitForAttr(el, "chat-open", "false", 1000);
  });

  // =========================================================================
  // 3. Game keys are suppressed while chat is open
  //
  //   Walking player opens chat → A/D keys don't move the player
  // =========================================================================

  it("game keys are suppressed while chat is open", async () => {
    const el = await mountGameWith({ deposits: [], inventory: [] });

    // Land and walk out
    await waitForAttr(el, "player-mode", "landed", 3000);
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Record position
    const xBefore = readStr(el, "player-x");

    // Open chat
    pressKey("Enter", "Enter");
    await waitForAttr(el, "chat-open", "true", 1000);

    // Press movement keys
    pressKey("d", "KeyD");
    await new Promise((r) => setTimeout(r, 300));
    releaseKey("d", "KeyD");

    // Position should not have changed
    const xAfter = readStr(el, "player-x");
    expect(xAfter).toBe(xBefore);

    // Close chat
    pressKey("Escape", "Escape");
    await waitForAttr(el, "chat-open", "false", 1000);
  });

  // =========================================================================
  // 4. Empty messages are not sent
  // =========================================================================

  it("does not send empty messages", async () => {
    const messages: string[] = [];

    const el = await mountGameWith({
      deposits: [],
      inventory: [],
      onSendMessage: (text: string) => {
        messages.push(text);
      },
    });

    await waitForAttr(el, "player-mode", "landed", 3000);

    // Open chat and immediately press Enter (empty input)
    pressKey("Enter", "Enter");
    await waitForAttr(el, "chat-open", "true", 1000);

    // Press Enter again with empty input → should not send
    pressKey("Enter", "Enter");
    releaseKey("Enter", "Enter");

    // Give time — no message should fire
    await new Promise((r) => setTimeout(r, 300));
    expect(messages).toHaveLength(0);
  });
});
