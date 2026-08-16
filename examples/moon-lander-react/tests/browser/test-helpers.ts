/**
 * Shared test helpers for Moon Lander browser tests.
 *
 * Provides DOM interaction utilities, data-attribute readers, and mount
 * lifecycle helpers used across all phase test files.
 */

import { act } from "react";
import type { Root } from "react-dom/client";
import { waitForClientRegistryIdleForTest } from "jazz-tools/_dev/client-registry";

export type MountEntry = { root: Root; container: HTMLDivElement };

/** Unmount all tracked game instances and remove their containers. */
export async function unmountAll(mounts: MountEntry[]): Promise<void> {
  for (const { root, container } of mounts) {
    try {
      await act(async () => root.unmount());
    } catch {
      /* best effort */
    }
    container.remove();
  }
  mounts.length = 0;

  // Jazz clients are ref-counted through deferred-release layers so a same-tick
  // React remount can reuse them. Wait for the complete shutdown chain before a
  // test tears down its temporary server.
  await waitForClientRegistryIdleForTest();
}

/** Poll until a condition is true, or throw after timeout. */
export async function waitFor(
  check: () => boolean | Promise<boolean>,
  timeoutMs: number,
  message: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  const syncState = [...document.querySelectorAll<HTMLElement>('[data-testid="sync-debug"]')].map(
    (element) => ({
      settled: element.dataset.syncSettled,
      localRows: element.dataset.syncLocalRows,
      totalDeposits: element.dataset.syncTotalDeposits,
      uncollected: element.dataset.syncUncollected,
      playerX: element.querySelector<HTMLElement>('[data-testid="game-container"]')?.dataset
        .playerX,
      landerX: element.querySelector<HTMLElement>('[data-testid="game-container"]')?.dataset
        .landerX,
    }),
  );
  throw new Error(`Timeout: ${message}; sync=${JSON.stringify(syncState)}`);
}

/** Read a numeric data attribute from the game container. */
export function readNum(el: HTMLDivElement, attr: string): number {
  const container = el.querySelector('[data-testid="game-container"]')!;
  const raw = container.getAttribute(`data-${attr}`);
  if (raw === null) throw new Error(`Missing data attribute: data-${attr}`);
  return parseFloat(raw);
}

/** Read a string data attribute from the game container. */
export function readStr(el: HTMLDivElement, attr: string): string {
  const container = el.querySelector('[data-testid="game-container"]')!;
  const raw = container.getAttribute(`data-${attr}`);
  if (raw === null) throw new Error(`Missing data attribute: data-${attr}`);
  return raw;
}

/** Wait until a data attribute equals the expected value. */
export async function waitForAttr(
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

/** Simulate pressing a key (keydown). */
export function pressKey(key: string, code?: string) {
  document.dispatchEvent(new KeyboardEvent("keydown", { key, code: code ?? key, bubbles: true }));
}

/** Simulate releasing a key (keyup). */
export function releaseKey(key: string, code?: string) {
  document.dispatchEvent(new KeyboardEvent("keyup", { key, code: code ?? key, bubbles: true }));
}

/** Hold a key for a duration (ms), then release. */
export async function holdKey(key: string, durationMs: number, code?: string): Promise<void> {
  pressKey(key, code);
  await new Promise((r) => setTimeout(r, durationMs));
  releaseKey(key, code);
}

/** Wait for N animation frames to let the game loop process. */
export async function waitFrames(n: number): Promise<void> {
  for (let i = 0; i < n; i++) {
    await new Promise((r) => requestAnimationFrame(r));
  }
}
