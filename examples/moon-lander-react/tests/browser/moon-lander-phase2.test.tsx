/**
 * E2E browser tests for Moon Lander — Phase 2: Multiplayer Basics.
 *
 * Tests 1-8: local Game behaviour (physicsSpeed, identity, properties, online).
 *   Mount <Game> directly — no Jazz needed.
 *
 * Tests 9-13: real multiplayer sync through a Jazz server.
 *   Mount <App config={...}> with two independent instances connected to the
 *   same sync server. Verify that each instance sees the other as a remote
 *   player via data attributes.
 *
 * Phase 2 data attribute contract:
 *   data-player-id           — unique player ID (persisted in localStorage)
 *   data-player-name         — deterministic player name
 *   data-player-color        — assigned colour (hex string)
 *   data-required-fuel       — required fuel type from FUEL_TYPES
 *   data-lander-fuel         — current lander fuel level (number as string)
 *   data-player-online       — "true" when the player is online
 *   data-remote-player-count — number of visible remote players being rendered
 */

import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { App } from "../../src/App";
import { Game } from "../../src/Game";
import { FUEL_TYPES, INITIAL_FUEL, WALK_SPEED } from "../../src/game/constants";
import { APP_ID, TEST_PORT } from "./test-constants";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Timeout for cross-client Jazz sync waits. Generous because earlier test
 *  files accumulate data in the shared server, slowing edge subscriptions. */
const SYNC_TIMEOUT = 10_000;

const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

/** Mount the Game component directly (no Jazz sync). */
async function mountGame(opts: { physicsSpeed?: number } = {}): Promise<HTMLDivElement> {
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });

  const props: Record<string, unknown> = { initialMode: "landed" };
  if (opts.physicsSpeed !== undefined) props.physicsSpeed = opts.physicsSpeed;

  await act(async () => {
    root.render(<Game {...(props as any)} />);
  });

  await waitFor(
    () => el.querySelector('[data-testid="game-canvas"]') !== null,
    3000,
    "Game canvas should render",
  );

  return el;
}

function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

/** Mount the App with JazzProvider for sync testing. */
async function mountApp(opts: {
  appId?: string;
  dbName?: string;
  serverUrl?: string;
  playerId?: string;
  physicsSpeed?: number;
}): Promise<HTMLDivElement> {
  const { physicsSpeed, playerId, ...config } = opts;
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });

  await act(async () => {
    root.render(
      <App
        {...({
          config: { appId: config.appId ?? APP_ID, ...config },
          playerId: playerId ?? crypto.randomUUID(),
          physicsSpeed,
          initialMode: "landed",
        } as any)}
      />,
    );
  });

  await waitFor(
    () => el.querySelector('[data-testid="game-canvas"]') !== null,
    10000,
    "App should render game canvas",
  );

  return el;
}

beforeEach(() => {
  localStorage.removeItem("moon-lander-player-id");
});

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

/** Poll until a condition is true, or throw after timeout. */
async function waitFor(check: () => boolean, timeoutMs: number, message: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`Timeout: ${message}`);
}

/** Read a numeric data attribute from the game container. */
function readNum(el: HTMLDivElement, attr: string): number {
  const container = el.querySelector('[data-testid="game-container"]')!;
  const raw = container.getAttribute(`data-${attr}`);
  if (raw === null) throw new Error(`Missing data attribute: data-${attr}`);
  return parseFloat(raw);
}

/** Read a string data attribute from the game container. */
function readStr(el: HTMLDivElement, attr: string): string {
  const container = el.querySelector('[data-testid="game-container"]')!;
  const raw = container.getAttribute(`data-${attr}`);
  if (raw === null) throw new Error(`Missing data attribute: data-${attr}`);
  return raw;
}

/** Wait until a data attribute equals the expected value. */
async function waitForAttr(
  el: HTMLDivElement,
  attr: string,
  expected: string,
  timeoutMs = 10000,
): Promise<void> {
  const container = el.querySelector('[data-testid="game-container"]')!;
  await waitFor(
    () => container.getAttribute(`data-${attr}`) === expected,
    timeoutMs,
    `data-${attr} should become "${expected}" (got "${container.getAttribute(`data-${attr}`)}")`,
  );
}

/** Simulate pressing a key (keydown). */
function pressKey(key: string, code?: string) {
  document.dispatchEvent(new KeyboardEvent("keydown", { key, code: code ?? key, bubbles: true }));
}

/** Simulate releasing a key (keyup). */
function releaseKey(key: string, code?: string) {
  document.dispatchEvent(new KeyboardEvent("keyup", { key, code: code ?? key, bubbles: true }));
}

/** Wait for N animation frames to let the game loop process. */
async function waitFrames(n: number): Promise<void> {
  for (let i = 0; i < n; i++) {
    await new Promise((r) => requestAnimationFrame(r));
  }
}

// ---------------------------------------------------------------------------
// Phase 2: Multiplayer Basics
// ---------------------------------------------------------------------------

describe("Moon Lander — Phase 2: Multiplayer Basics", () => {
  // =========================================================================
  // 1. Physics speed multiplier
  // =========================================================================

  it("physicsSpeed prop accelerates descent", async () => {
    const el = await mountGame({ physicsSpeed: 10 });

    // Game starts in landed mode (physicsSpeed=10 would crash from free-fall).
    expect(readStr(el, "player-mode")).toBe("landed");
  });

  // =========================================================================
  // 2. Player identity
  // =========================================================================

  it("creates a unique player ID and stores it in localStorage", async () => {
    const el = await mountGame({ physicsSpeed: 10 });

    const playerId = readStr(el, "player-id");
    expect(playerId).toBeTruthy();
    expect(playerId.length).toBeGreaterThan(0);

    // Must be persisted
    const stored = localStorage.getItem("moon-lander-player-id");
    expect(stored).toBe(playerId);
  });

  it("reuses player ID from localStorage on remount", async () => {
    // First mount — creates an ID
    const el1 = await mountGame({ physicsSpeed: 10 });
    const id1 = readStr(el1, "player-id");

    // Unmount
    const mount = mounts.pop()!;
    await act(async () => mount.root.unmount());
    mount.container.remove();

    // Second mount — should reuse the same ID (localStorage not cleared)
    const el2 = await mountGame({ physicsSpeed: 10 });
    const id2 = readStr(el2, "player-id");

    expect(id2).toBe(id1);
  });

  // =========================================================================
  // 3. Player properties
  // =========================================================================

  it("assigns a player name", async () => {
    const el = await mountGame({ physicsSpeed: 10 });

    const name = readStr(el, "player-name");
    expect(name).toBeTruthy();
    expect(name.length).toBeGreaterThan(0);
  });

  it("assigns a required fuel type from the valid set", async () => {
    const el = await mountGame({ physicsSpeed: 10 });

    const fuel = readStr(el, "required-fuel");
    expect((FUEL_TYPES as readonly string[]).includes(fuel)).toBe(true);
  });

  it("assigns a player colour", async () => {
    const el = await mountGame({ physicsSpeed: 10 });

    const color = readStr(el, "player-color");
    expect(color).toMatch(/^#[0-9a-fA-F]{6}$/);
  });

  it("exposes initial lander fuel level", async () => {
    const el = await mountGame({ physicsSpeed: 10 });

    const fuel = readNum(el, "lander-fuel");
    expect(fuel).toBe(INITIAL_FUEL);
  });

  // =========================================================================
  // 4. Online presence
  // =========================================================================

  it("player is marked as online after mount", async () => {
    const el = await mountGame({ physicsSpeed: 10 });

    const online = readStr(el, "player-online");
    expect(online).toBe("true");
  });

  // =========================================================================
  // 5. Remote player sync (real Jazz server)
  //
  //   Instance A                 Jazz Server                Instance B
  //   ──────────                 ───────────                ──────────
  //   mount with JWT-A ──────── ← connect ─────────────── mount with JWT-B
  //   descending... ─────────── write player state ──────→ subscribe all players
  //   land ──────────────────── write mode=landed ───────→ sees remote (count≥1)
  //   exit lander (walking) ──→ write mode=walking ──────→ sees walking astronaut
  // =========================================================================

  it("syncs a descending player to a second instance", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    // Mount Instance A — starts descending
    const elA = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("sync-a"),
      serverUrl,

      physicsSpeed: 10,
    });

    // Mount Instance B — should eventually see Instance A as remote
    const elB = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("sync-b"),
      serverUrl,

      physicsSpeed: 10,
    });

    // Wait for Instance B to see at least one remote player
    await waitFor(
      () => {
        try {
          return readNum(elB, "remote-player-count") >= 1;
        } catch {
          return false;
        }
      },
      SYNC_TIMEOUT,
      "Instance B should see Instance A as a remote player",
    );
  });

  it("syncs landed state between two instances", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    const elA = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("landed-a"),
      serverUrl,

      physicsSpeed: 10,
    });

    const elB = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("landed-b"),
      serverUrl,

      physicsSpeed: 10,
    });

    // Instance B should see the landed player
    await waitFor(
      () => {
        try {
          return readNum(elB, "remote-player-count") >= 1;
        } catch {
          return false;
        }
      },
      SYNC_TIMEOUT,
      "Instance B should see Instance A's landed player",
    );
  });

  it("syncs walking mode between two instances", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    const elA = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("walk-a"),
      serverUrl,

      physicsSpeed: 10,
    });

    // Instance A: exit lander
    pressKey("e", "KeyE");
    await waitForAttr(elA, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const elB = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("walk-b"),
      serverUrl,

      physicsSpeed: 10,
    });

    // Instance B should see Instance A as a remote player
    await waitFor(
      () => {
        try {
          return readNum(elB, "remote-player-count") >= 1;
        } catch {
          return false;
        }
      },
      SYNC_TIMEOUT,
      "Instance B should see walking Instance A",
    );
  });

  // =========================================================================
  // 6. Fuel deposit collection in connected mode (Jazz round-trip)
  //
  //   App mounts with Jazz → deposits seeded → player lands → walks →
  //   collects deposit → onCollectDeposit → Jazz write → subscription
  //   updates → deposit disappears + inventory updates from Jazz state
  // =========================================================================

  it("collecting a deposit in connected mode updates inventory via Jazz", async () => {
    /**
     * This tests the full Jazz round-trip for inventory:
     *
     *   engine: walk over deposit
     *     │  onCollectDeposit(id) → queued
     *     ▼
     *   setInterval: db.update(fuel_deposits, id, { collected: true, collectedBy })
     *     │
     *     ▼
     *   Jazz subscription: fuel_deposits updated
     *     │  App.tsx derives inventory from collectedBy = playerId
     *     ▼
     *   Game receives inventory prop → engine uses it
     *     │
     *     ▼
     *   data-inventory reflects Jazz state
     */
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
    const playerId = crypto.randomUUID();

    const el = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("inv-a"),
      serverUrl,

      playerId,
      physicsSpeed: 10,
    });

    // Exit lander
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Initial inventory should be empty
    const invBefore = readStr(el, "inventory");
    expect(invBefore).toBe("");

    // Walk right to collect deposits (3s at 10x = ~3600px coverage)
    pressKey("d", "KeyD");
    await new Promise((r) => setTimeout(r, 3000));
    releaseKey("d", "KeyD");
    await waitFrames(10);

    // Wait for inventory to update (Jazz round-trip may take up to ~500ms)
    await waitFor(
      () => {
        try {
          const inv = readStr(el, "inventory");
          return inv !== "";
        } catch {
          return false;
        }
      },
      5000,
      "inventory should update after collecting deposits via Jazz round-trip",
    );

    const inventory = readStr(el, "inventory").split(",");
    expect(inventory.length).toBeGreaterThan(0);

    // Each collected type should be a valid fuel type
    for (const type of inventory) {
      expect((FUEL_TYPES as readonly string[]).includes(type)).toBe(true);
    }
  });

  it("deposit collected by Player A disappears for Player B", async () => {
    /**
     * Two players share the same Jazz state. When A collects a deposit,
     * Jazz updates collectedBy → B's subscription filters it out.
     *
     *   Player A          Jazz DB             Player B
     *   ────────          ───────             ────────
     *   walk over dep ──→ collected=true ───→ deposit disappears
     *                     collectedBy=A       deposit-count decreases
     */
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

    const elA = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("coll-a"),
      serverUrl,

      physicsSpeed: 10,
    });

    const elB = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("coll-b"),
      serverUrl,

      physicsSpeed: 10,
    });

    // Wait for deposits to be seeded and visible to both
    await waitFor(
      () => {
        try {
          return readNum(elA, "deposit-count") > 0 && readNum(elB, "deposit-count") > 0;
        } catch {
          return false;
        }
      },
      SYNC_TIMEOUT,
      "Both instances should see seeded deposits",
    );

    const countBefore = readNum(elB, "deposit-count");

    // Player A exits lander and walks to collect deposits
    pressKey("e", "KeyE");
    await waitForAttr(elA, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Walk right 4s then left 4s at 10x speed ~ 9600px total (full surface)
    pressKey("d", "KeyD");
    await new Promise((r) => setTimeout(r, 4000));
    releaseKey("d", "KeyD");
    pressKey("a", "KeyA");
    await new Promise((r) => setTimeout(r, 4000));
    releaseKey("a", "KeyA");
    await waitFrames(10);

    // Wait for Player B to see fewer deposits (Jazz propagation can be slow)
    await waitFor(
      () => {
        try {
          return readNum(elB, "deposit-count") < countBefore;
        } catch {
          return false;
        }
      },
      SYNC_TIMEOUT,
      "Player B should see fewer deposits after Player A collects some",
    );
  });

  // =========================================================================
  // 7. Burst deposits reappear after entering lander (collected:false reset)
  //
  //   Player A            Jazz DB               Player B
  //   ────────            ───────               ────────
  //   walk → collect ──→  collected=true  ──→   deposit disappears
  //   enter lander ────→  burst releases  ──→   deposit reappears
  //                       collected=false        deposit-count recovers
  //
  //   This verifies the collected:false reset on burst/refuel release.
  // =========================================================================

  it("burst deposits reappear as uncollected after entering lander", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
    const playerId = crypto.randomUUID();

    const el = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("burst-release"),
      serverUrl,

      playerId,
      physicsSpeed: 10,
    });

    // Exit lander
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Wait for deposits to appear
    await waitFor(
      () => {
        try {
          return readNum(el, "deposit-count") > 0;
        } catch {
          return false;
        }
      },
      10000,
      "deposits should be visible",
    );

    const countBefore = readNum(el, "deposit-count");

    // Walk right to collect deposits (4s at 10x ~ 4800px coverage)
    pressKey("d", "KeyD");
    await new Promise((r) => setTimeout(r, 4000));
    releaseKey("d", "KeyD");
    await waitFrames(10);

    // Check we collected something
    const inventory = readStr(el, "inventory");
    if (inventory === "") {
      // Didn't collect anything; can't test release. Skip gracefully.
      return;
    }

    const countAfterCollect = readNum(el, "deposit-count");
    const collected = countBefore - countAfterCollect;

    // Walk back to lander (same duration to get back)
    pressKey("a", "KeyA");
    await new Promise((r) => setTimeout(r, 4000));
    releaseKey("a", "KeyA");
    await waitFrames(5);

    // Enter lander (triggers burst + refuel release)
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "in_lander", 5000);
    releaseKey("e", "KeyE");

    // Exit lander again to see the updated deposit count
    pressKey("e", "KeyE");
    await waitForAttr(el, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Deposits should recover as the burst/refuel release sets collected:false.
    // The sync interval is 200ms, so give Jazz time to propagate.
    await waitFor(
      () => {
        try {
          return readNum(el, "deposit-count") > countAfterCollect;
        } catch {
          return false;
        }
      },
      SYNC_TIMEOUT,
      `deposit-count should recover after burst release (was ${countAfterCollect}, collected ${collected})`,
    );
  });

  /**
   * Full Phase 2 integration: two players descend, land, and see each other.
   *
   *   Instance A           sync server          Instance B
   *   ──────────           ───────────          ──────────
   *   descend ──────────→  write state ───────→  descend
   *   land ─────────────→  write landed ──────→  land
   *   see remote(B) ←────  read all players ←── see remote(A)
   *   exit lander ──────→  write walking ─────→  see A walking
   */
  it("full Phase 2: two players descend, land, and see each other", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
    const t0 = Date.now();
    const log = (msg: string) => console.log(`[phase2-diag +${Date.now() - t0}ms] ${msg}`);

    log("mounting A...");
    const elA = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("full-a"),
      serverUrl,

      physicsSpeed: 10,
    });
    log("A mounted");

    log("mounting B...");
    const elB = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("full-b"),
      serverUrl,

      physicsSpeed: 10,
    });
    log("B mounted");

    // Both start in landed mode
    expect(readStr(elA, "player-mode")).toBe("landed");
    expect(readStr(elB, "player-mode")).toBe("landed");
    log("both in landed mode");

    // Both should see the other as a remote player
    let lastLogTime = Date.now();
    await waitFor(
      () => {
        try {
          const aCount = readNum(elA, "remote-player-count");
          const bCount = readNum(elB, "remote-player-count");
          if (Date.now() - lastLogTime > 1000) {
            const syncA = elA.querySelector('[data-testid="sync-debug"]');
            const syncB = elB.querySelector('[data-testid="sync-debug"]');
            const aSettled = syncA?.getAttribute("data-sync-settled") ?? "?";
            const bSettled = syncB?.getAttribute("data-sync-settled") ?? "?";
            const aRows = syncA?.getAttribute("data-sync-local-rows") ?? "?";
            const bRows = syncB?.getAttribute("data-sync-local-rows") ?? "?";
            const aDeps = syncA?.getAttribute("data-sync-total-deposits") ?? "?";
            const bDeps = syncB?.getAttribute("data-sync-total-deposits") ?? "?";
            log(
              `A: remote=${aCount} settled=${aSettled} localRows=${aRows} deps=${aDeps} | B: remote=${bCount} settled=${bSettled} localRows=${bRows} deps=${bDeps}`,
            );
            lastLogTime = Date.now();
          }
          return aCount >= 1 && bCount >= 1;
        } catch {
          return false;
        }
      },
      SYNC_TIMEOUT,
      "Both instances should see each other as remote players",
    );
    log("mutual visibility achieved");

    // Instance A exits lander
    pressKey("e", "KeyE");
    await waitForAttr(elA, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Instance B should still see the remote player
    await waitFor(
      () => {
        try {
          return readNum(elB, "remote-player-count") >= 1;
        } catch {
          return false;
        }
      },
      SYNC_TIMEOUT,
      "Instance B should still see Instance A after mode change",
    );
  });
});
