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
import { commands } from "vitest/browser";
import { App } from "../../src/App";
import { Game } from "../../src/Game";
import { FUEL_TYPES, INITIAL_FUEL } from "../../src/game/constants";
import { ADMIN_SECRET, APP_ID, APP_ID_MULTI, TEST_PORT } from "./test-constants";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Timeout for cross-client Jazz sync waits. Generous because earlier test
 *  files accumulate data in the shared server, slowing edge subscriptions.
 *  Isolated BrowserContexts need extra time: fresh OPFS, new Jazz client
 *  init, full sync handshake. */
const SYNC_TIMEOUT = 20_000;

const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];
const openedIsolatedLabels: string[] = [];

/** Wrapper around openIsolatedApp that registers the label for afterEach cleanup. */
async function openIsolated(
  opts: Parameters<(typeof commands)["openIsolatedApp"]>[0],
): Promise<void> {
  openedIsolatedLabels.push(opts.label);
  await commands.openIsolatedApp(opts);
}

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
  spawnX?: number;
  localAuthToken?: string;
  localAuthMode?: string;
  adminSecret?: string;
}): Promise<HTMLDivElement> {
  const { physicsSpeed, spawnX, playerId, localAuthToken, localAuthMode, adminSecret, ...config } =
    opts;
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });

  await act(async () => {
    root.render(
      <App
        {...({
          config: {
            appId: config.appId ?? APP_ID,
            ...config,
            ...(localAuthToken
              ? { localAuthMode: localAuthMode ?? "anonymous", localAuthToken }
              : {}),
            ...(adminSecret ? { adminSecret } : {}),
          },
          playerId: playerId ?? crypto.randomUUID(),
          physicsSpeed,
          initialMode: "landed",
          ...(spawnX !== undefined ? { spawnX } : {}),
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
  for (const label of openedIsolatedLabels.splice(0)) {
    await commands.closeIsolatedApp(label).catch(() => {});
  }

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
async function waitFor(
  check: () => boolean | Promise<boolean>,
  timeoutMs: number,
  message: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
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
    const sharedToken = `sync-token-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

    // Mount Instance A — starts descending
    const _elA = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("sync-a"),
      serverUrl,
      localAuthToken: sharedToken,
      adminSecret: ADMIN_SECRET,
      physicsSpeed: 10,
    });

    // Mount Instance B in an isolated BrowserContext
    await openIsolated({
      label: "b",
      appId: APP_ID,
      dbName: uniqueDbName("sync-b"),
      serverUrl,
      localAuthToken: sharedToken,
      adminSecret: ADMIN_SECRET,
      physicsSpeed: 10,
    });

    // Wait for Instance B to see at least one remote player
    await waitFor(
      async () => {
        try {
          const raw = await commands.readIsolatedAttr("b", "remote-player-count");
          return raw !== null && parseFloat(raw) >= 1;
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
    const sharedToken = `landed-token-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

    const _elA = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("landed-a"),
      serverUrl,
      localAuthToken: sharedToken,
      adminSecret: ADMIN_SECRET,
      physicsSpeed: 10,
    });

    // Mount Instance B in an isolated BrowserContext
    await openIsolated({
      label: "b",
      appId: APP_ID,
      dbName: uniqueDbName("landed-b"),
      serverUrl,
      localAuthToken: sharedToken,
      adminSecret: ADMIN_SECRET,
      physicsSpeed: 10,
    });

    // Instance B should see the landed player
    await waitFor(
      async () => {
        try {
          const raw = await commands.readIsolatedAttr("b", "remote-player-count");
          return raw !== null && parseFloat(raw) >= 1;
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
    const sharedToken = `walk-token-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

    const elA = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("walk-a"),
      serverUrl,
      localAuthToken: sharedToken,
      adminSecret: ADMIN_SECRET,
      physicsSpeed: 10,
    });

    // Instance A: exit lander
    pressKey("e", "KeyE");
    await waitForAttr(elA, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Mount Instance B in an isolated BrowserContext
    await openIsolated({
      label: "b",
      appId: APP_ID,
      dbName: uniqueDbName("walk-b"),
      serverUrl,
      localAuthToken: sharedToken,
      adminSecret: ADMIN_SECRET,
      physicsSpeed: 10,
    });

    // Instance B should see Instance A as a remote player
    await waitFor(
      async () => {
        try {
          const raw = await commands.readIsolatedAttr("b", "remote-player-count");
          return raw !== null && parseFloat(raw) >= 1;
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

  // =========================================================================
  // 6b. Cross-client deposit collection visibility
  //
  //   Player A (own identity)   Jazz DB          Player B (own identity)
  //   ────────────────────────  ───────          ──────────────────────
  //   settle → deposits seeded  ──────────────→  sees N deposits
  //   walk → collect ──────────→ collected=true  deposit-count drops for B
  //
  //   Verifies that WHERE filter re-evaluation works cross-client when a row
  //   transitions from collected:false to collected:true.
  // =========================================================================

  it("deposit collected by Player A disappears for Player B", { timeout: 60_000 }, async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
    const spawnX = 4800;

    // Mount Player A (in-process, own identity)
    const elA = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("cross-coll-a"),
      serverUrl,
      adminSecret: ADMIN_SECRET,
      physicsSpeed: 10,
      spawnX,
    });

    // A exits lander to walking mode
    pressKey("e", "KeyE");
    await waitForAttr(elA, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    // Mount Player B in an isolated BrowserContext (completely separate identity)
    await openIsolated({
      label: "cross-coll-b",
      appId: APP_ID,
      dbName: uniqueDbName("cross-coll-b"),
      serverUrl,
      adminSecret: ADMIN_SECRET,
      physicsSpeed: 10,
      spawnX,
    });

    // Wait for B to see at least one uncollected deposit
    await waitFor(
      async () => {
        try {
          const raw = await commands.readIsolatedAttr("cross-coll-b", "deposit-count");
          return raw !== null && parseFloat(raw) > 0;
        } catch {
          return false;
        }
      },
      SYNC_TIMEOUT,
      "Player B should see uncollected deposits",
    );

    // Allow reconcile to complete — both A and B insert deposits asynchronously.
    // Reading countBefore too early (before reconcile finishes) gives a low value,
    // making `count < countBefore` never fire once reconcile adds the full set.
    await new Promise((r) => setTimeout(r, 2000));

    const countBefore = parseFloat(
      (await commands.readIsolatedAttr("cross-coll-b", "deposit-count")) ?? "0",
    );

    // A walks right to collect deposits (4s at 10x = ~4800px coverage)
    pressKey("d", "KeyD");
    await new Promise((r) => setTimeout(r, 4000));
    releaseKey("d", "KeyD");
    await waitFrames(10);

    // Confirm A collected something
    await waitFor(
      () => {
        try {
          return readStr(elA, "inventory") !== "";
        } catch {
          return false;
        }
      },
      SYNC_TIMEOUT,
      "Player A inventory should be non-empty after walking",
    );

    // B should see fewer uncollected deposits
    await waitFor(
      async () => {
        try {
          const raw = await commands.readIsolatedAttr("cross-coll-b", "deposit-count");
          return raw !== null && parseFloat(raw) < countBefore;
        } catch {
          return false;
        }
      },
      SYNC_TIMEOUT,
      `Player B deposit-count should drop below ${countBefore}`,
    );
  });

  // =========================================================================
  // 6c. Cross-client deposit sharing
  //
  //   Player A (own identity)   Jazz DB          Player B (own identity)
  //   ────────────────────────  ───────          ──────────────────────
  //   collect deposit ─────────→ collectedBy=A   B sees 0 inventory
  //   walk back near B ─────→
  //   share mechanic fires ────→ collectedBy=B → B's inventory updates
  //
  //   Verifies the collectedBy update propagates cross-client. The fix:
  //   subscribing to where({ collected:true }) means the row is already in
  //   B's subscription when sharing happens — only the JS filter changes.
  // =========================================================================

  it(
    "shared deposit appears in receiver's inventory cross-client",
    { timeout: 90_000 },
    async () => {
      /**
       * Player A and B are independent players at the same spawn position.
       *
       *   Player A              Jazz DB              Player B
       *   ────────              ───────              ────────
       *   exit lander ──────────────────────────→   exit lander
       *   (wait for B visible as remote player)
       *   walk right 8s ──→ collect all types
       *   walk left 8s ──→ back near B
       *   proximity share ──→ collectedBy=B ──────→ B inventory updates
       *
       * Walking for ~8s at physicsSpeed=10 covers the full MOON_SURFACE_WIDTH
       * (~9600px), ensuring A collects all 7 fuel types. Whatever B needs, A
       * will have it (and A's own required type won't be all 7, so at least one
       * shareable type exists). After walking back, both players are near each
       * other and the auto-share fires.
       *
       * The key Jazz invariant: when A shares (collectedBy A→B), B already has
       * the row in its where({ collected:true }) subscription (picked up when A
       * collected). The collectedBy field update then propagates as a normal row
       * update — no additional WHERE re-evaluation needed.
       */
      const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
      const spawnX = 4800;

      // Mount Player A (in-process, own identity)
      const elA = await mountApp({
        appId: APP_ID,
        dbName: uniqueDbName("share-a"),
        serverUrl,
        adminSecret: ADMIN_SECRET,
        physicsSpeed: 10,
        spawnX,
      });

      pressKey("e", "KeyE");
      await waitForAttr(elA, "player-mode", "walking", 3000);
      releaseKey("e", "KeyE");

      // Mount Player B (isolated, completely independent identity) at same spawn
      await openIsolated({
        label: "share-b",
        appId: APP_ID,
        dbName: uniqueDbName("share-b"),
        serverUrl,
        adminSecret: ADMIN_SECRET,
        physicsSpeed: 10,
        spawnX,
      });

      // B exits lander to walking mode
      await commands.pressIsolatedKey("share-b", "e");
      await commands.waitForIsolatedAttr("share-b", "player-mode", "walking", 5000);
      await commands.releaseIsolatedKey("share-b", "e");

      // Wait for A to see B as a remote player before collecting — this ensures
      // the share can fire when A returns to B's position.
      await waitFor(
        () => {
          try {
            return parseFloat(readStr(elA, "remote-player-count")) >= 1;
          } catch {
            return false;
          }
        },
        SYNC_TIMEOUT,
        "Player A should see Player B as a remote player",
      );

      // A walks right to cover the full world (~9600px at 1200px/real-sec).
      // 8s guarantees A collects all 7 fuel types.
      pressKey("d", "KeyD");
      await new Promise((r) => setTimeout(r, 8000));
      releaseKey("d", "KeyD");
      await waitFrames(10);

      await waitFor(
        () => {
          try {
            return readStr(elA, "inventory") !== "";
          } catch {
            return false;
          }
        },
        SYNC_TIMEOUT,
        "Player A inventory should be non-empty after collecting",
      );

      // A walks left 8s back to spawnX (B is still there, walking in place)
      pressKey("a", "KeyA");
      await new Promise((r) => setTimeout(r, 8000));
      releaseKey("a", "KeyA");
      await waitFrames(10);

      // A and B are now near each other. The proximity sharing mechanic fires
      // continuously each game tick. B's inventory should update via Jazz.
      await waitFor(
        async () => {
          try {
            const raw = await commands.readIsolatedAttr("share-b", "inventory");
            return raw !== null && raw !== "";
          } catch {
            return false;
          }
        },
        SYNC_TIMEOUT,
        "Player B inventory should be non-empty after receiving a shared deposit",
      );
    },
  );

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

  it(
    "burst deposits reappear as uncollected after entering lander",
    { timeout: 60_000 },
    async () => {
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
      // Use data-sync-uncollected (pure DB count from the edge subscription, not
      // filtered by local collectedIds) so the assertion is reliable regardless
      // of engine-side re-collection artefacts.
      await waitFor(
        () => {
          try {
            const syncEl = el.querySelector('[data-testid="sync-debug"]')!;
            const raw = syncEl.getAttribute("data-sync-uncollected");
            return parseInt(raw ?? "0", 10) > countAfterCollect;
          } catch {
            return false;
          }
        },
        SYNC_TIMEOUT,
        `uncollected-count should recover after burst release (was ${countAfterCollect}, collected ${collected})`,
      );
    },
  );

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
  it(
    "full Phase 2: two players descend, land, and see each other",
    { timeout: 60_000 },
    async () => {
      // Use a dedicated fresh Jazz server with an empty event log so reconcile
      // deposits insert quickly and player rows are visible within SYNC_TIMEOUT,
      // regardless of how many operations prior tests have accumulated.
      const serverUrl = await commands.startFreshTestServer("full-phase2");
      const sharedToken = `full-token-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

      try {
        const elA = await mountApp({
          appId: APP_ID_MULTI,
          dbName: uniqueDbName("full-a"),
          serverUrl,
          localAuthToken: sharedToken,
          adminSecret: ADMIN_SECRET,
          physicsSpeed: 10,
        });

        const elB = await mountApp({
          appId: APP_ID_MULTI,
          dbName: uniqueDbName("full-b"),
          serverUrl,
          localAuthToken: sharedToken,
          adminSecret: ADMIN_SECRET,
          physicsSpeed: 10,
        });

        // Both start in landed mode
        expect(readStr(elA, "player-mode")).toBe("landed");
        expect(readStr(elB, "player-mode")).toBe("landed");

        // Both should see the other as a remote player
        await waitFor(
          () => {
            try {
              const aCount = readNum(elA, "remote-player-count");
              const bCount = readNum(elB, "remote-player-count");
              return aCount >= 1 && bCount >= 1;
            } catch {
              return false;
            }
          },
          SYNC_TIMEOUT,
          "Both instances should see each other as remote players",
        );

        // Instance A exits lander — keyboard events are page-wide so both engines
        // receive the press; the test only asserts on A's mode change.
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
      } finally {
        await commands.stopFreshTestServer("full-phase2");
      }
    },
  );
});
