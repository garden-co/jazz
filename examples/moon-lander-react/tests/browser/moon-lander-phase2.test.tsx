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

import { describe, it, expect, afterEach, beforeEach } from "vitest";
import { createRoot, type Root } from "react-dom/client";
import { act } from "react";
import { Game } from "../../src/Game.js";
import { App } from "../../src/App.js";
import {
  CANVAS_WIDTH,
  GROUND_LEVEL,
  FUEL_TYPES,
  INITIAL_FUEL,
  WALK_SPEED,
} from "../../src/game/constants.js";
import { TEST_PORT, JWT_SECRET, ADMIN_SECRET, APP_ID } from "./test-constants.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const mounts: Array<{ root: Root; container: HTMLDivElement }> = [];

/** Mount the Game component directly (no Jazz sync). */
async function mountGame(opts: { physicsSpeed?: number } = {}): Promise<HTMLDivElement> {
  const el = document.createElement("div");
  document.body.appendChild(el);
  const root = createRoot(el);
  mounts.push({ root, container: el });

  const props: Record<string, unknown> = {};
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

function base64url(input: string | Uint8Array): string {
  const str =
    typeof input === "string"
      ? btoa(input)
      : btoa(String.fromCharCode(...input));
  return str.replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

async function signJwt(sub: string, secret: string): Promise<string> {
  const header = { alg: "HS256", typ: "JWT" };
  const payload = {
    sub,
    claims: {},
    exp: Math.floor(Date.now() / 1000) + 3600,
  };
  const enc = new TextEncoder();
  const headerB64 = base64url(JSON.stringify(header));
  const payloadB64 = base64url(JSON.stringify(payload));
  const data = enc.encode(`${headerB64}.${payloadB64}`);
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, data);
  return `${headerB64}.${payloadB64}.${base64url(new Uint8Array(sig))}`;
}

/** Mount the App with JazzProvider for sync testing. */
async function mountApp(opts: {
  appId?: string;
  dbName?: string;
  serverUrl?: string;
  jwtToken?: string;
  adminSecret?: string;
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
  document.dispatchEvent(
    new KeyboardEvent("keydown", { key, code: code ?? key, bubbles: true }),
  );
}

/** Simulate releasing a key (keyup). */
function releaseKey(key: string, code?: string) {
  document.dispatchEvent(
    new KeyboardEvent("keyup", { key, code: code ?? key, bubbles: true }),
  );
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

    expect(readStr(el, "player-mode")).toBe("descending");

    // At 10x physics speed, landing should happen in ~1-2s instead of ~7s.
    // 3s timeout ensures it's meaningfully faster than the default.
    await waitForAttr(el, "player-mode", "landed", 3000);
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
    const token1 = await signJwt("lander-a", JWT_SECRET);
    const token2 = await signJwt("lander-b", JWT_SECRET);

    // Mount Instance A — starts descending
    const elA = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("sync-a"),
      serverUrl,
      jwtToken: token1,
      adminSecret: ADMIN_SECRET,
      physicsSpeed: 10,
    });

    // Mount Instance B — should eventually see Instance A as remote
    const elB = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("sync-b"),
      serverUrl,
      jwtToken: token2,
      adminSecret: ADMIN_SECRET,
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
      10000,
      "Instance B should see Instance A as a remote player",
    );
  });

  it("syncs landed state between two instances", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
    const token1 = await signJwt("lander-c", JWT_SECRET);
    const token2 = await signJwt("lander-d", JWT_SECRET);

    const elA = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("landed-a"),
      serverUrl,
      jwtToken: token1,
      adminSecret: ADMIN_SECRET,
      physicsSpeed: 10,
    });

    // Wait for Instance A to land
    await waitForAttr(elA, "player-mode", "landed", 3000);

    const elB = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("landed-b"),
      serverUrl,
      jwtToken: token2,
      adminSecret: ADMIN_SECRET,
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
      10000,
      "Instance B should see Instance A's landed player",
    );
  });

  it("does not render stale remote players (lastSeen > 180s ago)", async () => {
    // Tests Game's stale-filtering logic directly — no sync needed.
    const staleTime = Math.floor(Date.now() / 1000) - 300; // 5 minutes ago

    const el = document.createElement("div");
    document.body.appendChild(el);
    const root = createRoot(el);
    mounts.push({ root, container: el });

    await act(async () => {
      root.render(
        <Game
          {...({
            physicsSpeed: 10,
            remotePlayers: [
              {
                id: "remote-stale",
                name: "Ghost",
                mode: "walking",
                positionX: CANVAS_WIDTH / 2 + 50,
                positionY: GROUND_LEVEL,
                velocityX: 0,
                velocityY: 0,
                color: "#888888",
                requiredFuelType: "circle",
                lastSeen: staleTime,
                landerFuelLevel: 0,
              },
            ],
          } as any)}
        />,
      );
    });

    await waitFor(
      () => el.querySelector('[data-testid="game-canvas"]') !== null,
      3000,
      "Game canvas should render",
    );
    await waitFrames(5);

    const count = readNum(el, "remote-player-count");
    expect(count).toBe(0);
  });

  it("syncs walking mode between two instances", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
    const token1 = await signJwt("lander-e", JWT_SECRET);
    const token2 = await signJwt("lander-f", JWT_SECRET);

    const elA = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("walk-a"),
      serverUrl,
      jwtToken: token1,
      adminSecret: ADMIN_SECRET,
      physicsSpeed: 10,
    });

    // Instance A: land, then exit lander
    await waitForAttr(elA, "player-mode", "landed", 3000);
    pressKey("e", "KeyE");
    await waitForAttr(elA, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    const elB = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("walk-b"),
      serverUrl,
      jwtToken: token2,
      adminSecret: ADMIN_SECRET,
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
      10000,
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
    const token = await signJwt("inv-test", JWT_SECRET);
    const playerId = crypto.randomUUID();

    const el = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("inv-a"),
      serverUrl,
      jwtToken: token,
      adminSecret: ADMIN_SECRET,
      playerId,
      physicsSpeed: 10,
    });

    // Wait for landing
    await waitForAttr(el, "player-mode", "landed", 5000);

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
    const tokenA = await signJwt("coll-a", JWT_SECRET);
    const tokenB = await signJwt("coll-b", JWT_SECRET);

    const elA = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("coll-a"),
      serverUrl,
      jwtToken: tokenA,
      adminSecret: ADMIN_SECRET,
      physicsSpeed: 10,
    });

    const elB = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("coll-b"),
      serverUrl,
      jwtToken: tokenB,
      adminSecret: ADMIN_SECRET,
      physicsSpeed: 10,
    });

    // Both land
    await waitForAttr(elA, "player-mode", "landed", 5000);
    await waitForAttr(elB, "player-mode", "landed", 5000);

    // Wait for deposits to be seeded and visible to both
    await waitFor(
      () => {
        try {
          return readNum(elA, "deposit-count") > 0 && readNum(elB, "deposit-count") > 0;
        } catch {
          return false;
        }
      },
      10000,
      "Both instances should see seeded deposits",
    );

    const countBefore = readNum(elB, "deposit-count");

    // Player A exits lander and walks to collect deposits
    pressKey("e", "KeyE");
    await waitForAttr(elA, "player-mode", "walking", 3000);
    releaseKey("e", "KeyE");

    pressKey("d", "KeyD");
    await new Promise((r) => setTimeout(r, 3000));
    releaseKey("d", "KeyD");
    await waitFrames(10);

    // Wait for Player B to see fewer deposits (Jazz propagation)
    await waitFor(
      () => {
        try {
          return readNum(elB, "deposit-count") < countBefore;
        } catch {
          return false;
        }
      },
      10000,
      "Player B should see fewer deposits after Player A collects some",
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
    const token1 = await signJwt("lander-g", JWT_SECRET);
    const token2 = await signJwt("lander-h", JWT_SECRET);

    const elA = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("full-a"),
      serverUrl,
      jwtToken: token1,
      adminSecret: ADMIN_SECRET,
      physicsSpeed: 10,
    });

    const elB = await mountApp({
      appId: APP_ID,
      dbName: uniqueDbName("full-b"),
      serverUrl,
      jwtToken: token2,
      adminSecret: ADMIN_SECRET,
      physicsSpeed: 10,
    });

    // Both should start descending
    expect(readStr(elA, "player-mode")).toBe("descending");
    expect(readStr(elB, "player-mode")).toBe("descending");

    // Both land
    await waitForAttr(elA, "player-mode", "landed", 3000);
    await waitForAttr(elB, "player-mode", "landed", 3000);

    // Both should see the other as a remote player
    await waitFor(
      () => {
        try {
          return (
            readNum(elA, "remote-player-count") >= 1 &&
            readNum(elB, "remote-player-count") >= 1
          );
        } catch {
          return false;
        }
      },
      10000,
      "Both instances should see each other as remote players",
    );

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
      10000,
      "Instance B should still see Instance A after mode change",
    );
  });
});
