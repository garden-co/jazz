import type { DbConfig } from "jazz-tools";
import { JazzProvider, useAll, useDb } from "jazz-tools/react";
import { useCallback, useDeferredValue, useEffect, useMemo, useRef, useState } from "react";
import { app } from "../schema/app.js";
import { Game } from "./Game.js";
import type { FuelType } from "./game/constants.js";
import { DB_SYNC_INTERVAL_MS, FUEL_TYPES, MOON_SURFACE_WIDTH } from "./game/constants.js";
import type { ChatMessage, GameState, RemotePlayer } from "./game/types.js";

// ---------------------------------------------------------------------------
// Jazz write helpers — each function is a self-contained DB write pattern
// ---------------------------------------------------------------------------

const STALE_THRESHOLD_S = 180; // 3 minutes

/** Base number of uncollected deposits per fuel type. */
const DEPOSITS_PER_TYPE = 3;
/** Cooldown (ms) between top-up checks to avoid racing the subscription. */
const TOP_UP_COOLDOWN_MS = 3000;

/**
 * Top up fuel deposits so each type has the correct number uncollected:
 *   DEPOSITS_PER_TYPE base + 1 per player whose requiredFuelType matches.
 *
 * Runs on a cooldown to let the subscription reflect recent inserts before
 * recounting.
 */
/**
 * Insert deposits if any fuel type has fewer uncollected than target.
 *
 * Every client runs this — races are harmless because excess deposits
 * exceed the subscription limit and become invisible. The cooldown
 * prevents rapid duplicate creation.
 */
function topUpDeposits(
  db: ReturnType<typeof useDb>,
  perTypeCounts: number[],
  perTypeLimits: number[],
  elapsed: number,
  lastTopUpRef: React.MutableRefObject<number>,
) {
  const GRACE_MS = 2000;
  if (elapsed <= GRACE_MS) return;

  const now = Date.now();
  if (now - lastTopUpRef.current < TOP_UP_COOLDOWN_MS) return;

  const nowS = Math.floor(now / 1000);
  let inserted = false;

  for (let i = 0; i < FUEL_TYPES.length; i++) {
    const diff = perTypeLimits[i] - perTypeCounts[i];
    if (diff > 0) {
      for (let j = 0; j < diff; j++) {
        db.insert(app.fuel_deposits, {
          fuelType: FUEL_TYPES[i],
          positionX: Math.floor(Math.random() * MOON_SURFACE_WIDTH),
          createdAt: nowS,
          collected: false,
          collectedBy: "",
        });
      }
      inserted = true;
    }
  }

  if (inserted) lastTopUpRef.current = now;
}

/** Returns true if any synced field in GameState has changed meaningfully. */
function gameStateChanged(a: GameState, b: GameState): boolean {
  const POSITION_THRESHOLD = 2; // pixels
  const VELOCITY_THRESHOLD = 0.5; // pixels/tick
  return (
    a.mode !== b.mode ||
    Math.abs(a.positionX - b.positionX) > POSITION_THRESHOLD ||
    Math.abs(a.positionY - b.positionY) > POSITION_THRESHOLD ||
    Math.abs(a.velocityX - b.velocityX) > VELOCITY_THRESHOLD ||
    Math.abs(a.velocityY - b.velocityY) > VELOCITY_THRESHOLD ||
    a.fuel !== b.fuel ||
    a.landerSpawnX !== b.landerSpawnX ||
    a.playerName !== b.playerName ||
    a.playerColor !== b.playerColor ||
    a.requiredFuelType !== b.requiredFuelType ||
    a.thrusting !== b.thrusting
  );
}

/** Sync local player state to Jazz (insert or update). Skips writes when nothing changed. */
function syncPlayerState(
  db: ReturnType<typeof useDb>,
  playerId: string,
  state: GameState | null,
  dbRowIdRef: React.MutableRefObject<string | null>,
  lastSyncedRef: React.MutableRefObject<GameState | null>,
  localPlayerRows: Array<{ id: string }>,
  elapsed: number,
) {
  if (!state) return;
  const GRACE_MS = 2000;

  if (!dbRowIdRef.current && localPlayerRows.length > 0) {
    dbRowIdRef.current = localPlayerRows[0].id;
  }

  if (dbRowIdRef.current) {
    if (lastSyncedRef.current && !gameStateChanged(lastSyncedRef.current, state)) return;
    db.update(app.players, dbRowIdRef.current, {
      playerId,
      name: state.playerName,
      color: state.playerColor,
      mode: state.mode,
      online: true,
      lastSeen: Math.floor(Date.now() / 1000),
      positionX: state.positionX,
      positionY: state.positionY,
      velocityX: state.velocityX,
      velocityY: state.velocityY,
      requiredFuelType: state.requiredFuelType,
      landerFuelLevel: state.fuel,
      landerSpawnX: state.landerSpawnX,
      thrusting: state.thrusting,
    });
    lastSyncedRef.current = { ...state };
  } else if (elapsed > GRACE_MS) {
    dbRowIdRef.current = db.insert(app.players, {
      playerId,
      name: state.playerName,
      color: state.playerColor,
      mode: state.mode,
      online: true,
      lastSeen: Math.floor(Date.now() / 1000),
      positionX: state.positionX,
      positionY: state.positionY,
      velocityX: state.velocityX,
      velocityY: state.velocityY,
      requiredFuelType: state.requiredFuelType,
      landerFuelLevel: state.fuel,
      landerSpawnX: state.landerSpawnX,
      thrusting: state.thrusting,
    });
    lastSyncedRef.current = { ...state };
  }
}

/** Write pending deposit collections to Jazz. */
function flushDepositCollections(
  db: ReturnType<typeof useDb>,
  playerId: string,
  pending: React.MutableRefObject<string[]>,
) {
  for (const depId of pending.current.splice(0)) {
    db.update(app.fuel_deposits, depId, {
      collected: true,
      collectedBy: playerId,
    });
  }
}

/** Write pending refuel consumptions to Jazz. */
function flushRefuelConsumptions(
  db: ReturnType<typeof useDb>,
  playerId: string,
  pending: React.MutableRefObject<FuelType[]>,
  deposits: Array<{
    id: string;
    collected: boolean;
    collectedBy: string;
    fuelType: string;
  }>,
) {
  for (const fuelType of pending.current.splice(0)) {
    const dep = deposits.find(
      (d) => d.collected && d.collectedBy === playerId && d.fuelType === fuelType,
    );
    if (dep) {
      db.update(app.fuel_deposits, dep.id, { collectedBy: "" });
    }
  }
}

/** Write pending fuel shares to Jazz. */
function flushFuelShares(
  db: ReturnType<typeof useDb>,
  playerId: string,
  pending: React.MutableRefObject<Array<{ fuelType: string; receiverPlayerId: string }>>,
  deposits: Array<{
    id: string;
    collected: boolean;
    collectedBy: string;
    fuelType: string;
  }>,
) {
  for (const share of pending.current.splice(0)) {
    const dep = deposits.find(
      (d) => d.collected && d.collectedBy === playerId && d.fuelType === share.fuelType,
    );
    if (dep) {
      db.update(app.fuel_deposits, dep.id, {
        collectedBy: share.receiverPlayerId,
      });
    }
  }
}

/** Write pending burst deposits to Jazz — orphan them (collected but unclaimed). */
function flushBurstDeposits(
  db: ReturnType<typeof useDb>,
  playerId: string,
  pending: React.MutableRefObject<string[]>,
  deposits: Array<{
    id: string;
    collected: boolean;
    collectedBy: string;
    fuelType: string;
  }>,
) {
  for (const fuelType of pending.current.splice(0)) {
    const dep = deposits.find(
      (d) => d.collected && d.collectedBy === playerId && d.fuelType === fuelType,
    );
    if (dep) {
      db.update(app.fuel_deposits, dep.id, { collectedBy: "" });
    }
  }
}

/** Write pending chat messages to Jazz. */
function flushChatMessages(
  db: ReturnType<typeof useDb>,
  playerId: string,
  pending: React.MutableRefObject<string[]>,
) {
  for (const text of pending.current.splice(0)) {
    db.insert(app.chat_messages, {
      playerId,
      message: text,
      createdAt: Math.floor(Date.now() / 1000),
    });
  }
}

// ---------------------------------------------------------------------------
// Debug panel — toggled by pressing 'j', persisted in localStorage
// ---------------------------------------------------------------------------

const DEBUG_STORAGE_KEY = "moonlander-debug-open";

interface DebugStats {
  perTypeCounts: number[];
  perTypeLimits: number[];
  myCollectedCount: number;
  remotePlayerCount: number;
  localPlayerRowCount: number;
  chatMessageCount: number;
  totalSubscriptionItems: number;
  mode: string;
  posX: number;
  posY: number;
  velX: number;
  velY: number;
}

function DebugPanel({ stats }: { stats: DebugStats }) {
  return (
    <div
      style={{
        position: "absolute",
        top: 10,
        left: "50%",
        transform: "translateX(-50%)",
        fontFamily: "monospace",
        fontSize: 11,
        color: "#00ffff",
        background: "rgba(10, 4, 20, 0.92)",
        border: "1px solid #ff00ff",
        padding: "8px 14px",
        zIndex: 1000,
        pointerEvents: "none",
        minWidth: 300,
        lineHeight: 1.7,
      }}
    >
      <div
        style={{
          color: "#ff00ff",
          fontSize: 10,
          textTransform: "uppercase",
          letterSpacing: 1,
          marginBottom: 4,
        }}
      >
        debug (j to close)
      </div>
      <div style={{ color: "#ff66ff", fontSize: 10, textTransform: "uppercase", marginBottom: 2 }}>
        subscriptions
      </div>
      <div>total items: {stats.totalSubscriptionItems}</div>
      <div
        style={{
          color: "#ff66ff",
          fontSize: 10,
          textTransform: "uppercase",
          marginTop: 4,
          marginBottom: 2,
        }}
      >
        deposits (uncollected per type)
      </div>
      {FUEL_TYPES.map((ft, i) => (
        <div key={ft}>
          {ft}: {stats.perTypeCounts[i]} / {stats.perTypeLimits[i]}
        </div>
      ))}
      <div style={{ marginTop: 4 }}>my collected: {stats.myCollectedCount}</div>
      <div
        style={{
          color: "#ff66ff",
          fontSize: 10,
          textTransform: "uppercase",
          marginTop: 4,
          marginBottom: 2,
        }}
      >
        players
      </div>
      <div>remote: {stats.remotePlayerCount}</div>
      <div>local rows: {stats.localPlayerRowCount}</div>
      <div style={{ marginTop: 4 }}>chat messages: {stats.chatMessageCount}</div>
      <div
        style={{
          color: "#ff66ff",
          fontSize: 10,
          textTransform: "uppercase",
          marginTop: 4,
          marginBottom: 2,
        }}
      >
        game state
      </div>
      <div>mode: {stats.mode}</div>
      <div>
        pos: {Math.round(stats.posX)}, {Math.round(stats.posY)}
      </div>
      <div>
        vel: {Math.round(stats.velX)}, {Math.round(stats.velY)}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// GameWithSync — bridges Game ↔ Jazz DB
// ---------------------------------------------------------------------------

function GameWithSync({ physicsSpeed, playerId }: { physicsSpeed?: number; playerId: string }) {
  const db = useDb();
  // Jazz-native filtering: only subscribe to remote players (ne = local)
  const remotePlayerRows = useAll(app.players.where({ playerId: { ne: playerId } }));
  // Separate subscription for the local player's row (for finding existing row on reload)
  const localPlayerRows = useAll(app.players.where({ playerId }));
  // Compute per-type deposit limits: DEPOSITS_PER_TYPE base + non-stale players needing that type.
  // All clients compute the same limits from the same player data → same subscriptions → same deposits.
  const localFuelType = localPlayerRows[0]?.requiredFuelType ?? FUEL_TYPES[0];
  const perTypeLimit = useMemo(() => {
    const nowS = Math.floor(Date.now() / 1000);
    const counts = new Map<string, number>();
    for (const ft of FUEL_TYPES) counts.set(ft, DEPOSITS_PER_TYPE);
    // +1 for local player
    counts.set(localFuelType, (counts.get(localFuelType) ?? DEPOSITS_PER_TYPE) + 1);
    // +1 per non-stale remote player needing each type
    for (const p of remotePlayerRows) {
      if (p.requiredFuelType && nowS - p.lastSeen < STALE_THRESHOLD_S) {
        counts.set(p.requiredFuelType, (counts.get(p.requiredFuelType) ?? DEPOSITS_PER_TYPE) + 1);
      }
    }
    return FUEL_TYPES.map((ft) => counts.get(ft) ?? DEPOSITS_PER_TYPE);
  }, [remotePlayerRows, localFuelType]);

  // Per-type subscriptions: limit = DEPOSITS_PER_TYPE + players needing that type.
  // Each subscription tells the server exactly which objects to sync — no stale data.
  // FUEL_TYPES is a compile-time constant (7 elements), so hook count is stable.
  const uncollected0 = useAll(
    app.fuel_deposits.where({ fuelType: FUEL_TYPES[0], collected: false }).limit(perTypeLimit[0]),
  );
  const uncollected1 = useAll(
    app.fuel_deposits.where({ fuelType: FUEL_TYPES[1], collected: false }).limit(perTypeLimit[1]),
  );
  const uncollected2 = useAll(
    app.fuel_deposits.where({ fuelType: FUEL_TYPES[2], collected: false }).limit(perTypeLimit[2]),
  );
  const uncollected3 = useAll(
    app.fuel_deposits.where({ fuelType: FUEL_TYPES[3], collected: false }).limit(perTypeLimit[3]),
  );
  const uncollected4 = useAll(
    app.fuel_deposits.where({ fuelType: FUEL_TYPES[4], collected: false }).limit(perTypeLimit[4]),
  );
  const uncollected5 = useAll(
    app.fuel_deposits.where({ fuelType: FUEL_TYPES[5], collected: false }).limit(perTypeLimit[5]),
  );
  const uncollected6 = useAll(
    app.fuel_deposits.where({ fuelType: FUEL_TYPES[6], collected: false }).limit(perTypeLimit[6]),
  );
  // Deposits collected by this player (inventory)
  const myCollectedDeposits = useAll(app.fuel_deposits.where({ collectedBy: playerId }));
  const allChatMessages = useAll(app.chat_messages);

  // Merge per-type uncollected into a single array
  const uncollectedDeposits = useMemo(
    () => [
      ...uncollected0,
      ...uncollected1,
      ...uncollected2,
      ...uncollected3,
      ...uncollected4,
      ...uncollected5,
      ...uncollected6,
    ],
    [
      uncollected0,
      uncollected1,
      uncollected2,
      uncollected3,
      uncollected4,
      uncollected5,
      uncollected6,
    ],
  );
  // Combined view for consumers that need both uncollected + my-collected
  const allDepositsRaw = useMemo(
    () => [...uncollectedDeposits, ...myCollectedDeposits],
    [uncollectedDeposits, myCollectedDeposits],
  );

  // Track the Jazz row ID for the local player so we can update (not re-insert)
  const dbRowIdRef = useRef<string | null>(null);
  const localPlayerRowsRef = useRef(localPlayerRows);
  localPlayerRowsRef.current = localPlayerRows;

  // Keep latest subscriptions accessible from setInterval
  const allDepositsRef = useRef(allDepositsRaw);
  allDepositsRef.current = allDepositsRaw;
  const perTypeCountsRef = useRef(FUEL_TYPES.map(() => 0));
  perTypeCountsRef.current = [
    uncollected0.length,
    uncollected1.length,
    uncollected2.length,
    uncollected3.length,
    uncollected4.length,
    uncollected5.length,
    uncollected6.length,
  ];
  const perTypeLimitRef = useRef(perTypeLimit);
  perTypeLimitRef.current = perTypeLimit;
  const remotePlayerRowsRef = useRef(remotePlayerRows);
  remotePlayerRowsRef.current = remotePlayerRows;

  // Buffer latest game state in a ref — written to DB on a separate interval
  // to avoid re-entrant WASM borrows when sync messages trigger React renders
  const latestStateRef = useRef<GameState | null>(null);
  // Track last synced state to skip redundant writes when nothing changed
  const lastSyncedStateRef = useRef<GameState | null>(null);

  const handleStateChange = useCallback((state: GameState) => {
    latestStateRef.current = state;
  }, []);

  // Pending deposit collections (WASM safety — written in setInterval, not during render)
  const pendingCollectionsRef = useRef<string[]>([]);
  const handleCollectDeposit = useCallback((id: string) => {
    pendingCollectionsRef.current.push(id);
  }, []);

  // Pending refuel consumptions (deposit consumed for lander fuel)
  const pendingRefuelsRef = useRef<FuelType[]>([]);
  const handleRefuel = useCallback((fuelType: FuelType) => {
    pendingRefuelsRef.current.push(fuelType);
  }, []);

  // Pending fuel shares (rewrite collectedBy from local → receiver)
  const pendingSharesRef = useRef<Array<{ fuelType: string; receiverPlayerId: string }>>([]);
  const handleShareFuel = useCallback((fuelType: string, receiverPlayerId: string) => {
    pendingSharesRef.current.push({ fuelType, receiverPlayerId });
  }, []);

  // Pending burst deposits (eject back to surface at new position)
  const pendingBurstsRef = useRef<string[]>([]);
  const handleBurstDeposit = useCallback((fuelType: string) => {
    pendingBurstsRef.current.push(fuelType);
  }, []);

  // Pending chat messages
  const pendingMessagesRef = useRef<string[]>([]);
  const handleSendMessage = useCallback((text: string) => {
    pendingMessagesRef.current.push(text);
  }, []);

  // Cooldown ref for deposit top-up (prevents racing the subscription)
  const lastTopUpRef = useRef(0);
  const mountedAtRef = useRef(Date.now());

  // Track when each deposit ID was first seen (monotonic seconds) for fade-in
  const depositSpawnTimesRef = useRef<Map<string, number>>(new Map());

  // Flush all DB writes in a single setInterval (player sync + deposit collection + seeding)
  useEffect(() => {
    const id = setInterval(() => {
      const elapsed = Date.now() - mountedAtRef.current;

      topUpDeposits(db, perTypeCountsRef.current, perTypeLimitRef.current, elapsed, lastTopUpRef);
      syncPlayerState(
        db,
        playerId,
        latestStateRef.current,
        dbRowIdRef,
        lastSyncedStateRef,
        localPlayerRowsRef.current,
        elapsed,
      );
      flushDepositCollections(db, playerId, pendingCollectionsRef);
      flushRefuelConsumptions(db, playerId, pendingRefuelsRef, allDepositsRef.current);
      flushFuelShares(db, playerId, pendingSharesRef, allDepositsRef.current);
      flushBurstDeposits(db, playerId, pendingBurstsRef, allDepositsRef.current);
      flushChatMessages(db, playerId, pendingMessagesRef);

      // Release any stale collected deposits when the game is restarting.
      // mergeInventory is skipped during these modes, but we still need to
      // clear the DB so items don't reappear when the player lands.
      const mode = latestStateRef.current?.mode;
      if (mode === "start" || mode === "descending") {
        for (const d of allDepositsRef.current) {
          if (d.collected && d.collectedBy === playerId) {
            db.update(app.fuel_deposits, d.id, { collectedBy: "" });
          }
        }
      }
    }, DB_SYNC_INTERVAL_MS);

    return () => clearInterval(id);
  }, [db, playerId]);

  // Map Jazz deposit subscription → Deposit[] for Game (uncollected only, with fade-in timing)
  const deposits = useMemo(() => {
    const spawnTimes = depositSpawnTimesRef.current;
    const now = performance.now() / 1000;
    // Record first-seen time for new deposits
    const activeIds = new Set<string>();
    for (const d of allDepositsRaw) {
      if (d.collected) continue;
      activeIds.add(d.id);
      if (!spawnTimes.has(d.id)) {
        spawnTimes.set(d.id, now);
      }
    }
    // Prune stale entries
    for (const id of spawnTimes.keys()) {
      if (!activeIds.has(id)) spawnTimes.delete(id);
    }
    return allDepositsRaw
      .filter((d) => !d.collected)
      .map((d) => ({
        id: d.id,
        x: d.positionX,
        type: d.fuelType as FuelType,
        spawnTime: spawnTimes.get(d.id) ?? now,
      }));
  }, [allDepositsRaw]);

  // Derive inventory from Jazz: fuel types where collectedBy = this player
  const inventory = useMemo(() => {
    return allDepositsRaw
      .filter((d) => d.collected && d.collectedBy === playerId)
      .map((d) => d.fuelType as FuelType);
  }, [allDepositsRaw, playerId]);

  // Map Jazz chat messages → ChatMessage[] for Game (recent only)
  const chatMessages: ChatMessage[] = useMemo(() => {
    if (!allChatMessages) return [];
    const nowS = Math.floor(Date.now() / 1000);
    return allChatMessages
      .filter((m) => nowS - m.createdAt < 60) // only last 60 seconds
      .map((m) => ({
        id: m.id,
        playerId: m.playerId,
        message: m.message,
        createdAt: m.createdAt,
      }));
  }, [allChatMessages]);

  // Map Jazz subscription → RemotePlayer[] for Game.
  // Jazz query already excludes the local player (ne filter).
  // Staleness filter applied here so Game receives only active players.
  const remotePlayers: RemotePlayer[] = useMemo(() => {
    const nowS = Math.floor(Date.now() / 1000);
    return remotePlayerRows
      .filter((p) => nowS - p.lastSeen < STALE_THRESHOLD_S)
      .map((p) => ({
        id: p.id,
        name: p.name,
        mode: p.mode as RemotePlayer["mode"],
        positionX: p.positionX,
        positionY: p.positionY,
        velocityX: p.velocityX,
        velocityY: p.velocityY,
        color: p.color,
        requiredFuelType: p.requiredFuelType,
        lastSeen: p.lastSeen,
        landerFuelLevel: p.landerFuelLevel,
        thrusting: p.thrusting,
        playerId: p.playerId,
        landerX: p.landerSpawnX,
        // Approximate: if lander fuel is at max, they've already collected their required type
        hasRequiredFuel: p.landerFuelLevel >= 100,
      }));
  }, [remotePlayerRows]);

  // Defer subscription-derived values so React yields to the game loop (rAF)
  // between subscription update bursts. The game engine reads from refs anyway,
  // so a few frames of delay on these props is invisible.
  const deferredDeposits = useDeferredValue(deposits);
  const deferredInventory = useDeferredValue(inventory);
  const deferredRemotePlayers = useDeferredValue(remotePlayers);
  const deferredChatMessages = useDeferredValue(chatMessages);

  // Debug panel — toggled by 'j', persisted in localStorage
  const [debugOpen, setDebugOpen] = useState(() => {
    try {
      return localStorage.getItem(DEBUG_STORAGE_KEY) === "true";
    } catch {
      return false;
    }
  });
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.code === "KeyJ" && !e.ctrlKey && !e.metaKey && !e.altKey) {
        setDebugOpen((prev) => {
          const next = !prev;
          try {
            localStorage.setItem(DEBUG_STORAGE_KEY, String(next));
          } catch {
            /* ignore */
          }
          return next;
        });
      }
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  const debugStats: DebugStats | null = debugOpen
    ? {
        perTypeCounts: [
          uncollected0.length,
          uncollected1.length,
          uncollected2.length,
          uncollected3.length,
          uncollected4.length,
          uncollected5.length,
          uncollected6.length,
        ],
        perTypeLimits: perTypeLimit,
        myCollectedCount: myCollectedDeposits.length,
        remotePlayerCount: remotePlayerRows.length,
        localPlayerRowCount: localPlayerRows.length,
        chatMessageCount: allChatMessages?.length ?? 0,
        totalSubscriptionItems:
          uncollected0.length +
          uncollected1.length +
          uncollected2.length +
          uncollected3.length +
          uncollected4.length +
          uncollected5.length +
          uncollected6.length +
          myCollectedDeposits.length +
          remotePlayerRows.length +
          localPlayerRows.length +
          (allChatMessages?.length ?? 0),
        mode: latestStateRef.current?.mode ?? "?",
        posX: latestStateRef.current?.positionX ?? 0,
        posY: latestStateRef.current?.positionY ?? 0,
        velX: latestStateRef.current?.velocityX ?? 0,
        velY: latestStateRef.current?.velocityY ?? 0,
      }
    : null;

  return (
    <>
      <Game
        playerId={playerId}
        physicsSpeed={physicsSpeed}
        remotePlayers={deferredRemotePlayers}
        deposits={deferredDeposits}
        inventory={deferredInventory}
        chatMessages={deferredChatMessages}
        onCollectDeposit={handleCollectDeposit}
        onRefuel={handleRefuel}
        onShareFuel={handleShareFuel}
        onBurstDeposit={handleBurstDeposit}
        onSendMessage={handleSendMessage}
        onStateChange={handleStateChange}
      />
      {debugStats && <DebugPanel stats={debugStats} />}
    </>
  );
}

// ---------------------------------------------------------------------------
// App — wraps Game in JazzProvider when config is provided
// ---------------------------------------------------------------------------

interface AppProps {
  config?: DbConfig;
  playerId?: string;
  physicsSpeed?: number;
}

export function App({ config, playerId, physicsSpeed }: AppProps) {
  // No config → standalone Game (Phase 1 compatibility)
  if (!config) {
    return <Game physicsSpeed={physicsSpeed} />;
  }

  return (
    <JazzProvider config={config}>
      <GameWithSync physicsSpeed={physicsSpeed} playerId={playerId ?? crypto.randomUUID()} />
    </JazzProvider>
  );
}
