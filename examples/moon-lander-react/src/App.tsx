import { useRef, useCallback, useMemo } from "react";
import { JazzProvider, useDb, useAll } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { app } from "../schema/app.js";
import { Game } from "./Game.js";
import type { RemotePlayer, GameState, ChatMessage } from "./game/types.js";
import { DB_SYNC_INTERVAL_MS, FUEL_TYPES, MOON_SURFACE_WIDTH } from "./game/constants.js";
import type { FuelType } from "./game/constants.js";
import { useEffect } from "react";

// ---------------------------------------------------------------------------
// Jazz write helpers — each function is a self-contained DB write pattern
// ---------------------------------------------------------------------------

const STALE_THRESHOLD_S = 180; // 3 minutes

/** Seed fuel deposits into the DB if none exist (after a grace period). */
function seedDepositsIfEmpty(
  db: ReturnType<typeof useDb>,
  seededRef: React.MutableRefObject<boolean>,
  deposits: Array<{ id: string }> | undefined,
  elapsed: number,
) {
  const GRACE_MS = 2000;
  if (seededRef.current || !deposits) return;
  if (deposits.length > 0) {
    seededRef.current = true;
    return;
  }
  if (elapsed <= GRACE_MS) return;
  seededRef.current = true;
  const nowS = Math.floor(Date.now() / 1000);
  for (const fuelType of FUEL_TYPES) {
    for (let i = 0; i < 3; i++) {
      db.insert(app.fuel_deposits, {
        fuelType,
        positionX: Math.floor(Math.random() * MOON_SURFACE_WIDTH),
        createdAt: nowS,
        collected: false,
        collectedBy: "",
      });
    }
  }
}

/** Sync local player state to Jazz (insert or update). */
function syncPlayerState(
  db: ReturnType<typeof useDb>,
  playerId: string,
  state: GameState | null,
  dbRowIdRef: React.MutableRefObject<string | null>,
  localPlayerRows: Array<{ id: string }>,
  elapsed: number,
) {
  if (!state) return;
  const GRACE_MS = 2000;
  const playerData = {
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
  };

  if (!dbRowIdRef.current && localPlayerRows.length > 0) {
    dbRowIdRef.current = localPlayerRows[0].id;
  }

  if (dbRowIdRef.current) {
    db.update(app.players, dbRowIdRef.current, playerData);
  } else if (elapsed > GRACE_MS) {
    dbRowIdRef.current = db.insert(app.players, playerData);
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
  deposits:
    | Array<{ id: string; collected: boolean; collectedBy: string; fuelType: string }>
    | undefined,
) {
  for (const fuelType of pending.current.splice(0)) {
    if (!deposits) continue;
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
  deposits:
    | Array<{ id: string; collected: boolean; collectedBy: string; fuelType: string }>
    | undefined,
) {
  for (const share of pending.current.splice(0)) {
    if (!deposits) continue;
    const dep = deposits.find(
      (d) => d.collected && d.collectedBy === playerId && d.fuelType === share.fuelType,
    );
    if (dep) {
      db.update(app.fuel_deposits, dep.id, { collectedBy: share.receiverPlayerId });
    }
  }
}

/** Write pending burst deposits to Jazz. */
function flushBurstDeposits(
  db: ReturnType<typeof useDb>,
  playerId: string,
  pending: React.MutableRefObject<Array<{ fuelType: string; newX: number }>>,
  deposits:
    | Array<{ id: string; collected: boolean; collectedBy: string; fuelType: string }>
    | undefined,
) {
  for (const burst of pending.current.splice(0)) {
    if (!deposits) continue;
    const dep = deposits.find(
      (d) => d.collected && d.collectedBy === playerId && d.fuelType === burst.fuelType,
    );
    if (dep) {
      db.update(app.fuel_deposits, dep.id, {
        collected: false,
        collectedBy: "",
        positionX: Math.floor(burst.newX),
      });
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
// GameWithSync — bridges Game ↔ Jazz DB
// ---------------------------------------------------------------------------

function GameWithSync({ physicsSpeed, playerId }: { physicsSpeed?: number; playerId: string }) {
  const db = useDb();
  // Jazz-native filtering: only subscribe to remote players (ne = local)
  const remotePlayerRows = useAll(app.players.where({ playerId: { ne: playerId } }));
  // Separate subscription for the local player's row (for finding existing row on reload)
  const localPlayerRows = useAll(app.players.where({ playerId }));
  const allDepositsRaw = useAll(app.fuel_deposits);
  const allChatMessages = useAll(app.chat_messages);

  // Track the Jazz row ID for the local player so we can update (not re-insert)
  const dbRowIdRef = useRef<string | null>(null);
  const localPlayerRowsRef = useRef(localPlayerRows);
  localPlayerRowsRef.current = localPlayerRows;

  // Keep latest deposit subscription accessible from setInterval
  const allDepositsRef = useRef(allDepositsRaw);
  allDepositsRef.current = allDepositsRaw;

  // Buffer latest game state in a ref — written to DB on a separate interval
  // to avoid re-entrant WASM borrows when sync messages trigger React renders
  const latestStateRef = useRef<GameState | null>(null);

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
  const pendingBurstsRef = useRef<Array<{ fuelType: string; newX: number }>>([]);
  const handleBurstDeposit = useCallback((fuelType: string, newX: number) => {
    pendingBurstsRef.current.push({ fuelType, newX });
  }, []);

  // Pending chat messages
  const pendingMessagesRef = useRef<string[]>([]);
  const handleSendMessage = useCallback((text: string) => {
    pendingMessagesRef.current.push(text);
  }, []);

  // Seed flag — prevents re-seeding after initial population.
  // Grace period lets the subscription deliver existing data from OPFS/server
  // before we decide the table is empty.
  const seededRef = useRef(false);
  const mountedAtRef = useRef(Date.now());

  // Flush all DB writes in a single setInterval (player sync + deposit collection + seeding)
  useEffect(() => {
    const id = setInterval(() => {
      const elapsed = Date.now() - mountedAtRef.current;

      seedDepositsIfEmpty(db, seededRef, allDepositsRef.current, elapsed);
      syncPlayerState(
        db,
        playerId,
        latestStateRef.current,
        dbRowIdRef,
        localPlayerRowsRef.current,
        elapsed,
      );
      flushDepositCollections(db, playerId, pendingCollectionsRef);
      flushRefuelConsumptions(db, playerId, pendingRefuelsRef, allDepositsRef.current);
      flushFuelShares(db, playerId, pendingSharesRef, allDepositsRef.current);
      flushBurstDeposits(db, playerId, pendingBurstsRef, allDepositsRef.current);
      flushChatMessages(db, playerId, pendingMessagesRef);
    }, DB_SYNC_INTERVAL_MS);

    return () => clearInterval(id);
  }, [db, playerId]);

  // Map Jazz deposit subscription → Deposit[] for Game (uncollected only)
  const deposits = useMemo(() => {
    if (!allDepositsRaw) return [];
    return allDepositsRaw
      .filter((d) => !d.collected)
      .map((d) => ({
        id: d.id,
        x: d.positionX,
        type: d.fuelType as FuelType,
      }));
  }, [allDepositsRaw]);

  // Derive inventory from Jazz: fuel types where collectedBy = this player
  const inventory = useMemo(() => {
    if (!allDepositsRaw) return undefined;
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
        playerId: p.playerId,
        landerX: p.landerSpawnX,
      }));
  }, [remotePlayerRows]);

  return (
    <Game
      physicsSpeed={physicsSpeed}
      remotePlayers={remotePlayers}
      deposits={deposits}
      inventory={inventory}
      chatMessages={chatMessages}
      onCollectDeposit={handleCollectDeposit}
      onRefuel={handleRefuel}
      onShareFuel={handleShareFuel}
      onBurstDeposit={handleBurstDeposit}
      onSendMessage={handleSendMessage}
      onStateChange={handleStateChange}
    />
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
