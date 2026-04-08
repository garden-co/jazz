---
marp: true
title: How Moon Lander uses Jazz
theme: jazz
paginate: true
---

<!-- _class: hero -->

# How Moon Lander uses Jazz

A walkthrough of real-time multiplayer in a browser game, built with Jazz, React, and a canvas physics engine.

Players share a moon surface: they collect fuel deposits, trade fuel with each other, and see each other's landers moving in real time.

![bg right:42% 90%](screenshots/01-game-landed.png)

---

## What is Jazz?

Jazz is a **local-first** sync framework. Every client runs a full database in a WASM worker, persisted to disk via OPFS. Changes sync to an edge server and fan out to all connected clients in real time.

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 560 212" width="520" height="196" style="display:block;margin:0.5rem auto">
  <defs>
    <marker id="arr" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><polygon points="0 0, 8 3, 0 6" fill="#6b7280"/></marker>
    <marker id="arrs" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto-start-reverse"><polygon points="0 0, 8 3, 0 6" fill="#6b7280"/></marker>
  </defs>
  <rect x="180" y="10" width="200" height="58" rx="8" fill="#dcfce7" stroke="#16a34a" stroke-width="1.5"/>
  <text x="280" y="34" text-anchor="middle" font-family="ui-sans-serif,sans-serif" font-size="13" font-weight="700" fill="#166534">Jazz sync server</text>
  <text x="280" y="54" text-anchor="middle" font-family="ui-sans-serif,sans-serif" font-size="11" fill="#166534">sync + fan-out</text>
  <rect x="8" y="130" width="170" height="74" rx="8" fill="#dbeafe" stroke="#3b82f6" stroke-width="1.5"/>
  <text x="93" y="154" text-anchor="middle" font-family="ui-sans-serif,sans-serif" font-size="13" font-weight="700" fill="#1e40af">Browser A</text>
  <text x="93" y="174" text-anchor="middle" font-family="ui-monospace,monospace" font-size="11" fill="#1e3a8a">WASM worker</text>
  <text x="93" y="192" text-anchor="middle" font-family="ui-monospace,monospace" font-size="11" fill="#1e3a8a">OPFS (local DB)</text>
  <rect x="382" y="130" width="170" height="74" rx="8" fill="#dbeafe" stroke="#3b82f6" stroke-width="1.5"/>
  <text x="467" y="154" text-anchor="middle" font-family="ui-sans-serif,sans-serif" font-size="13" font-weight="700" fill="#1e40af">Browser B</text>
  <text x="467" y="174" text-anchor="middle" font-family="ui-monospace,monospace" font-size="11" fill="#1e3a8a">WASM worker</text>
  <text x="467" y="192" text-anchor="middle" font-family="ui-monospace,monospace" font-size="11" fill="#1e3a8a">OPFS (local DB)</text>
  <line x1="215" y1="68" x2="93" y2="128" stroke="#6b7280" stroke-width="1.5" stroke-dasharray="5,3" marker-start="url(#arrs)" marker-end="url(#arr)"/>
  <line x1="345" y1="68" x2="467" y2="128" stroke="#6b7280" stroke-width="1.5" stroke-dasharray="5,3" marker-start="url(#arrs)" marker-end="url(#arr)"/>
</svg>

- No REST API. No WebSockets to manage. No manual state reconciliation.
- Writes are **instant locally**. Sync happens in the background.
- Every client is always readable, even offline.

---

## The schema

Three tables define the entire multiplayer state. Written in a TypeScript DSL in [`schema.ts`](../schema.ts).

<div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-top:0.5rem">
<div>

```typescript
import { schema as s } from "jazz-tools";

const schema = {
  players: s.table({
    playerId: s.string(),
    name: s.string(),
    color: s.string(),
    mode: s.string(),
    online: s.boolean(),
    lastSeen: s.int(),
    positionX: s.int(),
    positionY: s.int(),
    velocityX: s.int(),
    velocityY: s.int(),
    requiredFuelType: s.string(),
    landerFuelLevel: s.int(),
    landerSpawnX: s.int(),
    thrusting: s.boolean(),
  }),
```

</div>
<div>

```typescript
  fuel_deposits: s.table({
    fuelType: s.string(),
    positionX: s.int(),
    createdAt: s.int(),
    collected: s.boolean(),
    collectedBy: s.string(),
  }),
  chat_messages: s.table({
    playerId: s.string(),
    message: s.string(),
    createdAt: s.int(),
  }),
};
```

</div>
</div>

---

## Client setup

`JazzProvider` accepts a `config` object and handles the WASM worker, OPFS database, and sync connection internally. It makes `db` available to every component in the tree.

**[`src/App.tsx`](../src/App.tsx)**

```typescript
import { JazzProvider } from "jazz-tools/react";

export function App({ config, playerId, physicsSpeed, initialMode, spawnX }: AppProps) {
  if (!config) {
    return <Game physicsSpeed={physicsSpeed} initialMode={initialMode} spawnX={spawnX} />;
  }

  return (
    <JazzProvider config={config}>
      <GameWithSync playerId={playerId ?? crypto.randomUUID()} />
    </JazzProvider>
  );
}
```

Without a config, `<Game>` mounts directly with no Jazz layer, useful for offline play and tests.

---

## Where Jazz lives in the source tree

All Jazz integration is in one folder:

```
src/jazz/
├── GameWithSync.tsx   ← bridge: Jazz data → Game props + write callbacks
├── useSync.ts         ← all Jazz reads (subscriptions to 3 tables)
└── SyncManager.ts     ← all Jazz writes (immediate fire-and-forget)
```

`src/game/` and `src/Game.tsx` are pure game engine code. They receive data via props and callbacks and know nothing about Jazz.

This separation means you can read the entire Jazz integration by looking at three files, without touching any physics or rendering code.

---

## Live subscriptions: `useAll`

![bg right:38% 88%](screenshots/02-surface-deposits.png)

**[`src/jazz/useSync.ts`](../src/jazz/useSync.ts)**

```typescript
import { useAll, useDb } from "jazz-tools/react";

export function useSync(playerId: string): SyncResult {
  // Other players' positions, modes, fuel levels (live from the server)
  const remotePlayers = useAll(
    app.players.where({ playerId: { ne: playerId } }),
  );

  // Deposits on the surface (drives the game's collectible objects)
  const uncollectedDeposits = useAll(
    app.fuel_deposits.where({ collected: false }),
  );

  // Chat messages, newest-last
  const chatMessages = useAll(
    app.chat_messages.orderBy("createdAt", "asc"),
  );
  ...
}
```

Results stream from the sync server. When any client writes, every subscriber re-renders automatically. No polling, no manual invalidation.

---

## Waiting for the edge: the `settled` flag

The first `useAll` result may arrive before all remote data has synced. Moon Lander uses a `settled` flag to delay game setup until the edge subscription has delivered its initial payload.

```typescript
// "edge" tier: undefined = still connecting to server; [] or [...] = server has responded
const allUncollected = useAll(app.fuel_deposits.where({ collected: false }), "edge");

const settled = allUncollected !== undefined;
```

`settled` gates two things:

1. **Deposit reconciliation**: ensuring the surface has the right number of deposits for the current player count (runs once, after settle).
2. **Player row insert**: the local player is only written to the DB after settle, preventing duplicate rows from concurrent joins.

---

## Durability tiers

The tier on a write controls where the promise resolves. All writes eventually propagate everywhere; the tier controls how far it must travel before the promise resolves.

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 640 238" width="960" height="357" style="display:block;margin:0.5rem auto">
  <defs>
    <marker id="aw" markerWidth="6" markerHeight="5" refX="6" refY="2.5" orient="auto"><polygon points="0 0,6 2.5,0 5" fill="rgba(20,106,255,0.45)"/></marker>
    <marker id="ae" markerWidth="6" markerHeight="5" refX="6" refY="2.5" orient="auto"><polygon points="0 0,6 2.5,0 5" fill="rgba(20,106,255,0.7)"/></marker>
    <marker id="ag" markerWidth="6" markerHeight="5" refX="6" refY="2.5" orient="auto"><polygon points="0 0,6 2.5,0 5" fill="#146aff"/></marker>
    <marker id="as" markerWidth="6" markerHeight="5" refX="6" refY="2.5" orient="auto"><polygon points="0 0,6 2.5,0 5" fill="#146aff"/></marker>
  </defs>

  <!-- Participant boxes (centers: 75, 230, 390, 555) -->
  <rect x="12"  y="10" width="126" height="38" rx="4" fill="#f8f8fc" stroke="#e0e0f0" stroke-width="1.5"/>
  <text x="75"  y="34" text-anchor="middle" font-family="Manrope,sans-serif" font-size="13" font-weight="700" fill="#1a1a2e">Client</text>

  <rect x="165" y="10" width="130" height="38" rx="4" fill="#f8f8fc" stroke="#e0e0f0" stroke-width="1.5"/>
  <text x="230" y="34" text-anchor="middle" font-family="Manrope,sans-serif" font-size="13" font-weight="700" fill="#1a1a2e">OPFS Worker</text>

  <rect x="325" y="10" width="130" height="38" rx="4" fill="#f8f8fc" stroke="#e0e0f0" stroke-width="1.5"/>
  <text x="390" y="34" text-anchor="middle" font-family="Manrope,sans-serif" font-size="13" font-weight="700" fill="#1a1a2e">Edge Node</text>

  <rect x="490" y="10" width="130" height="38" rx="4" fill="#f8f8fc" stroke="#e0e0f0" stroke-width="1.5"/>
  <text x="555" y="34" text-anchor="middle" font-family="Manrope,sans-serif" font-size="13" font-weight="700" fill="#1a1a2e">Global Core</text>

  <!-- Lifelines -->
  <line x1="75"  y1="48" x2="75"  y2="233" stroke="#e0e0f0" stroke-width="1" stroke-dasharray="4,3"/>
  <line x1="230" y1="48" x2="230" y2="233" stroke="#e0e0f0" stroke-width="1" stroke-dasharray="4,3"/>
  <line x1="390" y1="48" x2="390" y2="233" stroke="#e0e0f0" stroke-width="1" stroke-dasharray="4,3"/>
  <line x1="555" y1="48" x2="555" y2="233" stroke="#e0e0f0" stroke-width="1" stroke-dasharray="4,3"/>

  <!-- write "worker": Client -> OPFS Worker -->
  <line x1="79" y1="90" x2="226" y2="90" stroke="rgba(20,106,255,0.45)" stroke-width="1.5" marker-end="url(#aw)"/>
  <circle cx="230" cy="90" r="5" fill="rgba(20,106,255,0.45)" stroke="#fff" stroke-width="1.5"/>
  <line x1="235" y1="90" x2="386" y2="90" stroke="#e0e0f0" stroke-width="1" stroke-dasharray="4,3"/>
  <line x1="394" y1="90" x2="551" y2="90" stroke="#e0e0f0" stroke-width="1" stroke-dasharray="4,3"/>
  <text x="152" y="82" text-anchor="middle" font-family="JetBrains Mono,monospace" font-size="10.5" fill="rgba(20,106,255,0.6)">db.insert({tier:"worker"})</text>

  <!-- write "edge": Client -> Edge Node -->
  <line x1="79" y1="135" x2="386" y2="135" stroke="rgba(20,106,255,0.7)" stroke-width="1.5" marker-end="url(#ae)"/>
  <circle cx="390" cy="135" r="5" fill="rgba(20,106,255,0.7)" stroke="#fff" stroke-width="1.5"/>
  <line x1="395" y1="135" x2="551" y2="135" stroke="#e0e0f0" stroke-width="1" stroke-dasharray="4,3"/>
  <text x="232" y="127" text-anchor="middle" font-family="JetBrains Mono,monospace" font-size="10.5" fill="rgba(20,106,255,0.8)">db.insert({tier:"edge"})</text>

  <!-- write "global": Client -> Global Core -->
  <line x1="79" y1="180" x2="551" y2="180" stroke="#146aff" stroke-width="1.5" marker-end="url(#ag)"/>
  <circle cx="555" cy="180" r="5" fill="#146aff" stroke="#fff" stroke-width="1.5"/>
  <text x="316" y="172" text-anchor="middle" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#146aff">db.insert({tier:"global"})</text>

  <!-- Divider -->
  <line x1="12" y1="196" x2="628" y2="196" stroke="#e0e0f0" stroke-width="1.5"/>

  <!-- useAll(q): streams back from Global Core as writes propagate -->
  <line x1="551" y1="220" x2="83" y2="220" stroke="#146aff" stroke-width="1.5" stroke-dasharray="5,3" marker-end="url(#as)"/>
  <!-- flow chevrons -->
  <polyline points="468,215 462,220 468,225" stroke="#146aff" stroke-width="1.5" fill="none"/>
  <polyline points="368,215 362,220 368,225" stroke="#146aff" stroke-width="1.5" fill="none"/>
  <polyline points="268,215 262,220 268,225" stroke="#146aff" stroke-width="1.5" fill="none"/>
  <polyline points="168,215 162,220 168,225" stroke="#146aff" stroke-width="1.5" fill="none"/>
  <text x="316" y="212" text-anchor="middle" font-family="JetBrains Mono,monospace" font-size="10.5" fill="#146aff">useAll(q): re-renders as writes propagate</text>
</svg>

---

## Immediate writes: the SyncManager

Game callbacks fire Jazz writes directly. `SyncManager` owns every write and uses a `releasingIds` guard to prevent double-releasing the same deposit across concurrent async operations.

**[`src/jazz/SyncManager.ts`](../src/jazz/SyncManager.ts)**

```typescript
export class SyncManager {
  private collectedByThis = new Map<string, { fuelType: string; positionX: number }>();
  private releasingIds = new Set<string>(); // guards against double-release
  private dbRowId: string | null = null;

  constructor(
    private db: ReturnType<typeof useDb>,
    private playerId: string,
  ) {}

  collectDeposit(id: string) {
    /* db.update immediately */
  }
  refuel(fuelType: FuelType) {
    /* releaseDeposit - delete + insert */
  }
  shareFuel(fuelType: string, receiverPlayerId: string) {
    /* db.update */
  }
  sendMessage(text: string) {
    /* db.insertDurable with tier: "edge" */
  }
  updateState(state: PlayerInit) {
    /* db.updateDurable if changed */
  }
}
```

The game engine calls these methods synchronously. Writes that need durability guarantees use `insertDurable`/`updateDurable` with `tier: "edge"`. Hot-path writes like `collectDeposit` use eventually consistent `db.update` for instant local-store updates.

---

## Collecting a deposit

<div style="display:grid;grid-template-columns:3fr 2fr;gap:1.5rem;margin-top:0.4rem">
<div>

When the player walks over a fuel deposit, `collectDeposit` fires immediately:

```typescript
// src/jazz/SyncManager.ts - collectDeposit()
this.db.update(app.fuel_deposits, id, {
  collected: true,
  collectedBy: this.playerId,
});
```

This uses eventually consistent `db.update` so the local store updates instantly. The write still propagates to the server and fans out to every other client's `useAll(fuel_deposits.where({ collected: false }))` subscription, and the deposit disappears from their surface.

</div>
<div>
<img src="screenshots/03-player-walking.png" style="width:100%;border-radius:6px;box-shadow:0 2px 12px rgba(0,0,0,0.15)">
<blockquote>
<strong>Concurrent collect?</strong> Both writes go through with <code>collected: true</code>. Last-write-wins resolves <code>collectedBy</code> to whichever timestamp arrived at the edge later. One player wins, the other's collection is silently overwritten on sync. No locks, no errors.
</blockquote>
</div>
</div>

---

## Sharing fuel cross-client

When Player A gives a deposit to Player B, no new row is created. A updates `collectedBy`:

```typescript
// src/jazz/SyncManager.ts - shareFuel()
this.db.update(app.fuel_deposits, shareId, {
  collectedBy: receiverPlayerId,
});
```

Player B's `useAll(app.fuel_deposits.where({ collected: true }))` subscription already contains the row. The `collectedBy` update propagates as a plain row update, so Player B's inventory reflects the share immediately.

---

## Releasing a deposit: DELETE + INSERT

When a player refuels their lander, the deposit is returned to the surface. Rather than updating `collected: false` on the existing row, the code deletes it and inserts a fresh one:

```typescript
// src/jazz/SyncManager.ts - releaseDeposit()
this.db.delete(app.fuel_deposits, depId);
this.db.insert(app.fuel_deposits, {
  fuelType,
  positionX,
  createdAt: Math.floor(Date.now() / 1000),
  collected: false,
  collectedBy: "",
});
```

The eventually consistent `delete` + `insert` updates the local store immediately. The fresh INSERT is picked up by all clients' `where({ collected: false })` subscriptions, so the deposit reappears on everyone's surface.

---

## Player state sync

![bg right:38% 88%](screenshots/01-game-landed.png)

Every player's position, velocity, fuel level, and mode are written to the `players` table. SyncManager skips the write if nothing meaningful has changed, using configurable thresholds:

```typescript
// src/jazz/SyncManager.ts - updateState()
if (!this.dbRowId) return;
if (this.lastSynced && !playerStateChanged(this.lastSynced, state)) return;
this.lastSynced = { ...state };
this.db.updateDurable(app.players, this.dbRowId, state, { tier: "edge" });
```

Player insert happens once, after the edge subscription has settled:

```typescript
// src/jazz/SyncManager.ts - setInputs(), after settled
this.db.insertDurable(app.players, state, { tier: "edge" }).then((row) => {
  this.dbRowId = row.id;
});
```

Every other client's `useAll(app.players.where({ playerId: { ne: myId } }))` subscription updates automatically, keeping the remote lander in sync. (`ne` is Jazz's "not equal" operator, which excludes the local player's own row.)

---

## Jazz API surface used in Moon Lander

| API                                           | Used for                                                                     |
| --------------------------------------------- | ---------------------------------------------------------------------------- |
| `JazzProvider`                                | Wrap the app; handles WASM worker + OPFS + sync internally                   |
| `useDb()`                                     | Access the db write API from any component                                   |
| `useAll(query, tier?)`                        | Live subscription; re-renders on every remote or local change                |
| `db.insert(table, data)`                      | Eventually consistent insert. Instant local update, propagates in background |
| `db.update(table, id, data)`                  | Eventually consistent update. Instant local update, propagates in background |
| `db.delete(table, id)`                        | Eventually consistent delete. Used before re-inserting a released deposit    |
| `db.insertDurable(table, data, { tier })`     | Durable insert. Promise resolves when tier confirms                          |
| `db.updateDurable(table, id, data, { tier })` | Durable update. Used for player state sync at `"edge"` tier                  |

**Key insight:** the entire multiplayer state of a real-time game (positions, collectibles, inventory, chat) is managed with these eight API calls. No custom server, no WebSocket handlers, no conflict resolution code.
