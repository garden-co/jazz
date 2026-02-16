# Moon Lander — Cooperative Multiplayer Example App - TO DO

A real-time multiplayer game where players help each other escape the moon by sharing fuel. **Perfect launch hero example** — visually striking (synthwave/32-bit aesthetic), demonstrates real-time sync and elegant cooperative gameplay.

## Overview

Players start in space with enough fuel to land on the moon, but not enough to escape. They must:
1. **Descend** — controlled landing on moon surface
2. **Explore** — walk around to find fuel deposits
3. **Cooperate** — automatically share fuel when walking past each other
4. **Escape** — return to lander and launch to orbital

The game demonstrates Jazz's core strengths:
- **Real-time collaboration** — see other players move, collect fuel, send messages instantly
- **Smart cooperative mechanic** — automatic fuel sharing showcases sync elegance
- **Local-first gameplay** — responsive controls, play offline until others join
- **Presence & identity** — chat bubbles, fuel inventory above heads

## Game Mechanics

### Core Gameplay Loop

1. **Start in space** — Player spawns above the moon in their lander
2. **Descend** — Use thrust to land safely (have just enough fuel)
3. **Exit lander** — Walk on moon surface as astronaut (2D infinite scrolling platformer)
4. **Find fuel** — Collect fuel deposits scattered on the moon surface
   - Each fuel type is a different shape (triangle, square, pentagon, hexagon, heptagon, octagon, circle)
   - Each player's lander requires a specific fuel type
5. **Auto-share** — When walking past another player:
   - If they need a fuel type you have, they automatically collect it
   - If they don't need it, you keep it
6. **Return** — Walk back to your lander with the correct fuel
7. **Refuel** — Transfer collected fuel to lander
8. **Launch** — Escape moon's gravity and reach orbital

**Note:** The game is **never-ending** — players continuously spawn, collect fuel, and launch. The moon surface grows as more players join.

### Fuel Mechanic Details

**Fuel Types** (represented as shapes):
- Circle (1-sided, well, a circle)
- Triangle (3-sided)
- Square (4-sided)
- Pentagon (5-sided)
- Hexagon (6-sided)
- Heptagon (7-sided)
- Octagon (8-sided)

**World Generation:**
- **Initial seed:** 3 deposits of each fuel type distributed pseudo-randomly across the moon surface (with decent spacing)
- **Dynamic spawning:** When a new player spawns, a new deposit of their required fuel type is added to the world
- **Moon surface size:** Configurable (env var `MOON_SURFACE_WIDTH`). Default: wide enough for ~5 players to have a full screen of space each before overlap
- **Terrain:** Flat surface from a physics perspective (visual craters in foreground/background can be added later)
- **Required fuel spawns far away:** Each player's guaranteed deposit is placed 1/4–1/2 of `MOON_SURFACE_WIDTH` away from their spawn point. This forces walking across the surface, increasing the chance of encountering other players.

**Deposit Visibility (decay):**
- Clients only subscribe to the most recent **3 + N** uncollected deposits of each fuel type, where N is the number of currently active players whose `requiredFuelType` matches that type
- Older deposits aren't deleted — they simply fall outside the subscription window and become invisible
- This prevents the world from growing unboundedly as players join, collect, scatter, and leave
- The base of 3 ensures a solo player always has fuel to find; the +N scales with demand

**Fuel Matching:**
- Each lander requires exactly ONE fuel type (randomly assigned at spawn)
- Fuel deposits on the moon are random shapes
- Players collect all fuel they walk over, regardless of type
- **Inventory cap:** Players hold at most 1 unit of each fuel type. Walking over a deposit you already have does nothing.
- When two players walk past each other, fuel transfers automatically:
  - Player A needs triangle fuel
  - Player B has triangle fuel in inventory
  - Player A automatically "collects" the triangle fuel from Player B
  - **BUT**: If Player B ALSO needs triangle fuel, NO transfer happens (no stealing!)
- **Proximity sharing (two radii):** At 2x interact radius, show "move closer to share fuel" hint. At 1x interact radius, transfer happens automatically — no key press, no cooldown. Only fuel you don't need is given.
- **One-way giving:** Each client only gives its own fuel away, never takes. This avoids dual-write races.

**Refuelling:**
- Each correct fuel unit refuels the lander by +100 (capped at max capacity 100)
- Since landing consumes most of the initial 40 units, a single deposit of the correct type is sufficient to fully refuel and launch
- **Launch scatter:** When a player launches, unneeded inventory fuel is ejected as new `fuel_deposit` rows scattered on the moon surface. This recycles fuel back into the world.

**Inventory Display:**
- Icon above player's head shows collected fuel types
- Visual: small shape icons floating above astronaut
- Greyed out = need but don't have
- Coloured = collected

**Thrust & Fuel Burn:**
- Thrusting consumes fuel from `landerFuelLevel`
- Vertical thrust (up) burns at `FUEL_BURN_Y = 8` units/sec; horizontal (left/right) at `FUEL_BURN_X = 4` units/sec
- When fuel hits 0, thrust is disabled — gravity takes over
- A sloppy descent wastes fuel; the player may crash land

### Chat System

- Text input box (bottom of screen)
- Messages appear as speech bubbles above the sender's astronaut (attached to player, move with them)
- Bubbles fade after ~5 seconds
- Real-time sync — all players see messages instantly
- **Persistence:** All messages stored in DB (cheap), but clients only subscribe to recent messages (last ~10 seconds)

### Win Condition

Each player must:
1. Find their lander's specific fuel type (or receive it from another player)
2. Return to their lander
3. Launch successfully to orbital

All players can win independently — it's cooperative, not competitive.

## Schema Design

**Important:** The jazz-ts DSL supports `col.int()` (i32), `col.string()`, `col.boolean()`, and `col.ref()`. A `col.float()` helper exists but is **non-functional** — the entire stack lacks a float/f64 type (the codegen maps REAL → Integer, the Rust `Value` enum has no float variant, and the SQL parser rejects `REAL`). All numeric columns must use **integers**: positions and velocities use **fixed-point integers** (multiply by 100), timestamps are **Unix seconds** (not milliseconds) to fit i32.

```typescript
table("players", {
  name: col.string(),
  color: col.string(), // Hex colour for their astronaut/lander

  // State
  mode: col.string(), // "descending" | "landed" | "walking" | "in_lander" | "launched"
  online: col.boolean(),
  lastSeen: col.integer(), // Unix timestamp in SECONDS (i32 limit)

  // Position — fixed-point integers (pixel * 100)
  positionX: col.integer(), // e.g. 50000 = 500.00 pixels
  positionY: col.integer(),
  velocityX: col.integer(), // fixed-point velocity
  velocityY: col.integer(),

  // Lander requirements
  requiredFuelType: col.string(), // "circle" | "triangle" | "square" | "pentagon" | "hexagon" | "heptagon" | "octagon"
  landerFuelLevel: col.integer(), // 0-100, starts at ~40 (enough to land, not launch)
  landerSpawnX: col.integer(), // Where this player's lander landed (fixed-point)

  // Auth
  userId: col.string(),
});

table("player_inventory", {
  playerId: col.ref("players"),
  fuelType: col.string(), // "circle" | "triangle" | "square" | etc.
  quantity: col.integer(),
});

table("fuel_deposits", {
  fuelType: col.string(), // "circle" | "triangle" | etc.
  positionX: col.integer(), // Fixed-point — only X coord, deposits are always on the ground (Y = GROUND_LEVEL)
  createdAt: col.integer(), // Unix timestamp (seconds) — used for deposit decay ordering
  collected: col.boolean(),
  collectedBy: col.ref("players").optional(),
  collectedAt: col.integer().optional(), // Unix timestamp (seconds)
});

table("messages", {
  senderId: col.ref("players"),
  text: col.string(),
  timestamp: col.integer(), // Unix timestamp (seconds)
  // No position — messages are attached to the player, rendered above their current position
});
```

**Schema Notes:**
- **Separate `player_inventory` table** — Enables querying "who nearby has fuel I need?" for fuel-sharing logic. Also supports future features like directional arrows pointing to fuel sources.
- **Uses `col.ref()`** — Demonstrates relational patterns (ref to players), which existing examples don't showcase well (see `codegen_relations_demo.md`). Ref columns support `where({ playerId: { eq: someId } })` filtering.
- **No `game_config` table** — Game constants (gravity, escape velocity, ground level) hardcoded in `constants.ts`
- **Fuel deposits have no Y coord** — Always on surface (Y = GROUND_LEVEL constant)
- **Messages have no position** — Follow the player who sent them

## UI Layout

### Game Canvas (Full Screen)

**Descent Phase:**
- Deep space background (stars, Earth in distance)
- Moon surface at bottom
- Player's lander with thrust particles
- Fuel gauge, velocity indicator

**Walking Phase:**
- Moon surface (procedural craters, rocks — 32-bit style)
- Astronauts (player + others) — small pixel-art sprites
- Fuel deposits — glowing shapes scattered around
- Chat bubbles above astronauts
- Fuel inventory icons above each player's head
- **Synthwave colours**: Pinks, purples, cyans, neon outlines

### HUD Elements

**Top Left:**
- Your lander's required fuel type (large icon)
- "Need: [hexagon icon]"

**Top Right:**
- Other players online
- Their required fuel types
- Their status (walking, in lander, launched)

**Bottom:**
- Chat input box
- "Press E to enter lander" (when near it)
- "Press SPACE to launch" (when in lander with fuel)

**Above Your Head:**
- Your collected fuel inventory (shape icons with quantities)
- Greyed out if you need it but don't have it
- Coloured (synthwave hues) if collected

## Controls

### Descent Phase
- **Arrow keys / WASD** — Thrust (left, right, up)
- Goal: Land gently on moon surface

### Walking Phase
- **Arrow keys / WASD** — Walk left/right
- **E** — Enter lander (when nearby)
- **Enter** — Send chat message

### Lander Phase (After Refuelling)
- **Space** — Launch (if have required fuel)

## Technical Architecture

This is a demonstration of **Jazz**, a database which allows building rich, realtime collaborative applications. Separate concerns: physics and rendering are secondary to the main goal. Abstract game engine concerns into modules users don't need to inspect. Wherever we interact with Jazz, avoid large monolithic components — import non-Jazz-specific code instead. Users will primarily want to read the Jazz-specific source.

### Frontend Stack

- **React** + Canvas API for game rendering
- **jazz-react** — Hooks for game state subscriptions (`useDb()`, `useAll()`)
- **Simple 2D physics** — Custom (gravity, thrust, walking)
- **Pixel art sprites** — 32-bit astronauts, landers, fuel shapes
- **Synthwave palette** — Predefined colour scheme

### Key Jazz Pattern: Refs for Physics, DB on a Timer

**Critical constraint:** `db.update()` triggers `useAll()` subscriptions → React re-renders → frame drops. Never write to DB from the animation frame.

```typescript
// Physics state lives in refs (60fps, no re-renders)
const positionXRef = useRef(0);
const velocityYRef = useRef(0);

// Game loop — pure refs, no DB or React state
useEffect(() => {
  const gameLoop = () => {
    positionXRef.current += velocityXRef.current * deltaTime;
    requestAnimationFrame(gameLoop);
  };
  requestAnimationFrame(gameLoop);
}, []);

// DB persistence — standalone timer, completely decoupled
useEffect(() => {
  const interval = setInterval(() => {
    db.update(app.players, playerId, {
      positionX: Math.floor(positionXRef.current),
      positionY: Math.floor(positionYRef.current),
    });
  }, 200); // Every 200ms
  return () => clearInterval(interval);
}, [db, playerId]);

// Subscribe to all players for rendering
const allPlayers = useAll(app.players);
```

### Subscription Strategy & DB Thrashing

The query builder supports range operators (`gt`, `gte`, `lt`, `lte`) so spatial queries like `app.fuel_deposits.where({ positionX: { gte: minX, lte: maxX }, collected: false })` are valid.

However, `useAll()` re-subscribes whenever `query._build()` returns a new string (referential equality). For predicates that change frequently — viewport bounds shift as the player moves, `lastSeen` staleness thresholds change every second — pushing them into a where-clause would churn subscriptions.

**Strategy: split stable vs volatile predicates.**
- **Stable predicates** → WASM where-clause (e.g. `{ online: { eq: true } }`, `{ collected: false }` — subscribe once)
- **Volatile predicates** → JS `.filter()` (e.g. viewport bounds, `lastSeen` threshold)
- **Slow-changing predicates** → WASM where-clause with `.limit()` — OK to re-subscribe infrequently (e.g. fuel deposit decay, where the limit only changes when a player joins/leaves)

```typescript
// One stable subscription — never re-subscribes
const allPlayers = useAll(app.players.where({ online: { eq: true } }));

// Volatile filtering in JS
const staleThreshold = Math.floor(Date.now() / 1000) - 180;
const nearby = allPlayers.filter(
  (p) => p.id !== myPlayerId &&
    p.lastSeen >= staleThreshold &&
    p.positionX >= minX && p.positionX <= maxX
);
```

**Fuel deposit subscriptions (decay).** One subscription per fuel type (7 total). The limit is 3 + N where N = active players needing that type. This only re-subscribes when the player count for that type changes — infrequent.

```typescript
// Per fuel type — limit changes only when players join/leave
const FUEL_TYPES = ["circle", "triangle", "square", "pentagon", "hexagon", "heptagon", "octagon"];

// Count active players per required fuel type (derived from allPlayers)
const playersPerType = useMemo(() => {
  const counts: Record<string, number> = {};
  for (const t of FUEL_TYPES) counts[t] = 0;
  for (const p of allPlayers) counts[p.requiredFuelType]++;
  return counts;
}, [allPlayers]);

// One subscription per type, each with a slow-changing limit
const triangleDeposits = useAll(
  app.fuel_deposits
    .where({ fuelType: { eq: "triangle" }, collected: false })
    .orderBy("createdAt", "desc")
    .limit(3 + playersPerType["triangle"])
);
// ... repeat for each type, or build dynamically
```

**⚠ Watch: player position DB writes.** Position updates every 200ms means every nearby player triggers a DB write on their own timer. With N players, this is N×5 writes/sec, each of which triggers subscription callbacks. This is likely fine for small player counts (~10–20), but could cause performance issues at scale. If we see DB thrashing, options include: increasing the sync interval, only writing on meaningful position change (delta threshold), or batching updates. Flag during implementation and profile.

### Rendering Remote Players: Dead Reckoning

Physics runs locally at 60fps; the DB syncs position + velocity every 200ms. Between syncs, other clients have no knowledge of a remote player's inputs. **Dead reckoning** bridges the gap: extrapolate from the last known state using stored velocity and known gravity.

```
timeSinceSync = now - player.lastSeen
extrapolatedY = dbY + dbVelocityY * t + 0.5 * GRAVITY * t²
extrapolatedX = dbX + dbVelocityX * t
```

When a fresh DB update arrives, smoothly lerp toward the corrected position rather than snapping (avoid visual jitter).

**Why this works well enough:**
- **Descent is predictable** — gravity is constant. The only unknown is whether they're currently thrusting, and thrust is intermittent. Between thrusts the extrapolation is exact.
- **Walking is trivial** — flat surface, constant speed, no gravity. Linear extrapolation from velocity is near-perfect.
- **No competitive stakes** — this is a cooperative game with LWW conflict resolution. Small position discrepancies between what player A sees of player B vs reality are cosmetic. The gameplay-critical proximity check (fuel sharing) runs on each client independently against their own local position.

### Query Builder Reference

Supported where-clause operators (verified against current codebase):

| Column Type | Operators |
|---|---|
| Integer/BigInt | `eq`, `ne`, `gt`, `gte`, `lt`, `lte` |
| Text | `eq`, `ne` (note: `contains` is generated but not implemented in Rust) |
| Boolean | direct value only (e.g. `{ online: true }`) |
| UUID / Ref | `eq`, `ne`; nullable refs also support `isNull` |
| ID | `eq`, `ne`, `in` (note: `in` not implemented in Rust) |

Also supported: `.orderBy(column, "asc" | "desc")`, `.limit(n)`, `.offset(n)`, `.include({ relation: true })`.

## Visual Aesthetic — Synthwave / 32-bit

### Colour Palette

**Synthwave Neon:**
- Primary pink: `#ff00ff` (magenta)
- Primary cyan: `#00ffff` (cyan)
- Accent purple: `#8b00ff`
- Accent yellow: `#ffff00`
- Dark background: `#0a0a0f` (near-black blue)
- Ground: `#2a1a3a` (dark purple-gray)

**Fuel Shape Colours** (neon):
- Circle: Cyan (`#00ffff`)
- Triangle: Magenta (`#ff00ff`)
- Square: Yellow (`#ffff00`)
- Pentagon: Green (`#00ff00`)
- Hexagon: Orange (`#ff6600`)
- Heptagon: Pink (`#ff66ff`)
- Octagon: Purple (`#8b00ff`)

### Sprite Style

- **32-bit pixel art** — 32x32 or 64x64 sprites
- **Neon outlines** — Bright coloured borders around sprites
- **Glow effects** — Fuel deposits, lander thrust, player outlines
- **Scanlines** (optional) — CRT-style overlay for retro feel

### Animation

- **Lander thrust** — Particle effects (pink/cyan pixels shooting down)
- **Walking** — Simple 2-frame walk cycle
- **Fuel collection** — Shape zooms toward player, sparkle effect
- **Fuel transfer** — Shape floats from one player to another
- **Chat bubbles** — Fade in, hold, fade out

## Demo Value

### Visual Appeal

- **Synthwave aesthetic** — Instantly recognisable, looks great in screenshots/GIFs
- **Real-time multiplayer magic** — See other players move, collect, share fuel
- **Automatic cooperation** — Fuel transfers showcase Jazz sync elegance

### GIF/Video Moments

1. **Opening**: "Player descends onto moon, lands successfully"
2. **Exploration**: "Two players walking, collecting different coloured fuel shapes"
3. **The Magic Moment**: "Players walk past each other, a shape icon floats from one to the other (automatic transfer!)"
4. **Chat**: "Speech bubble appears: 'Thanks for the fuel!'"
5. **Victory**: "Player returns to lander, launches with pink/cyan thrust trail, reaches orbital"

### Talking Points

- "Real-time multiplayer with zero network lag"
- "Cooperative gameplay emerges naturally from simple rules"
- "Works offline — play solo, others join and sync seamlessly"
- "Beautiful synthwave aesthetic, runs smoothly in browser"

## Implementation Phases

### Phase 1: Solo Landing & Walking

- [x] Basic schema (players, fuel_deposits, player_inventory, messages)
- [x] Canvas rendering, parallax starfield background
- [x] Lander sprite with thrust physics (descent)
- [x] Land on moon surface
- [x] Exit lander (E key), walk as astronaut (A/D), re-enter lander when near
- [x] Parked lander remains visible while walking
- [x] Test: "Can I land and walk around?"

### Phase 2: Multiplayer Basics

- [x] Jazz sync server
- [x] Unique dbName per tab for OPFS lock avoidance
- [x] Player creation with stable identity (localStorage playerId, deterministic name)
- [x] Player presence (online/offline heartbeat every 3s)
- [x] Real-time position sync (DB persistence every 200ms via server)
- [x] Render other players' landers and astronauts
- [x] Test: "Can two players see each other descend and land?"

### Phase 3: Fuel Collection

- [x] Spawn random fuel shapes on moon surface (3 per type + 1 of player's required type 1/4–1/2 world away)
- [x] Thrust burns fuel (FUEL_BURN_Y=8/sec vertical, FUEL_BURN_X=4/sec horizontal; disabled at 0)
- [x] Collection mechanic (walk over deposit to collect, 1 per type max)
- [x] Inventory display in HUD (need/bag)
- [x] Return to lander and refuel (auto-transfer matching fuel on re-enter, +100 per unit, capped at 100)
- [x] Launch mechanic (Space key when in lander with fuel >= 100)
- [x] World wrapping (walking/flying off one edge loops to the other)
- [x] Test: "Can I collect fuel, return, and launch?" (17/17 green)
- [ ] Render fuel deposits on canvas (currently data-only)
- [ ] Launch animation (lander flies upward)

### Phase 4: Automatic Fuel Sharing

- [ ] Proximity hint: at 2x interact radius, show "move closer to share fuel" if sharing is possible
- [ ] Auto-share: at 1x interact radius, transfer fuel the other player needs (and you don't)
  - Both players must be walking
  - No cooldown — transfer is immediate on proximity
- [ ] Visual: shape icon floats from giver to receiver
- [ ] Launch scatter: when a player launches, unneeded inventory fuel explodes back onto the moonscape as new deposits
- [ ] Test: "Walk past another player, fuel transfers correctly"

### Phase 5: Chat & Polish

- [ ] Chat input UI
- [ ] Speech bubbles above players
- [ ] Test: "Send message, appears above head for 5 seconds"

### Phase 6: Synthwave Aesthetic

- [ ] Replace placeholder sprites with 32-bit pixel art
- [ ] Apply synthwave colour palette
- [ ] Add glow effects (fuel, thrust, outlines)
- [ ] Particle effects (thrust, collection sparkles)
- [ ] Background (Earth, stars, gradient)

### Phase 7: Demo Assets

- [ ] Record GIF/video of key moments
- [ ] Landing page with "How to Play" instructions
- [ ] E2E browser tests (two players, full gameplay loop)
- [ ] Deploy to public URL

## Design Constraints

### No working float type

The entire Groove stack lacks f64 support: the codegen (`packages/jazz-ts/src/codegen/schema-reader.ts`) maps REAL → Integer, the Rust `Value`/`ColumnType` enums have no float variant, and the SQL parser rejects REAL. All numeric values must fit i32 (max ~2.1 billion). Consequences:
- Positions/velocities: fixed-point ×100 integers
- Timestamps: Unix seconds (not ms — ms overflows i32; seconds work until 2038)

### OPFS exclusive file lock (unique dbName per tab)

OPFS `createSyncAccessHandle()` grants an exclusive lock. Two tabs with the same `dbName` fight over it. Workaround: each tab generates a unique `dbName` via `sessionStorage`. Tabs sync through the server; no offline cross-tab. Proper fix is multi-tab leader election (see `specs/todo/b_mvp/multi_tab_leader_election.md`). Flag in code with `// TODO: multi-tab leader election`.

### JWT auth for sync server

The Jazz sync server requires JWT authentication. The browser generates a JWT using `crypto.subtle` HMAC-SHA256, signed with a shared secret. Dev-only; production would use proper auth.

### Camera

**Horizontal:** Camera X follows the player, centred on screen. Rounded to whole pixels to avoid sub-pixel jitter in the starfield and ground line.

**Vertical:** Camera Y also follows the player during descent (centred vertically). After landing, camera locks so the ground sits near the bottom of the viewport — ensuring the moon surface is always visible regardless of screen height. This makes the game playable on short viewports (mobile, small browser windows) without clipping the ground off-screen.

**GROUND_LEVEL is a world coordinate, not a screen coordinate.** The camera transform converts it to screen space. On tall viewports the ground appears higher up with more space below; on short viewports it's near the bottom edge.

### Parallax starfield

Three depth layers (parallax speeds 0.05, 0.15, 0.3) give motion feedback during the long descent. ~200 pseudo-random stars with deterministic positions (seeded by index) wrap around screen edges.

## Open Questions — RESOLVED

- **Physics tick rate** — 60fps locally, DB sync every 200ms (not in animation frame)
- **Fuel deposit generation** — Initial seed of 3 per type, spawn new deposit when player joins (their required fuel type)
- **Terrain** — Flat surface (physics), visual craters later
- **Max players** — No hard limit. Spatial subscriptions scale, but watch for DB thrashing at high counts (see Subscription Strategy above)
- **Fuel conflict resolution** — Last write wins (LWW)
- **Chat persistence** — Store all messages (cheap), subscribe to recent only (~10 seconds)
- **Moon surface size** — Fixed game world width (`MOON_SURFACE_WIDTH` = 9600px default, ~5 screens). Each player sees a viewport-sized slice based on their device (desktop/mobile responsive)

## Success Criteria

Two players can:
1. Descend and land on moon independently
2. See each other walking in real-time
3. Collect fuel deposits
4. Walk past each other and automatically share fuel
5. Send chat messages that appear above their heads
6. Return to landers, refuel, and launch successfully

Works offline:
- Player can play solo, others join later and sync

Demo-ready:
- Synthwave aesthetic looks polished
- Smooth 60fps gameplay
- Recordable GIF showing "automatic fuel transfer magic moment"

## File Structure

**Code style:** Keep components and utility modules small (~150 lines max each). Isolate game mechanics from Jazz/DB concerns — a module should either deal with physics/rendering or with data sync, not both.

```
examples/moon-lander-react/
├── schema/
│   ├── app.ts              # Auto-generated: types, query builders, wasmSchema
│   ├── current.ts          # Schema DSL (players, fuel_deposits, messages)
│   └── current.sql         # Generated SQL
├── src/
│   ├── main.tsx            # App entry point
│   ├── App.tsx             # JazzProvider wrapper (JWT auth, unique dbName per tab)
│   ├── Game.tsx            # Top-level game component: wires hooks together, renders HUD
│   ├── GameCanvas.tsx      # Canvas rendering (lander, starfield, ground, nearby players)
│   ├── hooks/
│   │   ├── usePlayer.ts    # Get-or-create player record (Jazz)
│   │   ├── usePresence.ts  # Online/offline heartbeat (Jazz)
│   │   ├── useGameLoop.ts  # requestAnimationFrame loop, owns physics refs
│   │   └── useDbSync.ts    # Periodic DB persistence (decoupled from game loop)
│   ├── game/
│   │   ├── constants.ts    # Game balance, colours, fuel types
│   │   ├── physics.ts      # Pure functions: gravity, thrust, collision
│   │   ├── fuel.ts         # Collection, sharing, inventory logic
│   │   └── render.ts       # Canvas drawing helpers (lander, astronaut, starfield)
│   └── utils/
│       └── jwt.ts          # Browser-side JWT generation (dev auth)
├── package.json
├── vite.config.ts
└── index.html
```

## Related Specs

- `example_apps.md` — This is a **launch hero example**
- `supported_use_cases.md` — Demonstrates collaborative, local-first (creative/gaming tools)
- `benchmarks_and_performance.md` — Real-time position updates stress-test sync
- `minimal_react_bindings.md` — Will use `jazz-react` extensively

## Notes

- **TDD: red then green** — Write E2E tests for each phase first
- Start with **ugly placeholders** (coloured rectangles) for sprites, polish aesthetic in Phase 6
- The **automatic fuel sharing mechanic** is the star — make sure it's visually clear when it happens (animation!)
- Synthwave aesthetic is a competitive advantage — makes the demo memorable and shareable
