# Moon Lander

A multiplayer moon lander game built with React and [Jazz](https://jazz.tools). Players descend onto a shared lunar surface, collect fuel deposits, share resources with nearby astronauts, and launch back into space.

Jazz handles all the real-time sync: player positions, fuel deposits, inventory, and chat messages propagate between clients through a local-first relational database with no custom networking code.

## Running locally

You need two terminals: one for the Jazz sync server and one for the Vite dev server.

```bash
# Terminal 1: Jazz server (port 4200)
pnpm dev:server

# Terminal 2: Vite dev server
pnpm dev
```

Open the URL Vite prints (usually `http://localhost:5173`) in two browser windows to see multiplayer in action.

## How the game works

The game progresses through a series of modes:

```
  start
    |  Space
    v
  descending          Thrust with arrow keys to control descent.
    |                 Fuel burns while thrusting.
    v
  landed              Safely on the surface (velocity <= 80 px/s).
    |  E
    v
  walking             Explore the moon. Collect fuel deposits.
    |  E (near lander)
    v
  in_lander           Correct fuel type auto-refuels the lander.
    |  Space (fuel >= 100)
    v
  launched            Back to space.
```

If you hit the ground too fast, you crash and can restart with Space.

### Fuel and deposits

Each player is assigned a required fuel type (one of seven shapes: circle, triangle, square, pentagon, hexagon, diamond, octagon). Deposits of all types are scattered across the 9600px-wide lunar surface. Walking over a deposit picks it up (one of each type max). Entering the lander with the correct type refuels it; all other types are ejected back onto the surface.

### Multiplayer

When two walking players are near each other (within 80px), fuel the giver doesn't need is automatically shared with the receiver who does need it. A proximity hint appears when another player is nearby but not quite close enough.

All state syncs through Jazz: player positions, deposit collection, inventory changes, and chat messages. Each client runs the full physics simulation locally and writes snapshots to the database every 200ms.

### Chat

Press Enter while walking or landed to open the chat input. Type a message and press Enter to send, or Escape to cancel.

## Project structure

```
src/
  main.tsx                Entry point. JWT signing, Jazz config, React root.
  App.tsx                 JazzProvider wrapper. Bridges Game with Jazz subscriptions.
  Game.tsx                Main component. Canvas, HUD, chat input, data attributes.

  game/
    constants.ts          Physics, colours, fuel types, canvas dimensions.
    engine.ts             GameEngine class and useGameEngine hook. Owns the RAF loop.
    physics.ts            updatePhysics(). Gravity, thrust, landing, collection, sharing.
    scene.ts              renderScene(). Draws entities, overlays, proximity hints.
    terrain.ts            Moon surface, curvature, parallax starfield.
    sprites.ts            Pixel-art sprite drawing (lander, astronaut, deposits).
    particles.ts          Particle system for thrust, collection, and burst effects.
    inventory.ts          mergeInventory(). Reconciles Jazz props with local state.
    player.ts             Identity: getOrCreatePlayerId(), initPlayerProps().
    world.ts              Deposit generation, world wrapping helpers.
    overlays.ts           Full-screen messages (crash, launch, velocity warning).
    types.ts              EngineState, EngineProps, GameWorld, Deposit, etc.
    Hud.tsx               HUD component (fuel gauge, player list, deposit icons).

  sync/
    useSyncLoop.ts        Polls engine state, writes to Jazz every 200ms.
    useDeposits.ts        Deposit subscription and reconciliation.
    writes.ts             DB write helpers (playerStateChanged, reconcileDeposits).

schema/
  current.ts              Table definitions (players, fuel_deposits, chat_messages).
  app.ts                  Auto-generated TypeScript interfaces and query builders.
```

## Schema

Three tables, all synced through Jazz:

| Table           | Key columns                                                                                        |
| --------------- | -------------------------------------------------------------------------------------------------- |
| `players`       | playerId, name, color, mode, positionX/Y, velocityX/Y, requiredFuelType, landerFuelLevel, lastSeen |
| `fuel_deposits` | fuelType, positionX, collected, collectedBy, createdAt                                             |
| `chat_messages` | playerId, message, createdAt                                                                       |

Players older than 180 seconds (based on `lastSeen`) are filtered out as stale.

## Tests

Browser-based E2E tests run in Chromium via `@vitest/browser` and Playwright:

```bash
pnpm test
```

The test suite covers:

- **Phase 1** (15 tests): Canvas rendering, descent physics, thrust, landing, walking, lander re-entry.
- **Phase 2** (16 tests): Player identity, Jazz multiplayer sync, remote player rendering, deposit sync across instances.
- **Phase 3** (16 tests): Fuel deposits, thrust fuel burn, collection, inventory, refuelling, launch.
- **Phase 4** (12 tests): Proximity fuel sharing, share guards, share hints, inventory burst on lander entry.
- **Phase 5** (4 tests): Chat input open/close, message sending, game key suppression while chatting.
- **Phase 5b** (4 tests): Walking jump (Space/W), jump height, no double-jump.
- **Writes** (18 tests): Unit tests for `playerStateChanged` and `reconcileDeposits` pure functions.

Total: 85 tests.

## Build

```bash
pnpm build
```

Produces a production bundle targeting ES2020. The Jazz WASM runtime is loaded as an ES module.
