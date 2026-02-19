import {
  CANVAS_WIDTH,
  GRAVITY,
  THRUST_POWER,
  THRUST_POWER_X,
  MAX_LANDING_VELOCITY,
  WALK_SPEED,
  LANDER_INTERACT_RADIUS,
  MAX_FUEL,
  REFUEL_AMOUNT,
  FUEL_BURN_Y,
  FUEL_BURN_X,
  ASTRONAUT_WIDTH,
  SHARE_PROXIMITY_RADIUS,
  GROUND_LEVEL,
  INITIAL_ALTITUDE,
  INITIAL_FUEL,
  JUMP_VELOCITY,
  JUMP_GRAVITY,
  type FuelType,
} from "./constants.js";
import { wrapX, wrapDistance } from "./world.js";
import type {
  GameWorld,
  InputSnapshot,
  PhysicsCallbacks,
  ArcAnimation,
  Deposit,
  RemotePlayerView,
} from "./types.js";

// ---------------------------------------------------------------------------
// Physics context — mutable state the physics step reads and writes
// ---------------------------------------------------------------------------

export interface PhysicsContext {
  dt: number;
  requiredFuelType: FuelType;
  deposits: Deposit[];
  collectedIds: Set<string>;
  inventory: Set<FuelType>;
  optimisticInventory: Set<FuelType>;
  sharedOut: Set<FuelType>;
  remotePlayers: RemotePlayerView[];
  arcs: ArcAnimation[];
  callbacks: PhysicsCallbacks;
  collectEffects: Array<{ x: number; fuelType: FuelType; isRequired: boolean; burst?: boolean }>;
}

export interface PhysicsResult {
  thrusting: boolean;
  thrustLeft: boolean;
  thrustRight: boolean;
}

// ---------------------------------------------------------------------------
// updatePhysics — one simulation step, mutates world in place
// ---------------------------------------------------------------------------

export function updatePhysics(
  world: GameWorld,
  input: InputSnapshot,
  ctx: PhysicsContext,
): PhysicsResult {
  const {
    dt,
    requiredFuelType,
    deposits,
    collectedIds,
    inventory,
    optimisticInventory,
    sharedOut,
    remotePlayers,
    arcs,
    callbacks,
    collectEffects,
  } = ctx;

  // Start screen: wait for Space to begin
  if (world.mode === "start") {
    inventory.clear();
    optimisticInventory.clear();
    sharedOut.clear();
    collectedIds.clear();
    if (input.launch) {
      world.mode = "descending";
    }
    return { thrusting: false, thrustLeft: false, thrustRight: false };
  }

  const hasFuelForThrust = world.mode === "descending" && world.fuel > 0;
  const thrusting = hasFuelForThrust && input.up;
  const thrustLeft = hasFuelForThrust && input.left;
  const thrustRight = hasFuelForThrust && input.right;

  if (world.mode === "descending") {
    const hasFuel = world.fuel > 0;
    if (hasFuel && input.up) {
      world.velY -= THRUST_POWER * dt;
      world.fuel = Math.max(0, world.fuel - FUEL_BURN_Y * dt);
    }
    if (hasFuel && input.left) {
      world.velX -= THRUST_POWER_X * dt;
      world.fuel = Math.max(0, world.fuel - FUEL_BURN_X * dt);
    }
    if (hasFuel && input.right) {
      world.velX += THRUST_POWER_X * dt;
      world.fuel = Math.max(0, world.fuel - FUEL_BURN_X * dt);
    }

    world.velY += GRAVITY * dt;
    world.posX += world.velX * dt;
    world.posY += world.velY * dt;
    world.posX = wrapX(world.posX);

    // Landing detection
    if (world.posY >= GROUND_LEVEL) {
      world.posY = GROUND_LEVEL;
      if (Math.abs(world.velY) > 50 || Math.abs(world.velX) > 30) {
        world.mode = "crashed";
        world.crashElapsed = 0;
      } else if (Math.abs(world.velY) <= MAX_LANDING_VELOCITY) {
        world.mode = "landed";
      }
      world.velX = 0;
      world.velY = 0;
      world.landerX = world.posX;
      world.landerY = GROUND_LEVEL;
    }
  } else if (world.mode === "landed" || world.mode === "in_lander") {
    if (input.launch && world.mode === "in_lander" && world.fuel >= MAX_FUEL) {
      world.mode = "launched";
    } else if (input.interact) {
      world.mode = "walking";
    }
  } else if (world.mode === "walking") {
    if (input.left) {
      world.posX -= WALK_SPEED * dt;
    }
    if (input.right) {
      world.posX += WALK_SPEED * dt;
    }
    world.posX = wrapX(world.posX);

    // Jump: Space/W while on the ground (one-shot per key press)
    const onGround = world.posY >= GROUND_LEVEL;
    if (onGround && input.jump) {
      world.velY = JUMP_VELOCITY;
    }

    // Apply jump gravity and integrate
    world.velY += JUMP_GRAVITY * dt;
    world.posY += world.velY * dt;
    if (world.posY >= GROUND_LEVEL) {
      world.posY = GROUND_LEVEL;
      world.velY = 0;
    }

    // Collect deposits the player walks over (skip types already owned)
    const pickupRange = ASTRONAUT_WIDTH;
    for (const d of deposits) {
      if (collectedIds.has(d.id)) continue;
      if (wrapDistance(d.x, world.posX) < pickupRange && !inventory.has(d.type)) {
        inventory.add(d.type);
        optimisticInventory.add(d.type);
        collectedIds.add(d.id);
        collectEffects.push({ x: d.x, fuelType: d.type, isRequired: d.type === requiredFuelType });
        callbacks.onCollectDeposit?.(d.id);
      }
    }

    // Proximity fuel sharing: give fuel to nearby walking remote players
    for (const rp of remotePlayers) {
      if (rp.mode !== "walking") continue;
      if (!rp.requiredFuelType || !rp.playerId) continue;
      if (wrapDistance(world.posX, rp.positionX) > SHARE_PROXIMITY_RADIUS) continue;
      if (rp.hasRequiredFuel) continue; // receiver already has what they need
      const ft = rp.requiredFuelType as FuelType;
      if (ft === requiredFuelType) continue; // never give away what we need
      if (!inventory.has(ft)) continue;
      inventory.delete(ft);
      optimisticInventory.delete(ft);
      sharedOut.add(ft);
      callbacks.onShareFuel?.(rp.requiredFuelType, rp.playerId);
      arcs.push({
        fuelType: ft,
        startX: world.posX,
        endX: rp.positionX,
        peakHeight: 60 + Math.random() * 30,
        duration: 0.5,
        elapsed: 0,
        rotation: 0,
        glowPhase: Math.random() * Math.PI * 2,
        targetPlayerId: rp.playerId,
      });
    }

    // Lander re-entry
    if (input.interact) {
      if (wrapDistance(world.posX, world.landerX) <= LANDER_INTERACT_RADIUS) {
        world.mode = "in_lander";
        world.posX = world.landerX;
        world.posY = GROUND_LEVEL;
        world.velY = 0;

        // Refuel if carrying the correct fuel type
        if (inventory.has(requiredFuelType)) {
          world.fuel = Math.min(MAX_FUEL, world.fuel + REFUEL_AMOUNT);
          inventory.delete(requiredFuelType);
          optimisticInventory.delete(requiredFuelType);
          callbacks.onRefuel?.(requiredFuelType);
        }

        // Burst: eject all non-required fuel types into space
        for (const ft of [...inventory]) {
          if (ft === requiredFuelType) continue;
          inventory.delete(ft);
          optimisticInventory.delete(ft);
          sharedOut.add(ft);
          collectEffects.push({ x: world.posX, fuelType: ft, isRequired: false, burst: true });
          callbacks.onBurstDeposit?.(ft);
        }
      }
    }
  } else if (world.mode === "launched") {
    world.launchElapsed += dt;
    world.velY -= THRUST_POWER * 1.5 * dt;
    world.posY += world.velY * dt;
    world.fuel = Math.max(0, world.fuel - FUEL_BURN_Y * dt);
    if (world.posY < -100000) {
      world.posY = -100000;
      world.velY = 0;
    }
    // Restart after success splash
    if (input.launch && world.launchElapsed > 5) {
      inventory.clear();
      optimisticInventory.clear();
      sharedOut.clear();
      collectedIds.clear();
      world.mode = "descending";
      world.posX = CANVAS_WIDTH / 2;
      world.posY = INITIAL_ALTITUDE;
      world.velX = 0;
      world.velY = 0;
      world.fuel = INITIAL_FUEL;
      world.launchElapsed = 0;
      world.crashElapsed = 0;
    }
  } else if (world.mode === "crashed") {
    world.crashElapsed += dt;
    if (input.launch && world.crashElapsed > 1) {
      inventory.clear();
      optimisticInventory.clear();
      sharedOut.clear();
      collectedIds.clear();
      world.mode = "descending";
      world.posX = CANVAS_WIDTH / 2;
      world.posY = INITIAL_ALTITUDE;
      world.velX = 0;
      world.velY = 0;
      world.fuel = INITIAL_FUEL;
      world.launchElapsed = 0;
      world.crashElapsed = 0;
    }
  }

  return { thrusting, thrustLeft, thrustRight };
}
