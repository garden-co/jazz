import {
  GROUND_LEVEL,
  ASTRONAUT_HEIGHT,
  LANDER_HEIGHT,
  SHARE_PROXIMITY_RADIUS,
  curveOffset,
  leanAngle,
  type FuelType,
} from "./constants.js";
import {
  drawBackground,
  drawLander,
  drawAstronaut,
  drawDeposit,
  drawArrow,
  drawSplash,
  drawCrashSplash,
  drawBubbles,
  DEPOSIT_COLOURS,
} from "./render.js";
import { wrapScreenX, wrapDistance, wrapLerp } from "./world.js";
import { tickSpriteAnimation } from "./sprites.js";
import { updateParticles, drawParticles, emitThrust, emitSideThrust, emitSparkle, emitBurstUpward, emitTrail } from "./particles.js";
import type { SceneContext } from "./types.js";

// ---------------------------------------------------------------------------
// renderScene — draws one complete frame
// ---------------------------------------------------------------------------

export interface SceneResult {
  shareHint: boolean;
}

export function renderScene(scene: SceneContext): SceneResult {
  const {
    ctx, w, h, cameraX, cameraY, groundScreenY, dt, now,
    world, thrusting, thrustLeft, thrustRight,
    localPlayerName, localPlayerColor,
    deposits, collectedIds, requiredFuelType, inventory,
    arcs, remotePlayers, smoothedRemotes,
    chatMessages, localPlayerId,
    particles,
  } = scene;

  // Crisp pixel-art rendering
  ctx.imageSmoothingEnabled = false;

  // Advance sprite animation clock
  tickSpriteAnimation(dt);

  // Update particle simulation
  updateParticles(particles, dt);

  // --- Background ---
  drawBackground(ctx, cameraX, cameraY, w, h, now);

  // --- Fuel deposits ---
  const DEPOSIT_FADE_IN = 0.5; // seconds
  for (const dep of deposits) {
    if (collectedIds.has(dep.id)) continue;
    const dx = wrapScreenX(dep.x, cameraX);
    if (dx > -20 && dx < w + 20) {
      const depY = groundScreenY + curveOffset(dx, w);
      const age = now - dep.spawnTime;
      const alpha = Math.min(1, age / DEPOSIT_FADE_IN);
      if (alpha <= 0) continue;
      ctx.save();
      ctx.translate(dx, depY);
      ctx.rotate(leanAngle(dx, w));
      drawDeposit(ctx, 0, 0, dep.type, alpha);
      ctx.restore();
    }
  }

  // --- Arc animations (burst, share visuals) ---
  for (let i = arcs.length - 1; i >= 0; i--) {
    const arc = arcs[i];
    arc.elapsed += dt;
    if (arc.elapsed >= arc.duration) {
      arc.onComplete?.();
      arcs.splice(i, 1);
      continue;
    }

    // Reactive tracking: update end position for share arcs
    if (arc.targetPlayerId) {
      for (const rp of remotePlayers) {
        if (rp.playerId === arc.targetPlayerId) {
          const s = smoothedRemotes.get(rp.id);
          if (s) arc.endX = s.x;
          break;
        }
      }
    }

    const t = arc.elapsed / arc.duration;
    const arcX = wrapLerp(arc.startX, arc.endX, t);
    const arcY = GROUND_LEVEL - arc.peakHeight * 4 * t * (1 - t);
    const sx = wrapScreenX(arcX, cameraX);
    const sy = arcY - cameraY + curveOffset(sx, w);

    // Update rotation
    arc.rotation += dt * 6;

    // Emit trail particles
    const colour = DEPOSIT_COLOURS[arc.fuelType] ?? "#ffffff";
    emitTrail(particles, arcX, arcY, colour);

    if (sx > -20 && sx < w + 20 && sy > -20 && sy < h + 60) {
      // Pulsing glow
      const glowIntensity = 6 + 8 * Math.sin(now * 8 + arc.glowPhase);
      ctx.save();
      ctx.translate(sx, sy);
      ctx.rotate(arc.rotation);
      ctx.shadowColor = colour;
      ctx.shadowBlur = Math.max(0, glowIntensity);
      drawDeposit(ctx, 0, 8 + 2, arc.fuelType); // offset to account for deposit draw position
      ctx.restore();
      ctx.shadowBlur = 0;
    }
  }

  // --- Emit thrust particles ---
  const vx = world.velX;
  const vy = world.velY;
  if (thrusting && world.mode === "descending") {
    emitThrust(particles, world.posX, world.posY + 16, vx, vy);
  }
  if (world.mode === "launched") {
    const launchScreenY = world.posY - cameraY;
    if (launchScreenY > -60) {
      emitThrust(particles, world.posX, world.posY + 16, vx, vy);
    }
  }
  if (world.mode === "descending") {
    const jetY = world.posY - LANDER_HEIGHT * 0.65;
    if (thrustRight) emitSideThrust(particles, world.posX - 12 - 5, jetY, -1, vx, vy);
    if (thrustLeft) emitSideThrust(particles, world.posX + 12 + 5, jetY, 1, vx, vy);
  }

  // --- Collection sparkles + burst effects ---
  for (const effect of scene.collectEffects) {
    const colour = DEPOSIT_COLOURS[effect.fuelType] ?? "#ffffff";
    if (effect.burst) {
      // Fuel ejected into space — upward particle shower from the lander
      emitBurstUpward(particles, effect.x, GROUND_LEVEL - 20, colour);
    } else {
      emitSparkle(particles, effect.x, GROUND_LEVEL - 10, colour);
      if (effect.isRequired) {
        // Big celebration: 4 staggered bursts of sparkles in multiple colours
        emitSparkle(particles, effect.x, GROUND_LEVEL - 10, "#ffffff");
        emitSparkle(particles, effect.x, GROUND_LEVEL - 20, colour);
        emitSparkle(particles, effect.x - 8, GROUND_LEVEL - 15, "#ffffff");
        emitSparkle(particles, effect.x + 8, GROUND_LEVEL - 15, colour);
      }
    }
  }
  scene.collectEffects.length = 0;

  // --- Parked landers (local + remote) ---
  if (world.mode === "walking") {
    const landerSX = wrapScreenX(world.landerX, cameraX);
    if (landerSX > -40 && landerSX < w + 40) {
      const landerY = groundScreenY + curveOffset(landerSX, w);
      ctx.save();
      ctx.translate(landerSX, landerY);
      ctx.rotate(leanAngle(landerSX, w));
      drawLander(ctx, 0, 0, false);
      ctx.restore();
    }
  }
  for (const rp of remotePlayers) {
    if (rp.mode === "walking" && rp.landerX != null) {
      const rpLanderSX = wrapScreenX(rp.landerX, cameraX);
      if (rpLanderSX > -40 && rpLanderSX < w + 40) {
        const rpLanderY = groundScreenY + curveOffset(rpLanderSX, w);
        ctx.save();
        ctx.translate(rpLanderSX, rpLanderY);
        ctx.rotate(leanAngle(rpLanderSX, w));
        drawLander(ctx, 0, 0, false, rp.color);
        ctx.restore();
      }
    }
  }

  // --- Remote players (smooth + draw) ---
  const smoothed = smoothedRemotes;
  const lerpT = Math.min(1, 8 * dt);
  const activeIds = new Set<string>();
  for (const rp of remotePlayers) {
    activeIds.add(rp.id);
    let s = smoothed.get(rp.id);
    if (!s) {
      s = { x: rp.positionX, y: rp.positionY };
      smoothed.set(rp.id, s);
    }
    s.x = wrapLerp(s.x, rp.positionX, lerpT);
    s.y += (rp.positionY - s.y) * lerpT;

    const rpSX = wrapScreenX(s.x, cameraX);
    if (rpSX < -60 || rpSX > w + 60) continue;

    const rpCurve = curveOffset(rpSX, w);
    const rpLean = leanAngle(rpSX, w);
    if (rp.mode === "walking") {
      const rpWalkY = s.y - cameraY + rpCurve;
      const isMoving = Math.abs(rp.positionX - (smoothed.get(rp.id)?.x ?? rp.positionX)) > 0.5;
      ctx.save();
      ctx.translate(rpSX, rpWalkY);
      ctx.rotate(rpLean);
      drawAstronaut(ctx, 0, 0, rp.color, rp.name, isMoving);
      ctx.restore();
    } else if (rp.mode === "descending") {
      const rpSY = s.y - cameraY + rpCurve;
      if (rpSY > -60 && rpSY < h + 60) {
        drawLander(ctx, rpSX, rpSY, rp.thrusting, rp.color, rp.name);
      }
    } else if (rp.mode === "launched") {
      const rpSY = s.y - cameraY + rpCurve;
      if (rpSY > -60 && rpSY < h + 60) {
        drawLander(ctx, rpSX, rpSY, true, rp.color, rp.name);
      }
    } else {
      ctx.save();
      ctx.translate(rpSX, groundScreenY + rpCurve);
      ctx.rotate(rpLean);
      drawLander(ctx, 0, 0, false, rp.color, rp.name);
      ctx.restore();
    }
  }
  for (const id of smoothed.keys()) {
    if (!activeIds.has(id)) smoothed.delete(id);
  }

  // --- Local player ---
  const screenX = world.posX - cameraX;
  const localCurve = curveOffset(screenX, w);
  const localLean = leanAngle(screenX, w);
  const isWalking = world.mode === "walking";
  const localMoving = isWalking && scene.walkingInput;
  if (world.mode === "descending") {
    const screenY = world.posY - cameraY + localCurve;
    drawLander(ctx, screenX, screenY, thrusting, undefined, undefined, thrustLeft, thrustRight);
  } else if (world.mode === "landed" || world.mode === "in_lander") {
    const landedY = groundScreenY + localCurve;
    ctx.save();
    ctx.translate(screenX, landedY);
    ctx.rotate(localLean);
    drawLander(ctx, 0, 0, false);
    ctx.restore();
  } else if (isWalking) {
    const walkScreenY = world.posY - cameraY + localCurve;
    ctx.save();
    ctx.translate(screenX, walkScreenY);
    ctx.rotate(localLean);
    drawAstronaut(ctx, 0, 0, localPlayerColor || undefined, localPlayerName || undefined, localMoving);
    ctx.restore();
  } else if (world.mode === "launched") {
    const screenY = world.posY - cameraY + localCurve;
    if (screenY > -60 && screenY < h + 60) {
      drawLander(ctx, screenX, screenY, true);
    }
  }

  // --- Draw particles ---
  drawParticles(ctx, particles, cameraX, cameraY, w);

  // --- Speech bubbles ---
  const nowS = Math.floor(Date.now() / 1000);
  const BUBBLE_EXPIRY_S = 15;
  const recentMsgs = chatMessages.filter(
    (m) => nowS - m.createdAt < BUBBLE_EXPIRY_S,
  );
  if (recentMsgs.length > 0) {
    const byPlayer = new Map<string, string[]>();
    for (const m of recentMsgs) {
      let arr = byPlayer.get(m.playerId);
      if (!arr) {
        arr = [];
        byPlayer.set(m.playerId, arr);
      }
      arr.push(m.message);
    }
    const localMsgs = byPlayer.get(localPlayerId);
    if (localMsgs) {
      const localBubbleY = (world.mode === "walking" ? world.posY - cameraY : groundScreenY) + localCurve;
      // Lean the bubble along with the player — offset x by the lean at sprite top height
      const spriteH = ASTRONAUT_HEIGHT + 16;
      const bubbleDx = Math.sin(localLean) * spriteH;
      const spriteTop = localBubbleY - spriteH * Math.cos(localLean);
      drawBubbles(ctx, screenX - bubbleDx, spriteTop, localMsgs);
    }
    for (const rp of remotePlayers) {
      const rpMsgs = rp.playerId ? byPlayer.get(rp.playerId) : undefined;
      if (!rpMsgs) continue;
      const s = smoothed.get(rp.id);
      if (!s) continue;
      const rpSX = wrapScreenX(s.x, cameraX);
      if (rpSX < -60 || rpSX > w + 60) continue;
      const rpBubCurve = curveOffset(rpSX, w);
      const rpBubLean = leanAngle(rpSX, w);
      const rpBubbleY = (rp.mode === "walking" ? s.y - cameraY : groundScreenY) + rpBubCurve;
      const rpSpriteH = ASTRONAUT_HEIGHT + 16;
      const rpBubbleDx = Math.sin(rpBubLean) * rpSpriteH;
      const spriteTop = rpBubbleY - rpSpriteH * Math.cos(rpBubLean);
      drawBubbles(ctx, rpSX - rpBubbleDx, spriteTop, rpMsgs);
    }
  }

  // --- Success splash ---
  if (world.mode === "launched" && world.launchElapsed > 6) {
    const splashT = world.launchElapsed - 6;
    const splashAlpha = Math.min(1, splashT * 0.8);
    drawSplash(ctx, w, h, splashAlpha, splashT);
  }

  // --- Crash splash ---
  if (world.mode === "crashed") {
    const crashAlpha = Math.min(1, world.crashElapsed * 1.5);
    drawCrashSplash(ctx, w, h, crashAlpha, world.crashElapsed);
  }

  // --- Proximity hint (walking mode only) ---
  let shareHint = false;
  if (world.mode === "walking") {
    const HINT_RADIUS = SHARE_PROXIMITY_RADIUS * 2;
    for (const rp of remotePlayers) {
      if (rp.mode !== "walking") continue;
      if (!rp.requiredFuelType || !rp.playerId) continue;
      const dist = wrapDistance(world.posX, rp.positionX);
      if (dist > HINT_RADIUS || dist <= SHARE_PROXIMITY_RADIUS) continue;
      if (rp.hasRequiredFuel) continue;
      const ft = rp.requiredFuelType as FuelType;
      if (ft === requiredFuelType) continue;
      if (!inventory.has(ft)) continue;
      shareHint = true;
      break;
    }
    if (shareHint) {
      ctx.font = "12px monospace";
      ctx.textAlign = "center";
      ctx.fillStyle = "rgba(255, 0, 255, 0.7)";
      ctx.fillText("move closer to share fuel", w / 2, h - 90);
      ctx.textAlign = "start";
    }
  }

  // --- Arrows (walking mode only) ---
  if (world.mode === "walking") {
    const landerSX = wrapScreenX(world.landerX, cameraX);
    const landerDist = Math.floor(wrapDistance(world.posX, world.landerX));
    drawArrow(ctx, landerSX, w, h, "#00ffff", `lander ${landerDist}`);

    let nearestDep: { sx: number; dist: number } | null = null;
    for (const dep of deposits) {
      if (collectedIds.has(dep.id)) continue;
      if (dep.type !== requiredFuelType) continue;
      if (inventory.has(dep.type)) continue;
      const dist = wrapDistance(world.posX, dep.x);
      if (!nearestDep || dist < nearestDep.dist) {
        nearestDep = { sx: wrapScreenX(dep.x, cameraX), dist };
      }
    }
    if (nearestDep) {
      drawArrow(
        ctx,
        nearestDep.sx,
        w,
        h,
        DEPOSIT_COLOURS[requiredFuelType],
        `fuel ${Math.floor(nearestDep.dist)}`,
      );
    }
  }

  return { shareHint };
}
