import { COLOURS, curveOffset } from "./constants.js";

// ---------------------------------------------------------------------------
// Particle system — flat pre-allocated pool, no GC pressure
// ---------------------------------------------------------------------------

export const MAX_PARTICLES = 200;

export interface Particle {
  x: number;
  y: number;
  vx: number;
  vy: number;
  life: number;
  maxLife: number;
  colour: string;
  size: number;
  active: boolean;
}

// ---------------------------------------------------------------------------
// Pool management
// ---------------------------------------------------------------------------

export function createParticlePool(): Particle[] {
  const pool: Particle[] = [];
  for (let i = 0; i < MAX_PARTICLES; i++) {
    pool.push({ x: 0, y: 0, vx: 0, vy: 0, life: 0, maxLife: 1, colour: "#fff", size: 2, active: false });
  }
  return pool;
}

function spawn(pool: Particle[], x: number, y: number, vx: number, vy: number, life: number, colour: string, size: number): void {
  for (let i = 0; i < pool.length; i++) {
    if (!pool[i].active) {
      const p = pool[i];
      p.x = x;
      p.y = y;
      p.vx = vx;
      p.vy = vy;
      p.life = life;
      p.maxLife = life;
      p.colour = colour;
      p.size = size;
      p.active = true;
      return;
    }
  }
  // Pool full — silently drop
}

// ---------------------------------------------------------------------------
// Update — integrate positions, decay life
// ---------------------------------------------------------------------------

export function updateParticles(pool: Particle[], dt: number): void {
  for (let i = 0; i < pool.length; i++) {
    const p = pool[i];
    if (!p.active) continue;
    p.life -= dt;
    if (p.life <= 0) {
      p.active = false;
      continue;
    }
    p.x += p.vx * dt;
    p.y += p.vy * dt;
    // Slight gravity pull
    p.vy += 20 * dt;
  }
}

// ---------------------------------------------------------------------------
// Draw — batch by colour to minimise shadowBlur state changes
// ---------------------------------------------------------------------------

export function drawParticles(
  ctx: CanvasRenderingContext2D,
  pool: Particle[],
  cameraX: number,
  cameraY: number,
  screenW: number,
): void {
  // Group active particles by colour for batching
  const batches = new Map<string, Particle[]>();
  for (let i = 0; i < pool.length; i++) {
    const p = pool[i];
    if (!p.active) continue;
    let batch = batches.get(p.colour);
    if (!batch) {
      batch = [];
      batches.set(p.colour, batch);
    }
    batch.push(p);
  }

  const prevShadow = ctx.shadowBlur;
  const prevShadowColour = ctx.shadowColor;

  for (const [colour, particles] of batches) {
    ctx.fillStyle = colour;
    ctx.shadowColor = colour;
    ctx.shadowBlur = 6;
    for (const p of particles) {
      const alpha = p.life / p.maxLife;
      const sx = p.x - cameraX;
      const sy = p.y - cameraY + curveOffset(sx, screenW);
      ctx.globalAlpha = alpha;
      ctx.fillRect(Math.floor(sx), Math.floor(sy), p.size, p.size);
    }
  }

  ctx.globalAlpha = 1;
  ctx.shadowBlur = prevShadow;
  ctx.shadowColor = prevShadowColour;
}

// ---------------------------------------------------------------------------
// Emitters
// ---------------------------------------------------------------------------

const THRUST_COLOURS = [COLOURS.pink, COLOURS.orange, COLOURS.yellow, "#ff8844"];
const TAU = Math.PI * 2;

/** Emit 2-4 particles from the lander exhaust. Call each frame while thrusting. */
export function emitThrust(pool: Particle[], x: number, y: number, landerVx = 0, landerVy = 0): void {
  const count = 2 + Math.floor(Math.random() * 3);
  for (let i = 0; i < count; i++) {
    const colour = THRUST_COLOURS[Math.floor(Math.random() * THRUST_COLOURS.length)];
    const angle = -Math.PI / 2 + (Math.random() - 0.5) * 0.8; // mostly downward
    const speed = 40 + Math.random() * 60;
    spawn(
      pool, x, y,
      Math.cos(angle) * speed * -1 + landerVx, // inherit lander velocity
      Math.abs(Math.sin(angle)) * speed + 30 + landerVy,
      0.3 + Math.random() * 0.3,
      colour,
      2,
    );
  }
}

/** Emit 1-2 particles from a lateral thruster. Direction: -1 = left, +1 = right. */
export function emitSideThrust(pool: Particle[], x: number, y: number, direction: -1 | 1, landerVx = 0, landerVy = 0): void {
  const count = 1 + (Math.random() > 0.4 ? 1 : 0);
  for (let i = 0; i < count; i++) {
    const colour = THRUST_COLOURS[Math.floor(Math.random() * THRUST_COLOURS.length)];
    const spread = (Math.random() - 0.5) * 0.6;
    const speed = 30 + Math.random() * 40;
    spawn(
      pool, x, y,
      direction * speed + landerVx,
      spread * speed + landerVy,
      0.2 + Math.random() * 0.2,
      colour,
      1,
    );
  }
}

/** Burst of 10-16 radial sparkle particles on deposit pickup. */
export function emitSparkle(pool: Particle[], x: number, y: number, colour: string): void {
  const count = 10 + Math.floor(Math.random() * 7);
  for (let i = 0; i < count; i++) {
    const angle = (i / count) * TAU + (Math.random() - 0.5) * 0.4;
    const speed = 40 + Math.random() * 80;
    const c = Math.random() > 0.3 ? colour : "#ffffff";
    spawn(pool, x, y, Math.cos(angle) * speed, Math.sin(angle) * speed, 0.5 + Math.random() * 0.4, c, 2);
  }
}

/** Burst of 12-18 particles firing upward into space. */
export function emitBurstUpward(pool: Particle[], x: number, y: number, colour: string): void {
  const count = 12 + Math.floor(Math.random() * 7);
  for (let i = 0; i < count; i++) {
    const angle = -Math.PI / 2 + (Math.random() - 0.5) * 1.2; // mostly upward
    const speed = 100 + Math.random() * 150;
    const c = Math.random() > 0.3 ? colour : "#ffffff";
    spawn(pool, x, y, Math.cos(angle) * speed, Math.sin(angle) * speed, 0.6 + Math.random() * 0.5, c, 2);
  }
}

/** 1-2 trail particles along an arc path, tight clustering. */
export function emitTrail(pool: Particle[], x: number, y: number, colour: string): void {
  const count = 1 + (Math.random() > 0.5 ? 1 : 0);
  for (let i = 0; i < count; i++) {
    spawn(
      pool, x, y,
      (Math.random() - 0.5) * 6,
      (Math.random() - 0.5) * 6,
      0.15 + Math.random() * 0.1,
      colour,
      1,
    );
  }
}
