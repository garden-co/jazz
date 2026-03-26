<template>
  <div class="poster-overlay" @click.self="$emit('dismiss')">
    <div class="poster">
      <svg class="poster-bg" viewBox="0 0 595 842" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">
        <defs>
          <filter id="poster-grain">
            <feTurbulence type="fractalNoise" baseFrequency="0.65" numOctaves="3" stitchTiles="stitch" />
            <feColorMatrix type="saturate" values="0" />
            <feBlend in="SourceGraphic" mode="multiply" />
          </filter>
        </defs>
        <rect width="595" height="842" :fill="palette[0]" />
        <polygon
          v-for="(tri, i) in triangles"
          :key="i"
          :points="tri.points"
          :fill="tri.fill"
          :opacity="tri.opacity"
        />
        <rect width="595" height="842" fill="rgba(0,0,0,0.1)" filter="url(#poster-grain)" opacity="0.3" />
      </svg>
      <div class="poster-content">
        <div class="poster-header">
          <span class="poster-tag">WORLD TOUR</span>
          <h1 class="poster-band">{{ bandName }}</h1>
          <div class="poster-divider"></div>
        </div>

        <div class="poster-dates">
          <div v-for="stop in stops" :key="stop.id" class="poster-date-row">
            <div class="poster-date-left">
              <span class="poster-day">{{ formatDay(stop.date) }}</span>
              <span class="poster-month">{{ formatMonth(stop.date) }}</span>
            </div>
            <div class="poster-date-right">
              <span class="poster-venue">{{ stop.venueName }}</span>
              <span class="poster-city">{{ stop.city }}, {{ stop.country }}</span>
            </div>
          </div>
        </div>

        <div class="poster-footer">
          <span class="poster-cta">TAP ANYWHERE TO EXPLORE THE GLOBE</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";

const props = defineProps<{
  bandName: string;
  stops: Array<{
    id: string;
    date: Date;
    venueName: string;
    city: string;
    country: string;
  }>;
}>();

defineEmits<{ dismiss: [] }>();

function formatDay(d: Date | string): string {
  const date = d instanceof Date ? d : new Date(d);
  return date.getDate().toString();
}

function formatMonth(d: Date | string): string {
  const date = d instanceof Date ? d : new Date(d);
  return date.toLocaleDateString("en-GB", { month: "short" }).toUpperCase();
}

function hash(n: number): number {
  const x = Math.sin(n * 127.1 + 311.7) * 43758.5453;
  return x - Math.floor(x);
}

function blendHex(a: string, b: string, t: number): string {
  const pa = [parseInt(a.slice(1, 3), 16), parseInt(a.slice(3, 5), 16), parseInt(a.slice(5, 7), 16)];
  const pb = [parseInt(b.slice(1, 3), 16), parseInt(b.slice(3, 5), 16), parseInt(b.slice(5, 7), 16)];
  const r = Math.round(pa[0] + (pb[0] - pa[0]) * t);
  const g = Math.round(pa[1] + (pb[1] - pa[1]) * t);
  const bl = Math.round(pa[2] + (pb[2] - pa[2]) * t);
  return `#${r.toString(16).padStart(2, "0")}${g.toString(16).padStart(2, "0")}${bl.toString(16).padStart(2, "0")}`;
}

const palettes = [
  ["#1e0845", "#3a1078", "#0f3460"],
  ["#4a0e4e", "#810ca8", "#2d0036"],
  ["#0c2461", "#1e5799", "#0a3d62"],
  ["#5c2d91", "#8e44ad", "#2c0b4e"],
  ["#0e4d6b", "#1abc9c", "#0a2e3d"],
];

const seed = computed(() =>
  props.bandName.split("").reduce((a, c) => a + c.charCodeAt(0), 0),
);

const palette = computed(() => palettes[seed.value % palettes.length]);

const triangles = computed(() => {
  const W = 595;
  const H = 842;
  const cols = 6;
  const rows = 9;
  const cellW = W / cols;
  const cellH = H / rows;
  const s = seed.value;
  const tris: { points: string; fill: string; opacity: number }[] = [];

  const pts: [number, number][][] = [];
  for (let r = 0; r <= rows; r++) {
    pts[r] = [];
    for (let c = 0; c <= cols; c++) {
      const edge = r === 0 || r === rows || c === 0 || c === cols;
      const jx = edge ? 0 : (hash(s + r * 17 + c * 31) - 0.5) * cellW * 0.7;
      const jy = edge ? 0 : (hash(s + r * 53 + c * 97) - 0.5) * cellH * 0.7;
      pts[r][c] = [c * cellW + jx, r * cellH + jy];
    }
  }

  for (let r = 0; r < rows; r++) {
    for (let c = 0; c < cols; c++) {
      const tl = pts[r][c];
      const tr = pts[r][c + 1];
      const bl = pts[r + 1][c];
      const br = pts[r + 1][c + 1];
      const pal = palette.value;
      const h1 = hash(s + r * 7 + c * 13);
      const h2 = hash(s + r * 11 + c * 23 + 100);
      const lerp = (t: number) => {
        const idx = t * (pal.length - 1);
        const lo = Math.floor(idx);
        const hi = Math.min(lo + 1, pal.length - 1);
        return blendHex(pal[lo], pal[hi], idx - lo);
      };
      tris.push({
        points: `${tl[0]},${tl[1]} ${tr[0]},${tr[1]} ${bl[0]},${bl[1]}`,
        fill: lerp(h1),
        opacity: 0.4 + h1 * 0.5,
      });
      tris.push({
        points: `${tr[0]},${tr[1]} ${br[0]},${br[1]} ${bl[0]},${bl[1]}`,
        fill: lerp(h2),
        opacity: 0.4 + h2 * 0.5,
      });
    }
  }
  return tris;
});
</script>

<style scoped>
.poster-overlay {
  position: fixed;
  inset: 0;
  z-index: 30;
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgba(0, 0, 0, 0.8);
  backdrop-filter: blur(12px);
  -webkit-backdrop-filter: blur(12px);
  animation: overlay-in 0.4s ease-out;
  cursor: pointer;
}

@keyframes overlay-in {
  from { opacity: 0; }
  to { opacity: 1; }
}

.poster {
  position: relative;
  width: min(420px, 90vw);
  aspect-ratio: 595 / 842;
  border-radius: 4px;
  overflow: hidden;
  box-shadow:
    0 0 0 1px rgba(255, 255, 255, 0.06),
    0 25px 80px rgba(0, 0, 0, 0.6),
    0 0 120px rgba(255, 45, 123, 0.08);
  animation: poster-in 0.6s cubic-bezier(0.16, 1, 0.3, 1);
  cursor: default;
}

@keyframes poster-in {
  from { opacity: 0; transform: scale(0.92) translateY(30px); }
  to { opacity: 1; transform: scale(1) translateY(0); }
}

.poster-bg {
  position: absolute;
  inset: 0;
  width: 100%;
  height: 100%;
}

.poster-content {
  position: relative;
  z-index: 1;
  height: 100%;
  display: flex;
  flex-direction: column;
  padding: 8% 8%;
}

.poster-header {
  text-align: center;
  margin-bottom: auto;
}

.poster-tag {
  font-family: var(--font-mono);
  font-size: 9px;
  font-weight: 500;
  letter-spacing: 0.35em;
  color: var(--accent-primary);
  border: 1px solid var(--accent-primary);
  padding: 3px 14px;
  border-radius: 2px;
  display: inline-block;
  margin-bottom: 16px;
}

.poster-band {
  font-family: var(--font-display);
  font-weight: 800;
  font-size: clamp(18px, 5vw, 34px);
  letter-spacing: 0.04em;
  text-transform: uppercase;
  color: #fff;
  margin: 0;
  line-height: 1.15;
  text-shadow: 0 2px 40px rgba(0, 0, 0, 0.5);
  text-wrap: balance;
}

.poster-divider {
  width: 50px;
  height: 2px;
  background: var(--accent-primary);
  margin: 14px auto;
  box-shadow: 0 0 12px rgba(255, 45, 123, 0.5);
}

.poster-dates {
  flex: 1;
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 6px;
  overflow-y: auto;
}

.poster-date-row {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 5px 0;
  border-bottom: 1px solid rgba(255, 255, 255, 0.06);
}

.poster-date-row:last-child {
  border-bottom: none;
}

.poster-date-left {
  min-width: 52px;
  text-align: right;
  display: flex;
  flex-direction: column;
  align-items: flex-end;
}

.poster-day {
  font-family: var(--font-display);
  font-weight: 800;
  font-size: 22px;
  color: #fff;
  line-height: 1;
}

.poster-month {
  font-family: var(--font-mono);
  font-size: 9px;
  letter-spacing: 0.15em;
  color: var(--accent-primary);
}

.poster-date-right {
  display: flex;
  flex-direction: column;
  gap: 1px;
}

.poster-venue {
  font-family: var(--font-display);
  font-weight: 600;
  font-size: 13px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: rgba(255, 255, 255, 0.9);
}

.poster-city {
  font-family: var(--font-body);
  font-size: 11px;
  color: rgba(255, 255, 255, 0.4);
  letter-spacing: 0.04em;
}

.poster-footer {
  text-align: center;
  margin-top: auto;
  padding-top: 16px;
}

.poster-cta {
  font-family: var(--font-mono);
  font-size: 8px;
  letter-spacing: 0.3em;
  color: rgba(255, 255, 255, 0.3);
  animation: pulse-cta 3s ease-in-out infinite alternate;
}

@keyframes pulse-cta {
  0% { opacity: 0.3; }
  100% { opacity: 0.7; }
}
</style>
