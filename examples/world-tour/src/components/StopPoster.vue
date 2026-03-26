<template>
  <div class="poster-overlay" @click.self="$emit('close')">
    <div class="poster">
      <svg class="poster-bg" viewBox="0 0 595 842" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">
        <defs>
          <filter id="grain">
            <feTurbulence type="fractalNoise" baseFrequency="0.65" numOctaves="3" stitchTiles="stitch" />
            <feColorMatrix type="saturate" values="0" />
            <feBlend in="SourceGraphic" mode="multiply" />
          </filter>
        </defs>
        <rect width="595" height="842" :fill="colors[0]" />
        <polygon
          v-for="(tri, i) in triangles"
          :key="i"
          :points="tri.points"
          :fill="tri.fill"
          :opacity="tri.opacity"
        />
        <rect width="595" height="842" fill="rgba(0,0,0,0.1)" filter="url(#grain)" opacity="0.3" />
      </svg>
      <div class="poster-content">
        <div class="poster-top">
          <span class="poster-tag">LIVE</span>
        </div>
        <div class="poster-main">
          <h1 class="poster-band" :style="bandNameStyle">{{ bandName }}</h1>
          <div class="poster-divider"></div>
          <h2 class="poster-venue">{{ venue?.name }}</h2>
          <p class="poster-location">{{ venue?.city }}, {{ venue?.country }}</p>
        </div>
        <div class="poster-bottom">
          <div class="poster-date">
            <span class="poster-date-day">{{ day }}</span>
            <span class="poster-date-month">{{ month }}</span>
            <span class="poster-date-year">{{ year }}</span>
          </div>
          <p v-if="stop.publicDescription" class="poster-description">{{ stop.publicDescription }}</p>
          <p v-if="venue?.capacity" class="poster-capacity">{{ venue.capacity.toLocaleString() }} capacity</p>
        </div>
      </div>
      <button class="poster-close" @click="$emit('close')">×</button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { StopWithIncludes } from "../../schema/app.js";

const props = defineProps<{
  stop: StopWithIncludes<{ venue: true }>;
  bandName: string;
}>();

defineEmits<{ close: [] }>();

const venue = computed(() => props.stop.venue);

const dateObj = computed(() => {
  const d = props.stop.date;
  return d instanceof Date ? d : new Date(d);
});

const bandNameStyle = computed(() => {
  const len = props.bandName.length;
  if (len <= 10) return {};
  if (len <= 16) return { fontSize: "clamp(26px, 6vw, 42px)" };
  if (len <= 24) return { fontSize: "clamp(20px, 5vw, 34px)" };
  return { fontSize: "clamp(16px, 4vw, 26px)" };
});

const day = computed(() => dateObj.value.getDate().toString());
const month = computed(() =>
  dateObj.value.toLocaleDateString("en-GB", { month: "long" }).toUpperCase(),
);
const year = computed(() => dateObj.value.getFullYear().toString());

function hash(n: number): number {
  const x = Math.sin(n * 127.1 + 311.7) * 43758.5453;
  return x - Math.floor(x);
}

const triangles = computed(() => {
  const W = 595;
  const H = 842;
  const cols = 5;
  const rows = 7;
  const cellW = W / cols;
  const cellH = H / rows;
  const seed = props.stop.id.split("").reduce((a, c) => a + c.charCodeAt(0), 0);
  const tris: { points: string; fill: string; opacity: number }[] = [];

  const pts: [number, number][][] = [];
  for (let r = 0; r <= rows; r++) {
    pts[r] = [];
    for (let c = 0; c <= cols; c++) {
      const baseX = c * cellW;
      const baseY = r * cellH;
      const jitterX = (r === 0 || r === rows || c === 0 || c === cols) ? 0 : (hash(seed + r * 17 + c * 31) - 0.5) * cellW * 0.7;
      const jitterY = (r === 0 || r === rows || c === 0 || c === cols) ? 0 : (hash(seed + r * 53 + c * 97) - 0.5) * cellH * 0.7;
      pts[r][c] = [baseX + jitterX, baseY + jitterY];
    }
  }

  for (let r = 0; r < rows; r++) {
    for (let c = 0; c < cols; c++) {
      const tl = pts[r][c];
      const tr = pts[r][c + 1];
      const bl = pts[r + 1][c];
      const br = pts[r + 1][c + 1];

      const h1 = hash(seed + r * 7 + c * 13);
      const h2 = hash(seed + r * 11 + c * 23 + 100);
      const pal = colors.value;

      const lerpColor = (t: number) => {
        const idx = t * (pal.length - 1);
        const lo = Math.floor(idx);
        const hi = Math.min(lo + 1, pal.length - 1);
        const f = idx - lo;
        return lo === hi ? pal[lo] : blendHex(pal[lo], pal[hi], f);
      };

      tris.push({
        points: `${tl[0]},${tl[1]} ${tr[0]},${tr[1]} ${bl[0]},${bl[1]}`,
        fill: lerpColor(h1),
        opacity: 0.4 + h1 * 0.5,
      });
      tris.push({
        points: `${tr[0]},${tr[1]} ${br[0]},${br[1]} ${bl[0]},${bl[1]}`,
        fill: lerpColor(h2),
        opacity: 0.4 + h2 * 0.5,
      });
    }
  }
  return tris;
});

function blendHex(a: string, b: string, t: number): string {
  const pa = [parseInt(a.slice(1, 3), 16), parseInt(a.slice(3, 5), 16), parseInt(a.slice(5, 7), 16)];
  const pb = [parseInt(b.slice(1, 3), 16), parseInt(b.slice(3, 5), 16), parseInt(b.slice(5, 7), 16)];
  const r = Math.round(pa[0] + (pb[0] - pa[0]) * t);
  const g = Math.round(pa[1] + (pb[1] - pa[1]) * t);
  const bl = Math.round(pa[2] + (pb[2] - pa[2]) * t);
  return `#${r.toString(16).padStart(2, "0")}${g.toString(16).padStart(2, "0")}${bl.toString(16).padStart(2, "0")}`;
}

const colorPalettes = [
  ["#1e0845", "#3a1078", "#0f3460"],
  ["#4a0e4e", "#810ca8", "#2d0036"],
  ["#0c2461", "#1e5799", "#0a3d62"],
  ["#6b0f1a", "#b91646", "#3b0a1a"],
  ["#0a4d3c", "#0f7b5f", "#073b30"],
  ["#5c2d91", "#8e44ad", "#2c0b4e"],
  ["#b7410e", "#e74c3c", "#4a1a0a"],
  ["#0e4d6b", "#1abc9c", "#0a2e3d"],
];

const paletteIndex = computed(() => {
  const h = props.stop.id.split("").reduce((a, c) => a + c.charCodeAt(0), 0);
  return h % colorPalettes.length;
});

const colors = computed(() => colorPalettes[paletteIndex.value]);
</script>

<style scoped>
.poster-overlay {
  position: fixed;
  inset: 0;
  z-index: 30;
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgba(0, 0, 0, 0.75);
  backdrop-filter: blur(8px);
  -webkit-backdrop-filter: blur(8px);
  animation: overlay-in 0.3s ease-out;
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
  animation: poster-in 0.5s cubic-bezier(0.16, 1, 0.3, 1);
}

@keyframes poster-in {
  from { opacity: 0; transform: scale(0.9) translateY(20px); }
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
  justify-content: space-between;
  padding: 10% 8%;
}

.poster-top {
  display: flex;
  justify-content: flex-end;
}

.poster-tag {
  font-family: var(--font-mono);
  font-size: 10px;
  font-weight: 500;
  letter-spacing: 0.3em;
  color: var(--accent-primary);
  border: 1px solid var(--accent-primary);
  padding: 3px 12px;
  border-radius: 2px;
}

.poster-main {
  text-align: center;
}

.poster-band {
  font-family: var(--font-display);
  font-weight: 800;
  font-size: clamp(32px, 8vw, 52px);
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: #fff;
  margin: 0;
  line-height: 1;
  text-shadow: 0 2px 40px rgba(0, 0, 0, 0.5);
  overflow-wrap: break-word;
  word-break: break-word;
}

.poster-divider {
  width: 60px;
  height: 2px;
  background: var(--accent-primary);
  margin: 20px auto;
  box-shadow: 0 0 12px rgba(255, 45, 123, 0.5);
}

.poster-venue {
  font-family: var(--font-display);
  font-weight: 600;
  font-size: clamp(16px, 4vw, 22px);
  letter-spacing: 0.15em;
  text-transform: uppercase;
  color: rgba(255, 255, 255, 0.85);
  margin: 0 0 6px;
}

.poster-location {
  font-family: var(--font-body);
  font-size: 14px;
  color: rgba(255, 255, 255, 0.5);
  margin: 0;
  letter-spacing: 0.06em;
}

.poster-bottom {
  text-align: center;
}

.poster-date {
  display: flex;
  align-items: baseline;
  justify-content: center;
  gap: 8px;
  margin-bottom: 16px;
}

.poster-date-day {
  font-family: var(--font-display);
  font-weight: 800;
  font-size: clamp(40px, 10vw, 64px);
  color: #fff;
  line-height: 1;
}

.poster-date-month {
  font-family: var(--font-display);
  font-weight: 600;
  font-size: clamp(16px, 4vw, 22px);
  letter-spacing: 0.2em;
  color: var(--accent-primary);
  text-transform: uppercase;
}

.poster-date-year {
  font-family: var(--font-mono);
  font-size: 13px;
  color: rgba(255, 255, 255, 0.4);
}

.poster-description {
  font-family: var(--font-body);
  font-size: 13px;
  font-style: italic;
  color: rgba(255, 255, 255, 0.55);
  margin: 0 0 8px;
  line-height: 1.5;
}

.poster-capacity {
  font-family: var(--font-mono);
  font-size: 11px;
  color: rgba(255, 255, 255, 0.3);
  margin: 0;
  letter-spacing: 0.06em;
}

.poster-close {
  position: absolute;
  top: 12px;
  right: 12px;
  z-index: 2;
  background: rgba(0, 0, 0, 0.4);
  border: none;
  color: rgba(255, 255, 255, 0.6);
  font-size: 24px;
  width: 36px;
  height: 36px;
  border-radius: 50%;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: color 0.15s, background 0.15s;
}

.poster-close:hover {
  color: #fff;
  background: rgba(0, 0, 0, 0.6);
}
</style>
