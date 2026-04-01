<template>
  <div class="poster-overlay" @click.self="$emit('close')">
    <div class="poster">
      <svg
        class="poster-bg"
        viewBox="0 0 595 842"
        preserveAspectRatio="none"
        xmlns="http://www.w3.org/2000/svg"
      >
        <defs>
          <filter id="grain">
            <feTurbulence
              type="fractalNoise"
              baseFrequency="0.65"
              numOctaves="3"
              stitchTiles="stitch"
            />
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
          <p v-if="stop.publicDescription" class="poster-description">
            {{ stop.publicDescription }}
          </p>
          <p v-if="venue?.capacity" class="poster-capacity">
            {{ venue.capacity.toLocaleString() }} capacity
          </p>
        </div>
      </div>
      <button class="poster-close" @click="$emit('close')">×</button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { StopWithVenue } from "../../schema.js";
import { generateTriangles, seedFromString, pickPalette } from "../lib/poster-triangles.js";

const props = defineProps<{
  stop: StopWithVenue;
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

const seed = computed(() => seedFromString(props.stop.id));
const colors = computed(() => pickPalette(seed.value, colorPalettes));

const triangles = computed(() =>
  generateTriangles({
    cols: 5,
    rows: 7,
    seed: seed.value,
    palette: colors.value,
  }),
);
</script>

<style scoped>
@import "../styles/stop-poster.css";
</style>
