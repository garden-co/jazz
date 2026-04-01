<template>
  <div class="poster-overlay" @click.self="$emit('dismiss')">
    <div class="poster">
      <svg
        class="poster-bg"
        viewBox="0 0 595 842"
        preserveAspectRatio="none"
        xmlns="http://www.w3.org/2000/svg"
      >
        <defs>
          <filter id="poster-grain">
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
        <rect width="595" height="842" :fill="palette[0]" />
        <polygon
          v-for="(tri, i) in triangles"
          :key="i"
          :points="tri.points"
          :fill="tri.fill"
          :opacity="tri.opacity"
        />
        <rect
          width="595"
          height="842"
          fill="rgba(0,0,0,0.1)"
          filter="url(#poster-grain)"
          opacity="0.3"
        />
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
import { generateTriangles, seedFromString, pickPalette } from "../lib/poster-triangles.js";

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

const palettes = [
  ["#1e0845", "#3a1078", "#0f3460"],
  ["#4a0e4e", "#810ca8", "#2d0036"],
  ["#0c2461", "#1e5799", "#0a3d62"],
  ["#5c2d91", "#8e44ad", "#2c0b4e"],
  ["#0e4d6b", "#1abc9c", "#0a2e3d"],
];

const seed = computed(() => seedFromString(props.bandName));
const palette = computed(() => pickPalette(seed.value, palettes));

const triangles = computed(() =>
  generateTriangles({
    cols: 6,
    rows: 9,
    seed: seed.value,
    palette: palette.value,
  }),
);
</script>

<style scoped>
@import "../styles/tour-poster.css";
</style>
