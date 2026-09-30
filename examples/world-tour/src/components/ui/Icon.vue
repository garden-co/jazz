<template>
  <svg
    class="icon"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    stroke-width="2"
    stroke-linecap="round"
    stroke-linejoin="round"
    aria-hidden="true"
  >
    <path v-for="d in paths[name]" :key="d" :d="d" />
  </svg>
</template>

<script setup lang="ts">
// Outline icons drawn on a 24px grid, in the same style as the site's icon set.
const paths = {
  close: ["M18 6 6 18", "m6 6 12 12"],
  "chevron-left": ["m15 18-6-6 6-6"],
  "chevron-right": ["m9 18 6-6-6-6"],
  locate: ["M2 12h3", "M19 12h3", "M12 2v3", "M12 19v3", "M12 7a5 5 0 1 0 0 10 5 5 0 1 0 0-10"],
  play: ["M6 3v18l15-9z"],
  stop: ["M6 6h12v12H6z"],
  pencil: ["M12 20h9", "M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4Z"],
  users: [
    "M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2",
    "M9 3a4 4 0 1 0 0 8 4 4 0 1 0 0-8",
    "M22 21v-2a4 4 0 0 0-3-3.9",
    "M16 3.1a4 4 0 0 1 0 7.8",
  ],
} as const;

defineProps<{ name: keyof typeof paths }>();
</script>
