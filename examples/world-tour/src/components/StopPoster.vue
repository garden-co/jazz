<template>
  <Dialog :title="stop.venue?.name ?? 'Tour stop'" @close="emit('close')">
    <p class="text-secondary">{{ stop.venue?.city }}, {{ stop.venue?.country }}</p>
    <p class="poster-date">
      <time :datetime="stop.date.toISOString()">{{ formatLongDate(stop.date) }}</time>
    </p>
    <p>{{ bandName }}<template v-if="stop.publicDescription">: {{ stop.publicDescription }}</template></p>
    <p v-if="stop.venue?.capacity" class="text-secondary">
      {{ stop.venue.capacity.toLocaleString("en-GB") }} capacity
    </p>
  </Dialog>
</template>

<script setup lang="ts">
import type { StopWithVenue } from "../../schema.js";
import { formatLongDate } from "../lib/format.js";
import Dialog from "./ui/Dialog.vue";

defineProps<{ stop: StopWithVenue; bandName: string }>();
const emit = defineEmits<{ close: [] }>();
</script>
