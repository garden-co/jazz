<template>
  <Dialog :title="bandName" @close="emit('dismiss')">
    <template v-if="stops.length > 0">
      <p class="text-secondary">Confirmed dates for the next three weeks</p>
      <ol class="date-list">
        <li v-for="stop in stops" :key="stop.id">
          <button type="button" class="date-list__row" @click="emit('select', stop.id)">
            <time class="date-list__date" :datetime="stop.date.toISOString()">
              {{ formatShortDate(stop.date) }}
            </time>
            <span>
              <span class="date-list__venue">{{ stop.venue?.name }}</span>
              <span class="text-secondary">{{ stop.venue?.city }}, {{ stop.venue?.country }}</span>
            </span>
          </button>
        </li>
      </ol>
    </template>
    <p v-else class="text-secondary">No confirmed dates in the next three weeks.</p>
    <div class="actions">
      <Button variant="primary" autofocus @click="emit('dismiss')">Explore the globe</Button>
    </div>
  </Dialog>
</template>

<script setup lang="ts">
import type { StopWithVenue } from "../../schema.js";
import { formatShortDate } from "../lib/format.js";
import Button from "./ui/Button.vue";
import Dialog from "./ui/Dialog.vue";

defineProps<{ bandName: string; stops: StopWithVenue[] }>();
const emit = defineEmits<{ dismiss: []; select: [stopId: string] }>();
</script>
