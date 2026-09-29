<template>
  <section class="calendar" aria-label="Tour calendar">
    <header class="row-between">
      <h3 class="heading-4">{{ monthLabel }}</h3>
      <div class="actions">
        <Button variant="ghost" icon-only aria-label="Previous month" @click="shiftMonth(-1)">
          <Icon name="chevron-left" />
        </Button>
        <Button variant="ghost" icon-only aria-label="Next month" @click="shiftMonth(1)">
          <Icon name="chevron-right" />
        </Button>
      </div>
    </header>

    <div class="calendar__grid">
      <span v-for="d in weekdays" :key="d" class="calendar__weekday">{{ d }}</span>
      <div
        v-for="day in days"
        :key="day.key"
        class="calendar__day"
        :class="{ outside: !day.isCurrentMonth, 'drop-target': dropKey === day.key }"
        @dragover.prevent="dropKey = day.key"
        @dragleave="dropKey = null"
        @drop.prevent="onDrop(day.date, $event)"
      >
        <span class="calendar__date">{{ day.dayOfMonth }}</span>
        <button
          v-for="stop in stopsByDay.get(day.key) ?? []"
          :key="stop.id"
          type="button"
          class="calendar__stop"
          :data-status="stop.status"
          :aria-pressed="stop.id === selectedStopId"
          draggable="true"
          :title="`${stop.venue?.name} (${statusLabels[stop.status]})`"
          @click="emit('selectStop', stop.id)"
          @dragstart="$event.dataTransfer?.setData('text/plain', stop.id)"
        >
          {{ stop.venue?.city }}
        </button>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { useDb } from "jazz-tools/vue";
import { app, type StopWithVenue } from "../../schema.js";
import { buildMonthGrid } from "../lib/calendar-grid.js";
import { statusLabels, toDateInput } from "../lib/format.js";
import Button from "./ui/Button.vue";
import Icon from "./ui/Icon.vue";

const props = defineProps<{ stops: StopWithVenue[]; selectedStopId: string | null }>();
const emit = defineEmits<{ selectStop: [stopId: string] }>();

const db = useDb();
const weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

// Open on the selected stop's month, or the first stop's, until the user pages.
const offset = ref(0);
const anchor = computed(() => {
  const selected = props.stops.find((s) => s.id === props.selectedStopId);
  return selected?.date ?? props.stops[0]?.date ?? new Date();
});
const month = computed(
  () => new Date(anchor.value.getFullYear(), anchor.value.getMonth() + offset.value, 1),
);
const monthLabel = computed(() =>
  month.value.toLocaleDateString("en-GB", { month: "long", year: "numeric" }),
);

const days = computed(() =>
  buildMonthGrid(month.value.getFullYear(), month.value.getMonth())
    .flat()
    .map((day) => ({ ...day, key: toDateInput(day.date) })),
);

const stopsByDay = computed(() => {
  const map = new Map<string, StopWithVenue[]>();
  for (const stop of props.stops) {
    const key = toDateInput(stop.date);
    map.set(key, [...(map.get(key) ?? []), stop]);
  }
  return map;
});

watch(
  () => props.selectedStopId,
  () => (offset.value = 0),
);

function shiftMonth(delta: number) {
  offset.value += delta;
}

// Drag a stop onto another day to move it; the show keeps its time of day.
const dropKey = ref<string | null>(null);
function onDrop(day: Date, event: DragEvent) {
  dropKey.value = null;
  const stop = props.stops.find((s) => s.id === event.dataTransfer?.getData("text/plain"));
  if (!stop) return;
  const date = new Date(day);
  date.setHours(stop.date.getHours(), stop.date.getMinutes());
  db.update(app.stops, stop.id, { date });
}
</script>
