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
          :aria-label="`${stop.venue?.city}, ${statusLabels[stop.status]}`"
          @click="emit('selectStop', stop.id)"
          @dragstart="$event.dataTransfer?.setData('text/plain', stop.id)"
        >
          <span class="status-mark" :data-status="stop.status" aria-hidden="true" />{{
            stop.venue?.city
          }}
        </button>
      </div>
    </div>
    <ul class="calendar__legend" aria-hidden="true">
      <li v-for="(label, status) in statusLabels" :key="status">
        <span class="status-mark" :data-status="status" />{{ label }}
      </li>
    </ul>
  </section>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { useAll, useDb } from "jazz-tools/vue";
import { app, type StopWithVenue } from "../../schema.js";
import { buildMonthGrid } from "../lib/calendar-grid.js";
import { statusLabels, toDateInput } from "../lib/format.js";
import Button from "./ui/Button.vue";
import Icon from "./ui/Icon.vue";

const props = defineProps<{ bandId: string; anchor: Date; selectedStopId: string | null }>();
const emit = defineEmits<{ selectStop: [stopId: string] }>();

const db = useDb();
const weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

// Open on the anchor's month (the selected stop, or the next one) until the user pages.
const offset = ref(0);
const month = computed(
  () => new Date(props.anchor.getFullYear(), props.anchor.getMonth() + offset.value, 1),
);
const monthLabel = computed(() =>
  month.value.toLocaleDateString("en-GB", { month: "long", year: "numeric" }),
);

const days = computed(() =>
  buildMonthGrid(month.value.getFullYear(), month.value.getMonth())
    .flat()
    .map((day) => ({ ...day, key: toDateInput(day.date) })),
);

// Every stop in the visible grid, including the padding days of the next and
// previous months. The stops policy hides tentative and cancelled dates from
// non-members, but only members see the calendar.
const { data: stopRows } = useAll(() => {
  const first = days.value[0].date;
  const end = new Date(days.value[days.value.length - 1].date);
  end.setDate(end.getDate() + 1);
  return app.stops
    .where({ bandId: props.bandId, date: { gte: first, lt: end } })
    .include({ venue: true })
    .orderBy("date", "asc");
});
const stops = computed(() => (stopRows.value ?? []).filter((s) => s.venue));

const stopsByDay = computed(() => {
  const map = new Map<string, StopWithVenue[]>();
  for (const stop of stops.value) {
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
  const stop = stops.value.find((s) => s.id === event.dataTransfer?.getData("text/plain"));
  if (!stop) return;
  const date = new Date(day);
  date.setHours(stop.date.getHours(), stop.date.getMinutes());
  db.update(app.stops, stop.id, { date });
}
</script>
