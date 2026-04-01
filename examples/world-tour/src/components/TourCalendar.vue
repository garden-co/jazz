<template>
  <div class="tour-calendar">
    <div class="cal-header">
      <button class="cal-nav" @click="prevMonth">&larr;</button>
      <span class="cal-title">{{ monthLabel }}</span>
      <button class="cal-nav" @click="nextMonth">&rarr;</button>
    </div>

    <div class="cal-grid">
      <div class="cal-day-header" v-for="d in dayHeaders" :key="d">{{ d }}</div>

      <template v-for="(week, wi) in grid" :key="wi">
        <div
          v-for="(day, di) in week"
          :key="di"
          class="cal-cell"
          :class="{
            dimmed: !day.isCurrentMonth,
            selected: isSelectedDate(day),
            'drag-over': dragOverKey === dateKey(day),
          }"
          @dragover.prevent="onDragOver(day)"
          @dragleave="onDragLeave"
          @drop.prevent="onDrop(day, $event)"
        >
          <span class="cal-day-num">{{ day.dayOfMonth }}</span>
          <div
            v-for="stop in stopsForDay(day)"
            :key="stop.id"
            class="stop-chip"
            :draggable="canEdit"
            @click.stop="$emit('selectStop', stop.id)"
            @dragstart="onDragStart($event, stop.id)"
          >
            {{ stop.name }}
          </div>
        </div>
      </template>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from "vue";
import { useDb, useSession } from "jazz-tools/vue";
import { app } from "../../schema.js";
import { buildMonthGrid, mapStopsToGrid } from "../lib/calendar-grid.js";

interface StopProp {
  id: string;
  date: Date;
  venue: { name: string };
}

const props = defineProps<{
  stops: StopProp[];
  selectedStopId: string | null;
}>();

const emit = defineEmits<{
  selectStop: [stopId: string];
}>();

const db = useDb();
const session = useSession();
const canEdit = !!session;

const dayHeaders = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

const monthFormatter = new Intl.DateTimeFormat("en-GB", { month: "long" });

// Default to earliest stop's month, or current month
const initialDate = computed(() => {
  if (props.stops.length > 0) {
    const sorted = [...props.stops].sort((a, b) => a.date.getTime() - b.date.getTime());
    return sorted[0]!.date;
  }
  return new Date();
});

const viewYear = ref<number | null>(null);
const viewMonth = ref<number | null>(null);

const currentYear = computed(() => viewYear.value ?? initialDate.value.getFullYear());
const currentMonth = computed(() => viewMonth.value ?? initialDate.value.getMonth());

const monthLabel = computed(() => {
  const name = monthFormatter.format(new Date(currentYear.value, currentMonth.value, 1));
  return `${name} ${currentYear.value}`;
});

function prevMonth() {
  const y = currentYear.value;
  const m = currentMonth.value;
  if (m === 0) {
    viewYear.value = y - 1;
    viewMonth.value = 11;
  } else {
    viewYear.value = y;
    viewMonth.value = m - 1;
  }
}

function nextMonth() {
  const y = currentYear.value;
  const m = currentMonth.value;
  if (m === 11) {
    viewYear.value = y + 1;
    viewMonth.value = 0;
  } else {
    viewYear.value = y;
    viewMonth.value = m + 1;
  }
}

const grid = computed(() => buildMonthGrid(currentYear.value, currentMonth.value));

const stopMap = computed(() => {
  const mapped = props.stops.map((s) => ({ id: s.id, date: s.date }));
  return mapStopsToGrid(mapped, grid.value);
});

// Build a lookup from stop id to venue name
const stopNameMap = computed(() => {
  const m = new Map<string, string>();
  for (const s of props.stops) {
    m.set(s.id, s.venue.name);
  }
  return m;
});

function dateKey(day: { date: Date }): string {
  const y = day.date.getFullYear();
  const m = String(day.date.getMonth() + 1).padStart(2, "0");
  const d = String(day.date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function stopsForDay(day: { date: Date }): Array<{ id: string; name: string }> {
  const key = dateKey(day);
  const entries = stopMap.value.get(key);
  if (!entries) return [];
  return entries.map((e) => ({
    id: e.id,
    name: stopNameMap.value.get(e.id) ?? "",
  }));
}

function isSelectedDate(day: { date: Date }): boolean {
  if (!props.selectedStopId) return false;
  const selected = props.stops.find((s) => s.id === props.selectedStopId);
  if (!selected) return false;
  return dateKey(day) === dateKey({ date: selected.date });
}

// Drag-and-drop
const dragOverKey = ref<string | null>(null);

function onDragStart(event: DragEvent, stopId: string) {
  if (!canEdit) return;
  event.dataTransfer?.setData("text/plain", stopId);
}

function onDragOver(day: { date: Date }) {
  if (!canEdit) return;
  dragOverKey.value = dateKey(day);
}

function onDragLeave() {
  dragOverKey.value = null;
}

function onDrop(day: { date: Date }, event: DragEvent) {
  dragOverKey.value = null;
  if (!canEdit) return;
  const stopId = event.dataTransfer?.getData("text/plain");
  if (!stopId) return;
  // day.date is already local midnight from the calendar grid — no UTC parsing issue
  db.update(app.stops, stopId, { date: day.date });
}
</script>

<style scoped>
@import "../styles/tour-calendar.css";
</style>
