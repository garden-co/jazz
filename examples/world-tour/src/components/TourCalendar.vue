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
import { buildMonthGrid, mapStopsToGrid } from "../lib/calendar-grid.js";

interface StopProp {
  id: string;
  date: Date;
  venue: { name: string };
}

const props = defineProps<{
  stops: StopProp[];
  selectedStopId: string | null;
  canEdit: boolean;
}>();

const emit = defineEmits<{
  selectStop: [stopId: string];
  reschedule: [stopId: string, newDate: Date];
}>();

const dayHeaders = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

const monthNames = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

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

const monthLabel = computed(() => `${monthNames[currentMonth.value]} ${currentYear.value}`);

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
  if (!props.canEdit) return;
  event.dataTransfer?.setData("text/plain", stopId);
}

function onDragOver(day: { date: Date }) {
  if (!props.canEdit) return;
  dragOverKey.value = dateKey(day);
}

function onDragLeave() {
  dragOverKey.value = null;
}

function onDrop(day: { date: Date }, event: DragEvent) {
  dragOverKey.value = null;
  if (!props.canEdit) return;
  const stopId = event.dataTransfer?.getData("text/plain");
  if (!stopId) return;
  emit("reschedule", stopId, day.date);
}
</script>

<style scoped>
.tour-calendar {
  margin-bottom: 16px;
}

.cal-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 10px;
}

.cal-nav {
  background: none;
  border: none;
  color: var(--text-secondary);
  width: 30px;
  height: 30px;
  border-radius: 50%;
  cursor: pointer;
  font-size: 14px;
  line-height: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  transition:
    color var(--duration-fast),
    box-shadow var(--duration-fast);
}

.cal-nav:hover {
  color: var(--accent-primary);
  box-shadow: 0 0 8px var(--accent-primary-muted);
}

.cal-title {
  font-family: var(--font-display);
  font-weight: 700;
  font-size: 18px;
  text-transform: uppercase;
  color: var(--text-primary);
}

.cal-grid {
  display: grid;
  grid-template-columns: repeat(7, 1fr);
  gap: 1px;
}

.cal-day-header {
  text-align: center;
  font-family: var(--font-mono);
  font-weight: 500;
  font-size: 11px;
  color: var(--text-muted);
  padding: 4px 0;
  text-transform: uppercase;
  letter-spacing: 0.08em;
}

.cal-cell {
  min-height: 36px;
  padding: 2px 3px;
  background: rgba(255, 255, 255, 0.02);
  border-bottom: 1px solid var(--border-subtle);
  transition: background var(--duration-fast);
  overflow: hidden;
}

.cal-cell.dimmed .cal-day-num {
  opacity: 0.4;
  color: var(--text-muted);
}

.cal-cell.selected {
  border-left: 3px solid var(--accent-primary);
}

.cal-cell.drag-over {
  box-shadow: inset 0 0 12px var(--accent-primary-muted);
}

.cal-day-num {
  display: block;
  font-family: var(--font-body);
  font-weight: 400;
  font-size: 13px;
  color: var(--text-secondary);
  line-height: 1;
  margin-bottom: 2px;
}

.stop-chip {
  font-family: var(--font-mono);
  font-weight: 400;
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  padding: 1px 6px;
  border-radius: var(--radius-pill);
  background: var(--accent-secondary-muted);
  color: var(--accent-secondary);
  cursor: pointer;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  margin-bottom: 1px;
  line-height: 1.4;
  transition: background var(--duration-fast);
}

.stop-chip:hover {
  background: rgba(0, 229, 204, 0.22);
}
</style>
