<template>
  <div class="stop-detail">
    <template v-if="!editing">
      <div v-if="canEdit" class="edit-bar">
        <button class="edit-btn" @click="startEdit">Edit</button>
      </div>

      <h2 class="venue-name">{{ venue?.name }}</h2>
      <p class="venue-location">{{ venue?.city }}, {{ venue?.country }}</p>

      <p class="stop-date">{{ formattedDate }}</p>

      <p v-if="stop.publicDescription" class="public-description">
        {{ stop.publicDescription }}
      </p>

      <p v-if="venue?.capacity" class="venue-capacity">
        Capacity: {{ venue.capacity.toLocaleString() }}
      </p>

      <template v-if="canEdit">
        <span class="status-badge" :class="stop.status">{{ stop.status }}</span>

        <p v-if="stop.privateNotes" class="private-notes">
          {{ stop.privateNotes }}
        </p>
      </template>
    </template>

    <template v-else>
      <h2 class="venue-name">{{ venue?.name }}</h2>
      <p class="venue-location">{{ venue?.city }}, {{ venue?.country }}</p>

      <label class="label">
        Date
        <input v-model="editDate" class="input" type="date" required />
      </label>

      <label class="label">
        Status
        <select v-model="editStatus" class="input">
          <option value="confirmed">Confirmed</option>
          <option value="tentative">Tentative</option>
          <option value="cancelled">Cancelled</option>
        </select>
      </label>

      <label class="label">
        Public description
        <textarea v-model="editDescription" class="input textarea" rows="3" />
      </label>

      <label class="label">
        Private notes
        <textarea v-model="editNotes" class="input textarea" rows="2" />
      </label>

      <div class="actions">
        <button class="btn primary" @click="save">Save</button>
        <button class="btn secondary" @click="cancelEdit">Cancel</button>
      </div>

      <button class="btn destructive" @click="deleteStop">Delete stop</button>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { useDb, useSession } from "jazz-tools/vue";
import { app } from "../../schema/app.js";
import type { StopWithIncludes } from "../../schema/app.js";

const props = defineProps<{
  stop: StopWithIncludes<{ venue: true }>;
}>();

const emit = defineEmits<{
  close: [];
}>();

const db = useDb();
const session = useSession();
const canEdit = !!session;

const venue = computed(() => props.stop.venue);

const formattedDate = computed(() => {
  const d = props.stop.date;
  const date = d instanceof Date ? d : new Date(d);
  return date.toLocaleDateString("en-GB", {
    weekday: "long",
    day: "numeric",
    month: "long",
    year: "numeric",
  });
});

const editing = ref(false);
const editDate = ref("");
const editStatus = ref<"confirmed" | "tentative" | "cancelled">("confirmed");
const editDescription = ref("");
const editNotes = ref("");

function formatDateForInput(d: Date | string): string {
  const date = d instanceof Date ? d : new Date(d);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function startEdit() {
  editDate.value = formatDateForInput(props.stop.date);
  editStatus.value = props.stop.status;
  editDescription.value = props.stop.publicDescription;
  editNotes.value = props.stop.privateNotes ?? "";
  editing.value = true;
}

function save() {
  db.update(app.stops, props.stop.id, {
    date: new Date(editDate.value),
    status: editStatus.value,
    publicDescription: editDescription.value,
    privateNotes: editNotes.value || undefined,
  });
  editing.value = false;
}

function cancelEdit() {
  editing.value = false;
}

function deleteStop() {
  db.delete(app.stops, props.stop.id);
  emit("close");
}
</script>

<style scoped>
.stop-detail {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.edit-bar {
  display: flex;
  justify-content: flex-end;
}

.edit-btn {
  background: none;
  border: none;
  color: var(--accent-primary);
  font-family: var(--font-display);
  font-weight: 600;
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  cursor: pointer;
  padding: 4px 10px;
  transition: opacity var(--duration-fast);
}

.edit-btn:hover {
  opacity: 0.8;
}

.venue-name {
  margin: 0;
  font-family: var(--font-display);
  font-weight: 800;
  font-size: 28px;
  color: var(--text-primary);
  letter-spacing: -0.01em;
  line-height: 1.2;
}

.venue-location {
  margin: 0;
  font-family: var(--font-body);
  font-weight: 400;
  font-size: 14px;
  color: var(--text-secondary);
}

.stop-date {
  margin: 0;
  font-family: var(--font-mono);
  font-weight: 500;
  font-size: 14px;
  color: var(--accent-secondary);
}

.public-description {
  margin: 0;
  font-family: var(--font-body);
  font-weight: 400;
  font-size: 15px;
  color: var(--text-primary);
  line-height: 1.6;
}

.venue-capacity {
  margin: 0;
  font-family: var(--font-mono);
  font-weight: 400;
  font-size: 12px;
  color: var(--text-muted);
}

.status-badge {
  display: inline-block;
  align-self: flex-start;
  padding: 5px 14px;
  border-radius: var(--radius-sm);
  font-family: var(--font-display);
  font-weight: 700;
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.12em;
  transform: rotate(-2deg);
}

.status-badge.confirmed {
  background: var(--status-confirmed-bg);
  color: var(--status-confirmed);
  border: 2px solid var(--status-confirmed);
  outline: 1px solid var(--status-confirmed);
  outline-offset: 2px;
}

.status-badge.tentative {
  background: var(--status-tentative-bg);
  color: var(--status-tentative);
  border: 2px solid var(--status-tentative);
  outline: 1px solid var(--status-tentative);
  outline-offset: 2px;
}

.status-badge.cancelled {
  background: var(--status-cancelled-bg);
  color: var(--status-cancelled);
  border: 2px solid var(--status-cancelled);
  outline: 1px solid var(--status-cancelled);
  outline-offset: 2px;
}

.private-notes {
  margin: 0;
  font-family: var(--font-body);
  font-style: italic;
  font-size: 13px;
  color: var(--text-muted);
  border-left: 3px solid var(--accent-primary);
  padding-left: 12px;
  line-height: 1.5;
}

.label {
  display: flex;
  flex-direction: column;
  gap: 4px;
  font-family: var(--font-mono);
  font-weight: 400;
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--text-muted);
}

.input {
  padding: 8px 10px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: var(--bg-input);
  color: var(--text-primary);
  font-size: 14px;
  font-family: var(--font-body);
  outline: none;
  transition: border-color var(--duration-fast);
}

.input:focus {
  border-color: var(--accent-primary);
}

.textarea {
  resize: vertical;
}

.actions {
  display: flex;
  gap: 10px;
}

.btn {
  padding: 10px 16px;
  border: none;
  border-radius: var(--radius-md);
  font-size: 14px;
  cursor: pointer;
  transition: background var(--duration-fast);
}

.btn.primary {
  flex: 1;
  background: var(--accent-primary);
  color: var(--text-inverse);
  font-family: var(--font-display);
  font-weight: 600;
}

.btn.primary:hover {
  opacity: 0.9;
}

.btn.secondary {
  flex: 1;
  background: transparent;
  border: 1px solid var(--border-subtle);
  color: var(--text-secondary);
  font-family: var(--font-body);
  font-weight: 500;
}

.btn.secondary:hover {
  background: rgba(255, 255, 255, 0.04);
  color: var(--text-primary);
}

.btn.destructive {
  background: var(--status-cancelled-bg);
  color: var(--status-cancelled);
  width: 100%;
  font-family: var(--font-body);
  font-weight: 500;
}

.btn.destructive:hover {
  background: rgba(255, 68, 68, 0.22);
}
</style>
