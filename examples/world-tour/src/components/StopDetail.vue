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

function parseLocalDate(dateString: string): Date {
  const [y, m, d] = dateString.split("-").map(Number);
  return new Date(y, m - 1, d);
}

function save() {
  db.update(app.stops, props.stop.id, {
    date: parseLocalDate(editDate.value),
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

<style>
@import "../styles/forms.css";
</style>

<style scoped>
@import "../styles/stop-detail.css";
</style>
