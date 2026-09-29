<template>
  <section class="stop-detail">
    <template v-if="!editing">
      <div class="stack-2">
        <div class="row-between">
          <h3 class="heading-3">{{ stop.venue?.name }}</h3>
          <span class="badge" :data-tone="stop.status">{{ statusLabels[stop.status] }}</span>
        </div>
        <p class="text-secondary">
          {{ stop.venue?.city }}, {{ stop.venue?.country }} · {{ formatLongDate(stop.date) }}
        </p>
      </div>
      <p v-if="stop.publicDescription">{{ stop.publicDescription }}</p>
      <dl class="facts">
        <template v-if="stop.venue?.capacity">
          <dt>Capacity</dt>
          <dd>{{ stop.venue.capacity.toLocaleString("en-GB") }}</dd>
        </template>
        <template v-if="note">
          <dt>Private notes</dt>
          <dd>{{ note.body }}</dd>
        </template>
      </dl>
      <div class="actions">
        <Button @click="startEdit">Edit stop</Button>
      </div>
    </template>

    <form v-else class="form" @submit.prevent="save">
      <h3 class="heading-3">Edit {{ stop.venue?.name }}</h3>
      <label class="field">
        <span class="field__label">Date</span>
        <input v-model="draft.date" class="input" type="date" required />
      </label>
      <label class="field">
        <span class="field__label">Status</span>
        <select v-model="draft.status" class="input">
          <option v-for="(label, value) in statusLabels" :key="value" :value="value">
            {{ label }}
          </option>
        </select>
      </label>
      <label class="field">
        <span class="field__label">Public description</span>
        <textarea v-model="draft.description" class="input" rows="3" />
      </label>
      <label class="field">
        <span class="field__label">Private notes</span>
        <textarea v-model="draft.notes" class="input" rows="2" />
        <span class="field__hint">Only band members can read these.</span>
      </label>
      <div class="actions">
        <Button variant="primary" type="submit">Save</Button>
        <Button @click="editing = false">Cancel</Button>
        <Button variant="destructive" class="push-end" @click="deleteStop">Delete stop</Button>
      </div>
    </form>
  </section>
</template>

<script setup lang="ts">
import { computed, reactive, ref } from "vue";
import { useAll, useDb } from "jazz-tools/vue";
import { app, type StopStatus, type StopWithVenue } from "../../schema.js";
import { formatLongDate, fromDateInput, statusLabels, toDateInput } from "../lib/format.js";
import Button from "./ui/Button.vue";

const props = defineProps<{ stop: StopWithVenue }>();
const emit = defineEmits<{ deleted: [] }>();

const db = useDb();
const { data: notes } = useAll(() => app.stopNotes.where({ stopId: props.stop.id }).limit(1));
const note = computed(() => notes.value?.[0] ?? null);

const editing = ref(false);
const draft = reactive({ date: "", status: "confirmed" as StopStatus, description: "", notes: "" });

function startEdit() {
  Object.assign(draft, {
    date: toDateInput(props.stop.date),
    status: props.stop.status,
    description: props.stop.publicDescription,
    notes: note.value?.body ?? "",
  });
  editing.value = true;
}

function save() {
  const { id, bandId, date } = props.stop;
  // Keep the show's time of day when only the day changes.
  const day = fromDateInput(draft.date);
  day.setHours(date.getHours(), date.getMinutes());
  db.update(app.stops, id, {
    date: day,
    status: draft.status,
    publicDescription: draft.description,
  });

  const body = draft.notes.trim();
  if (note.value && body) db.update(app.stopNotes, note.value.id, { body });
  else if (note.value) db.delete(app.stopNotes, note.value.id);
  else if (body) db.insert(app.stopNotes, { stopId: id, bandId, body });
  editing.value = false;
}

function deleteStop() {
  if (note.value) db.delete(app.stopNotes, note.value.id);
  db.delete(app.stops, props.stop.id);
  emit("deleted");
}
</script>
