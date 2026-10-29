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
      <div
        v-if="stop.publicDescription"
        class="markdown"
        v-html="renderMarkdown(stop.publicDescription)"
      />
      <dl class="facts">
        <template v-if="stop.venue?.capacity">
          <dt>Capacity</dt>
          <dd>{{ stop.venue.capacity.toLocaleString("en-GB") }}</dd>
        </template>
        <template v-if="note">
          <dt>Private notes</dt>
          <dd class="markdown" v-html="renderMarkdown(note.body)" />
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
        <span class="field__hint">Markdown: **bold**, *italic*, [link](https://…)</span>
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
import { renderMarkdown } from "../lib/markdown.js";
import { rebaseText } from "../lib/text-rebase.js";
import { reportWriteError } from "../lib/write-errors.js";
import Button from "./ui/Button.vue";

const props = defineProps<{ stop: StopWithVenue }>();
const emit = defineEmits<{ deleted: [] }>();

const db = useDb();
const { data: notes } = useAll(() => app.stopNotes.where({ stopId: props.stop.id }).limit(1));
const note = computed(() => notes.value?.[0] ?? null);

const editing = ref(false);
const draft = reactive({ date: "", status: "confirmed" as StopStatus, description: "", notes: "" });
// The texts when editing began, to tell the form's edits from a bandmate's.
const opened = { description: "", notes: "" };

function startEdit() {
  opened.description = props.stop.publicDescription;
  opened.notes = note.value?.body ?? "";
  Object.assign(draft, {
    date: toDateInput(props.stop.date),
    status: props.stop.status,
    description: opened.description,
    notes: opened.notes,
  });
  editing.value = true;
}

// The stop and its private note change together, in one transaction.
function save() {
  const { id, bandId, date } = props.stop;
  const existing = note.value;
  // Keep the show's time of day when only the day changes.
  const day = fromDateInput(draft.date);
  day.setHours(date.getHours(), date.getMinutes());
  // A bandmate may have edited the texts while the form was open; keep
  // their edits rather than saving over them.
  const description = rebaseText(
    opened.description,
    draft.description,
    props.stop.publicDescription,
  );
  const body = rebaseText(opened.notes, draft.notes, existing?.body ?? "").trim();
  db.transaction((tx) => {
    tx.update(app.stops, id, {
      date: day,
      status: draft.status,
      publicDescription: description,
    });
    if (existing && body) tx.update(app.stopNotes, existing.id, { body });
    else if (existing) tx.delete(app.stopNotes, existing.id);
    else if (body) tx.insert(app.stopNotes, { stopId: id, bandId, body });
  }).catch(reportWriteError);
  editing.value = false;
}

function deleteStop() {
  const existing = note.value;
  db.transaction((tx) => {
    if (existing) tx.delete(app.stopNotes, existing.id);
    tx.delete(app.stops, props.stop.id);
  }).catch(reportWriteError);
  emit("deleted");
}
</script>
