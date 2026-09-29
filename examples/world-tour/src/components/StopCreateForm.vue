<template>
  <form class="form" @submit.prevent="submit">
    <fieldset class="fieldset">
      <legend class="heading-4">Venue</legend>
      <div class="segmented" role="radiogroup" aria-label="Venue">
        <label><input v-model="venueMode" type="radio" value="new" />New venue</label>
        <label><input v-model="venueMode" type="radio" value="existing" />Existing venue</label>
      </div>

      <template v-if="venueMode === 'new'">
        <label class="field">
          <span class="field__label">Name</span>
          <input v-model="venue.name" class="input" required />
        </label>
        <div class="field-row">
          <label class="field">
            <span class="field__label">City</span>
            <input v-model="venue.city" class="input" required />
          </label>
          <label class="field">
            <span class="field__label">Country</span>
            <input v-model="venue.country" class="input" required />
          </label>
        </div>
        <div class="field-row">
          <label class="field">
            <span class="field__label">Latitude</span>
            <input v-model.number="venue.lat" class="input" type="number" step="any" min="-90" max="90" required />
          </label>
          <label class="field">
            <span class="field__label">Longitude</span>
            <input v-model.number="venue.lng" class="input" type="number" step="any" min="-180" max="180" required />
          </label>
        </div>
        <label class="field">
          <span class="field__label">Capacity</span>
          <input v-model.number="venue.capacity" class="input" type="number" min="0" />
        </label>
      </template>

      <label v-else class="field">
        <span class="field__label">Venue</span>
        <select v-model="existingVenueId" class="input" required>
          <option value="" disabled>Choose a venue</option>
          <option v-for="v in venues ?? []" :key="v.id" :value="v.id">
            {{ v.name }}, {{ v.city }}
          </option>
        </select>
      </label>
    </fieldset>

    <fieldset class="fieldset">
      <legend class="heading-4">Show</legend>
      <div class="field-row">
        <label class="field">
          <span class="field__label">Date</span>
          <input v-model="show.date" class="input" type="date" required />
        </label>
        <label class="field">
          <span class="field__label">Status</span>
          <select v-model="show.status" class="input">
            <option v-for="(label, value) in statusLabels" :key="value" :value="value">
              {{ label }}
            </option>
          </select>
        </label>
      </div>
      <label class="field">
        <span class="field__label">Public description</span>
        <textarea v-model="show.description" class="input" rows="3" required />
      </label>
      <label class="field">
        <span class="field__label">Private notes</span>
        <textarea v-model="show.notes" class="input" rows="2" />
        <span class="field__hint">Only band members can read these.</span>
      </label>
    </fieldset>

    <div class="actions">
      <Button variant="primary" type="submit">Add stop</Button>
      <Button @click="emit('cancel')">Cancel</Button>
    </div>
  </form>
</template>

<script setup lang="ts">
import { reactive, ref } from "vue";
import { useAll, useDb } from "jazz-tools/vue";
import { app, type StopStatus } from "../../schema.js";
import { fromDateInput, statusLabels } from "../lib/format.js";
import Button from "./ui/Button.vue";

const props = defineProps<{ lat: number; lng: number; bandId: string; userId: string }>();
const emit = defineEmits<{ created: [stopId: string]; cancel: [] }>();

const db = useDb();
const { data: venues } = useAll(app.venues.orderBy("name", "asc"));

const venueMode = ref<"new" | "existing">("new");
const existingVenueId = ref("");
const venue = reactive({
  name: "",
  city: "",
  country: "",
  lat: Number(props.lat.toFixed(4)),
  lng: Number(props.lng.toFixed(4)),
  capacity: undefined as number | undefined,
});
const show = reactive({ date: "", status: "tentative" as StopStatus, description: "", notes: "" });

async function submit() {
  const venueId =
    venueMode.value === "existing"
      ? existingVenueId.value
      : db.insert(app.venues, {
          ...venue,
          capacity: venue.capacity || undefined,
          ownerId: props.userId,
          bandId: props.bandId,
        }).value.id;

  const date = fromDateInput(show.date);
  date.setHours(20);
  const stop = db.insert(app.stops, {
    bandId: props.bandId,
    venueId,
    date,
    status: show.status,
    publicDescription: show.description,
  });
  emit("created", stop.value.id);

  const body = show.notes.trim();
  if (body) {
    // The note's policy checks that the stop belongs to the band, so let the stop land first.
    await stop.wait({ tier: "global" });
    db.insert(app.stopNotes, { stopId: stop.value.id, bandId: props.bandId, body });
  }
}
</script>
