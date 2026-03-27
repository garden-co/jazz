<template>
  <div class="stop-create-form">
    <h2 class="form-title">New tour stop</h2>

    <!-- Venue selection mode -->
    <fieldset class="fieldset">
      <legend class="legend">Venue</legend>

      <div class="toggle-row">
        <button
          class="toggle-btn"
          :class="{ active: venueMode === 'new' }"
          @click="venueMode = 'new'"
        >
          Create new
        </button>
        <button
          class="toggle-btn"
          :class="{ active: venueMode === 'existing' }"
          @click="venueMode = 'existing'"
        >
          Use existing
        </button>
      </div>

      <template v-if="venueMode === 'new'">
        <label class="label">
          Name
          <input v-model="newVenue.name" class="input" type="text" required />
        </label>
        <label class="label">
          City
          <input v-model="newVenue.city" class="input" type="text" required />
        </label>
        <label class="label">
          Country
          <input v-model="newVenue.country" class="input" type="text" required />
        </label>
        <div class="row">
          <label class="label half">
            Latitude
            <input v-model.number="newVenue.lat" class="input" type="number" step="any" required />
          </label>
          <label class="label half">
            Longitude
            <input v-model.number="newVenue.lng" class="input" type="number" step="any" required />
          </label>
        </div>
        <label class="label">
          Capacity
          <input v-model.number="newVenue.capacity" class="input" type="number" />
        </label>
      </template>

      <template v-if="venueMode === 'existing'">
        <label class="label">
          Select venue
          <select v-model="selectedVenueId" class="input">
            <option value="" disabled>Choose a venue...</option>
            <option v-for="venue in venues ?? []" :key="venue.id" :value="venue.id">
              {{ venue.name }} — {{ venue.city }}, {{ venue.country }}
            </option>
          </select>
        </label>
      </template>
    </fieldset>

    <!-- Stop fields -->
    <fieldset class="fieldset">
      <legend class="legend">Stop details</legend>

      <label class="label">
        Date
        <input v-model="stopDate" class="input" type="date" required />
      </label>

      <label class="label">
        Status
        <select v-model="stopStatus" class="input">
          <option value="tentative">Tentative</option>
          <option value="confirmed">Confirmed</option>
          <option value="cancelled">Cancelled</option>
        </select>
      </label>

      <label class="label">
        Public description
        <textarea v-model="publicDescription" class="input textarea" rows="3" required />
      </label>

      <label class="label">
        Private notes
        <textarea v-model="privateNotes" class="input textarea" rows="2" />
      </label>
    </fieldset>

    <div class="actions">
      <button class="btn primary" :disabled="!canSubmit" @click="submit">Create stop</button>
      <button class="btn secondary" @click="$emit('cancel')">Cancel</button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, computed } from "vue";
import { useDb, useAll } from "jazz-tools/vue";
import { app } from "../../schema/app.js";

const props = defineProps<{
  lat: number;
  lng: number;
  bandId: string;
}>();

const emit = defineEmits<{
  created: [];
  cancel: [];
}>();

const db = useDb();
const venues = useAll(app.venues);

const venueMode = ref<"new" | "existing">("new");

const newVenue = reactive({
  name: "",
  city: "",
  country: "",
  lat: props.lat,
  lng: props.lng,
  capacity: undefined as number | undefined,
});

const selectedVenueId = ref("");

const stopDate = ref("");
const stopStatus = ref<"confirmed" | "tentative" | "cancelled">("tentative");
const publicDescription = ref("");
const privateNotes = ref("");

const canSubmit = computed(() => {
  const hasVenue =
    venueMode.value === "existing"
      ? selectedVenueId.value !== ""
      : newVenue.name !== "" &&
        newVenue.city !== "" &&
        newVenue.country !== "" &&
        newVenue.lat >= -90 &&
        newVenue.lat <= 90 &&
        newVenue.lng >= -180 &&
        newVenue.lng <= 180;
  const hasStopFields = stopDate.value !== "" && publicDescription.value !== "";
  return hasVenue && hasStopFields;
});

function submit() {
  if (!canSubmit.value) return;

  let venueId: string;
  if (venueMode.value === "existing" && selectedVenueId.value) {
    venueId = selectedVenueId.value;
  } else if (venueMode.value === "new") {
    const venue = db.insert(app.venues, {
      name: newVenue.name,
      city: newVenue.city,
      country: newVenue.country,
      lat: newVenue.lat,
      lng: newVenue.lng,
      ...(newVenue.capacity != null ? { capacity: newVenue.capacity } : {}),
    });
    venueId = venue.id;
  } else {
    return;
  }

  const [y, m, d] = stopDate.value.split("-").map(Number);
  const localDate = new Date(y, m - 1, d);

  db.insert(app.stops, {
    bandId: props.bandId,
    venueId,
    date: localDate,
    status: stopStatus.value,
    publicDescription: publicDescription.value,
    ...(privateNotes.value ? { privateNotes: privateNotes.value } : {}),
  });

  emit("created");
}
</script>

<style>
@import "../styles/forms.css";
</style>

<style scoped>
@import "../styles/stop-create-form.css";
</style>
