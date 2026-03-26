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

  db.insert(app.stops, {
    bandId: props.bandId,
    venueId,
    date: new Date(stopDate.value),
    status: stopStatus.value,
    publicDescription: publicDescription.value,
    ...(privateNotes.value ? { privateNotes: privateNotes.value } : {}),
  });

  emit("created");
}
</script>

<style scoped>
.stop-create-form {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.form-title {
  margin: 0;
  font-family: var(--font-display);
  font-weight: 800;
  font-size: 22px;
  color: var(--text-primary);
}

.fieldset {
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  padding: 16px;
  margin: 0;
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.legend {
  font-family: var(--font-display);
  font-weight: 600;
  font-size: 13px;
  color: var(--accent-primary);
  text-transform: uppercase;
  letter-spacing: 0.06em;
  padding: 0 6px;
}

.toggle-row {
  display: flex;
  gap: 6px;
}

.toggle-btn {
  flex: 1;
  padding: 6px 12px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: transparent;
  color: var(--text-secondary);
  font-family: var(--font-body);
  font-size: 13px;
  cursor: pointer;
  transition: all var(--duration-fast);
}

.toggle-btn.active {
  background: var(--accent-primary-muted);
  border-color: var(--accent-primary);
  color: var(--accent-primary);
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

.row {
  display: flex;
  gap: 12px;
}

.half {
  flex: 1;
}

.actions {
  display: flex;
  gap: 10px;
}

.btn {
  flex: 1;
  padding: 10px 16px;
  border: none;
  border-radius: var(--radius-md);
  font-size: 14px;
  cursor: pointer;
  transition: background var(--duration-fast);
}

.btn.primary {
  background: var(--accent-primary);
  color: var(--text-inverse);
  font-family: var(--font-display);
  font-weight: 600;
}

.btn.primary:hover:not(:disabled) {
  opacity: 0.9;
}

.btn.primary:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.btn.secondary {
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
</style>
