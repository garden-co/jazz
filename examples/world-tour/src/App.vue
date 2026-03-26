<template>
  <div class="space-bg">
    <div class="control-bar">
      <div class="control-bar__brand">
        <div v-if="bandNameDisplay" class="band-name-row">
          <input
            v-if="editingBandName"
            ref="bandNameInput"
            class="band-name-input"
            :value="bandNameDisplay"
            @keydown.enter="saveBandName(($event.target as HTMLInputElement).value)"
            @keydown.escape="editingBandName = false"
            @blur="saveBandName(($event.target as HTMLInputElement).value)"
          />
          <span
            v-else
            class="band-name"
            :class="{ editable: canEdit }"
            @click="canEdit && startEditBandName()"
            >{{ bandNameDisplay }}<span v-if="canEdit" class="band-name-pencil">✎</span></span
          >
        </div>
        <div class="wordmark">
          <span class="wordmark__world">WORLD</span>
          <span class="wordmark__tour">TOUR</span>
        </div>
      </div>
      <div class="control-bar__row">
        <BandLogo v-if="firstBandId" :bandId="firstBandId" />
        <SyntheticUserSwitcher
          v-if="canEdit"
          :appId="appId"
          :defaultMode="'demo'"
          :reloadOnSwitch="true"
        />
        <button v-if="canEdit" class="btn-auth" @click="switchView()">View as public</button>
        <span v-if="!canEdit" class="public-label">Public view</span>
        <button v-if="!canEdit" class="btn-auth" @click="switchView()">Log in</button>
      </div>
    </div>
    <div id="map" style="width: 100vw; height: 100vh"></div>

    <AddStopPopover
      :x="popoverX"
      :y="popoverY"
      :visible="popoverVisible"
      @confirm="onPopoverConfirm"
      @dismiss="dismissPopover"
    />

    <Sheet :open="sheetOpen" @close="closeSheet" @closed="onSheetClosed">
      <TourCalendar
        v-if="sheetMode === 'detail'"
        :stops="calendarStops"
        :selectedStopId="selectedStop?.id ?? null"
        @selectStop="onCalendarSelectStop"
      />
      <StopDetail
        v-if="sheetMode === 'detail' && selectedStop"
        :stop="selectedStop"
        @close="closeSheet"
      />
      <StopCreateForm
        v-if="sheetMode === 'create' && firstBandId"
        :lat="createLat"
        :lng="createLng"
        :bandId="firstBandId"
        @created="closeSheet"
        @cancel="closeSheet"
      />
    </Sheet>

    <TourPoster
      v-if="showLandingPoster && landingPosterStops.length > 0"
      :bandName="bandsData?.[0]?.name ?? 'Unknown Band'"
      :stops="landingPosterStops"
      @dismiss="showLandingPoster = false"
    />

    <StopPoster
      v-if="posterStop && !showLandingPoster"
      :stop="posterStop"
      :bandName="bandsData?.[0]?.name ?? 'Unknown Band'"
      @close="posterStop = null"
    />

    <GeolocateFab :sheetOpen="sheetOpen" @locate="onGeolocate" />

    <!-- Splash hint for logged-in users -->
    <Transition name="splash-fade">
      <div v-if="showSplash" class="splash-overlay" @click="showSplash = false">
        <div class="splash-modal" @click.stop>
          <p class="splash-text">Click on an event to edit details</p>
          <button class="splash-btn" @click="showSplash = false">Got it</button>
        </div>
      </div>
    </Transition>

    <button
      class="tour-btn"
      :class="{ 'sheet-open': sheetOpen, touring: isTouring }"
      @click="isTouring ? stopTour() : startTour()"
    >
      {{ isTouring ? "■ Stop" : "▶ Tour" }}
    </button>
  </div>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted, ref, computed, watch, nextTick } from "vue";
import { useAll, useDb, useSession, SyntheticUserSwitcher } from "jazz-tools/vue";
import { app } from "../schema/app.js";
import type { StopWithIncludes } from "../schema/app.js";
import { MapController, TilePrefetcher, type StopMapData } from "./lib/map-controller";
import { findNearestStop } from "./lib/nearest-stop";
import type { StopWithLocation } from "./lib/nearest-stop";
import { ensureData } from "./seed-loader";
import Sheet from "./components/Sheet.vue";
import StopDetail from "./components/StopDetail.vue";
import StopPoster from "./components/StopPoster.vue";
import TourPoster from "./components/TourPoster.vue";
import StopCreateForm from "./components/StopCreateForm.vue";
import AddStopPopover from "./components/AddStopPopover.vue";
import TourCalendar from "./components/TourCalendar.vue";
import BandLogo from "./components/BandLogo.vue";
import GeolocateFab from "./components/GeolocateFab.vue";

const appId = "world-tour-example";

const db = useDb();
const session = useSession();
const isPublicMode = (window as any).__worldtour_public === true;
const canEdit = !!session && !isPublicMode;

function switchView() {
  if (canEdit) {
    window.location.href = window.location.pathname + "?public";
  } else {
    window.location.href = window.location.pathname;
  }
}

ensureData(db, session?.user_id, canEdit).catch((err) =>
  console.error("Failed to ensure data:", err),
);

type StopWithVenue = StopWithIncludes<{ venue: true }>;
const selectedStop = ref<StopWithVenue | null>(null);
const posterStop = ref<StopWithVenue | null>(null);
const showLandingPoster = ref(!canEdit);
const showSplash = ref(canEdit);
const sheetOpen = ref(false);
const sheetMode = ref<"detail" | "create">("detail");

const createLat = ref(0);
const createLng = ref(0);

const popoverVisible = ref(false);
const popoverX = ref(0);
const popoverY = ref(0);
const popoverLat = ref(0);
const popoverLng = ref(0);

const today = new Date();
today.setHours(0, 0, 0, 0);
const threeWeeks = new Date(today.getTime() + 21 * 24 * 60 * 60 * 1000);

const baseStopsQuery = app.stops
  .where({ date: { gte: today, lte: threeWeeks } })
  .include({ venue: true })
  .orderBy("date", "asc")
  .limit(12);
const confirmedStopsQuery = app.stops
  .where({ status: "confirmed", date: { gte: today, lte: threeWeeks } })
  .include({ venue: true })
  .orderBy("date", "asc")
  .limit(12);
const stopsQuery = canEdit ? baseStopsQuery : confirmedStopsQuery;
const stopsData = useAll(stopsQuery);

const bandsData = useAll(app.bands.limit(1));

const firstBandId = computed(() => {
  const bands = bandsData.value;
  return bands && bands.length > 0 ? bands[0].id : null;
});

const bandNameDisplay = computed(() => bandsData.value?.[0]?.name ?? null);
const editingBandName = ref(false);
const bandNameInput = ref<HTMLInputElement | null>(null);

function startEditBandName() {
  editingBandName.value = true;
  nextTick(() => bandNameInput.value?.focus());
}

function saveBandName(value: string) {
  editingBandName.value = false;
  const trimmed = value.trim();
  if (!trimmed || trimmed === bandNameDisplay.value) return;
  const bandId = firstBandId.value;
  if (bandId) db.update(app.bands, bandId, { name: trimmed });
}

const landingPosterStops = computed(() => {
  const stops = stopsData.value as StopWithVenue[] | null;
  if (!stops) return [];
  return stops
    .filter((s) => s.venue != null)
    .map((s) => ({
      id: s.id,
      date: s.date instanceof Date ? s.date : new Date(s.date),
      venueName: s.venue!.name,
      city: s.venue!.city,
      country: s.venue!.country,
    }));
});

const calendarStops = computed(() => {
  const stops = stopsData.value;
  if (!stops) return [];
  return stops
    .filter((s) => s.venue != null)
    .map((s) => ({
      id: s.id,
      date: s.date instanceof Date ? s.date : new Date(s.date),
      venue: { name: s.venue!.name },
    }));
});

function selectStop(stop: StopWithVenue) {
  if (!canEdit) {
    posterStop.value = stop;
  } else {
    selectedStop.value = stop;
    sheetMode.value = "detail";
    sheetOpen.value = true;
  }

  if (stop.venue && mapCtrl) {
    mapCtrl.flyTo(
      { lng: stop.venue.lng, lat: stop.venue.lat },
      { zoom: 5, pitch: 40, duration: 1500 },
    );
  }
}

function closeSheet() {
  sheetOpen.value = false;
}

function onSheetClosed() {
  selectedStop.value = null;
  sheetMode.value = "detail";
}

function dismissPopover() {
  popoverVisible.value = false;
}

function onPopoverConfirm() {
  popoverVisible.value = false;
  createLat.value = popoverLat.value;
  createLng.value = popoverLng.value;
  sheetMode.value = "create";
  sheetOpen.value = true;
}

function onCalendarSelectStop(stopId: string) {
  const stops = stopsData.value as StopWithVenue[] | null;
  if (!stops) return;
  const stop = stops.find((s) => s.id === stopId);
  if (stop) selectStop(stop);
}

function onGeolocate(coords: { lat: number; lng: number }) {
  const stops = stopsData.value as StopWithVenue[] | null;
  if (!stops || stops.length === 0) return;

  const stopsWithLocation: StopWithLocation[] = stops
    .filter((s) => s.venue != null)
    .map((s) => ({ id: s.id, lat: s.venue!.lat, lng: s.venue!.lng }));

  const nearest = findNearestStop(coords, stopsWithLocation);
  if (!nearest) return;

  const nearestStop = stops.find((s) => s.id === nearest.id);
  if (!nearestStop?.venue) return;

  mapCtrl?.flyTo(
    { lng: nearestStop.venue.lng, lat: nearestStop.venue.lat },
    { zoom: 6, pitch: 50, bearing: -20, duration: 3000 },
  );

  selectStop(nearestStop);
}

const isTouring = ref(false);

function stopTour() {
  mapCtrl?.stopTour();
  isTouring.value = false;
}

async function startTour() {
  const stops = stopsData.value as StopWithVenue[] | null;
  if (!stops || !mapCtrl) return;

  const sorted = stops
    .filter((s) => s.venue != null)
    .sort((a, b) => {
      const da = a.date instanceof Date ? a.date : new Date(a.date);
      const dateB = b.date instanceof Date ? b.date : new Date(b.date);
      return da.getTime() - dateB.getTime();
    });

  if (sorted.length === 0) return;

  isTouring.value = true;
  closeSheet();

  const tourStops: StopMapData[] = sorted.map((s) => ({
    id: s.id,
    name: s.venue!.name,
    lng: s.venue!.lng,
    lat: s.venue!.lat,
  }));

  await mapCtrl.tour(tourStops);
  isTouring.value = false;
}

let mapCtrl: MapController | null = null;

// Hidden off-canvas map that visits each stop to warm the tile cache
const prefetcher = new TilePrefetcher();
watch(
  stopsData,
  (stops) => {
    if (!stops || stops.length === 0) return;
    const mapData: StopMapData[] = (stops as StopWithVenue[])
      .filter((s) => s.venue != null)
      .map((s) => ({ id: s.id, name: s.venue!.name, lng: s.venue!.lng, lat: s.venue!.lat }));
    prefetcher.prefetch(mapData);
  },
  { immediate: true },
);

onMounted(async () => {
  mapCtrl = new MapController({ container: "map" });

  mapCtrl.on("stopClick", (e) => {
    dismissPopover();
    const stops = stopsData.value as StopWithVenue[] | null;
    const stop = stops?.find((s) => s.id === e.stopId);
    if (stop) selectStop(stop);
  });

  mapCtrl.on("mapClick", (e) => {
    dismissPopover();

    // Reject clicks outside valid Earth coordinates
    if (e.lat < -90 || e.lat > 90 || e.lng < -180 || e.lng > 180) return;

    if (canEdit) {
      popoverX.value = e.x;
      popoverY.value = e.y;
      popoverLat.value = e.lat;
      popoverLng.value = e.lng;
      popoverVisible.value = true;
    } else {
      const stops = stopsData.value as StopWithVenue[] | null;
      if (!stops || stops.length === 0) return;

      const confirmedWithLocation: StopWithLocation[] = stops
        .filter((s) => s.status === "confirmed" && s.venue != null)
        .map((s) => ({ id: s.id, lat: s.venue!.lat, lng: s.venue!.lng }));

      const nearestLoc = findNearestStop({ lat: e.lat, lng: e.lng }, confirmedWithLocation);
      if (!nearestLoc) return;

      const nearestStop = stops.find((s) => s.id === nearestLoc.id);
      if (!nearestStop?.venue) return;

      mapCtrl!.flyTo(
        { lng: nearestStop.venue.lng, lat: nearestStop.venue.lat },
        { zoom: 5, pitch: 45 },
      );

      selectStop(nearestStop);
    }
  });

  await mapCtrl.whenReady();
  renderStops();
  mapCtrl.startRotation();

  const stops = stopsData.value as StopWithVenue[] | null;
  const firstStop = stops?.find((s) => s.venue != null);
  if (firstStop?.venue) {
    mapCtrl.flyTo(
      { lng: firstStop.venue.lng, lat: firstStop.venue.lat },
      { zoom: 4, pitch: 40, duration: 2000 },
    );
  }
});

onUnmounted(() => {
  prefetcher.destroy();
  mapCtrl?.destroy();
  mapCtrl = null;
});

watch(
  stopsData,
  () => {
    renderStops();
    if (selectedStop.value) {
      const stops = stopsData.value;
      const updated = stops?.find((s) => s.id === selectedStop.value!.id);
      selectedStop.value = updated ?? null;
    }
  },
  { deep: true },
);

function renderStops() {
  if (!mapCtrl) return;

  const stops = stopsData.value;
  if (!stops) return;

  const withVenue = (stops as StopWithVenue[]).filter((stop) => stop.venue != null);

  const stopMapData: StopMapData[] = withVenue.map((stop) => ({
    id: stop.id,
    name: stop.venue!.name,
    lng: stop.venue!.lng,
    lat: stop.venue!.lat,
  }));

  const dates = withVenue.map((stop) =>
    stop.date instanceof Date ? stop.date : new Date(stop.date),
  );

  mapCtrl.setStops(stopMapData, dates);
}
</script>

<style>
:root {
  /* Surfaces */
  --bg-base: #08090d;
  --bg-surface: #11131a;
  --bg-elevated: #1a1c25;
  --bg-input: rgba(255, 255, 255, 0.04);
  --border-subtle: rgba(255, 255, 255, 0.08);
  --border-focus: #ff2d7b;

  /* Accent */
  --accent-primary: #ff2d7b;
  --accent-secondary: #00e5cc;
  --accent-primary-muted: rgba(255, 45, 123, 0.15);
  --accent-secondary-muted: rgba(0, 229, 204, 0.12);

  /* Status */
  --status-confirmed: #00dd6e;
  --status-confirmed-bg: rgba(0, 221, 110, 0.12);
  --status-tentative: #ffb800;
  --status-tentative-bg: rgba(255, 184, 0, 0.12);
  --status-cancelled: #ff4444;
  --status-cancelled-bg: rgba(255, 68, 68, 0.12);

  /* Text */
  --text-primary: #ffffff;
  --text-secondary: #c0bdc6;
  --text-muted: #8a8694;
  --text-inverse: #08090d;

  /* Typography */
  --font-display: "Syne", sans-serif;
  --font-body: "Urbanist", sans-serif;
  --font-mono: "IBM Plex Mono", monospace;

  /* Effects */
  --shadow-elevated: 0 8px 32px rgba(0, 0, 0, 0.6);
  --shadow-popover: 0 4px 20px rgba(0, 0, 0, 0.5);
  --radius-sm: 4px;
  --radius-md: 8px;
  --radius-lg: 12px;
  --radius-pill: 100px;

  /* Transitions */
  --ease-smooth: cubic-bezier(0.4, 0, 0.2, 1);
  --duration-fast: 0.15s;
  --duration-normal: 0.3s;
}

* {
  box-sizing: border-box;
}

body {
  margin: 0;
  padding: 0;
  background: var(--bg-base);
  color: var(--text-primary);
  font-family: var(--font-body);
  -webkit-font-smoothing: antialiased;
}

/* ─── Change 1: Space background ─── */
.space-bg {
  position: relative;
  width: 100vw;
  height: 100vh;
  overflow: hidden;
  background: #000;
}

.space-bg::before {
  content: "";
  position: absolute;
  inset: -50%;
  width: 200%;
  height: 200%;
  z-index: 0;
  background:
    radial-gradient(ellipse at 25% 35%, rgba(30, 15, 80, 0.9) 0%, transparent 55%),
    radial-gradient(ellipse at 75% 55%, rgba(80, 10, 40, 0.7) 0%, transparent 50%),
    radial-gradient(ellipse at 50% 80%, rgba(10, 30, 80, 0.6) 0%, transparent 55%),
    radial-gradient(ellipse at 60% 20%, rgba(60, 5, 50, 0.5) 0%, transparent 45%);
  animation: nebula-drift 60s linear infinite;
}

.space-bg::after {
  content: "";
  position: absolute;
  inset: 0;
  z-index: 0;
  pointer-events: none;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='400' height='400'%3E%3Cfilter id='s'%3E%3CfeTurbulence baseFrequency='0.8' numOctaves='1' seed='2'/%3E%3CfeColorMatrix values='0 0 0 9 -4 0 0 0 9 -4 0 0 0 9 -4 0 0 0 0 0.4'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23s)'/%3E%3C/svg%3E");
  background-size: 400px 400px;
  animation: star-twinkle 8s ease-in-out infinite alternate;
}

@keyframes nebula-drift {
  0% {
    transform: rotate(0deg);
  }
  100% {
    transform: rotate(360deg);
  }
}

@keyframes star-twinkle {
  0% {
    opacity: 0.3;
  }
  100% {
    opacity: 0.9;
  }
}

.space-bg #map {
  position: relative;
  z-index: 1;
  background: transparent;
}

/* Noise texture overlay for the sheet */
.noise-texture::before {
  content: "";
  position: absolute;
  inset: 0;
  opacity: 0.03;
  pointer-events: none;
  background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)'/%3E%3C/svg%3E");
}

.control-bar {
  position: absolute;
  top: 16px;
  left: 16px;
  z-index: 10;
  background: rgba(8, 9, 13, 0.7);
  backdrop-filter: blur(20px);
  -webkit-backdrop-filter: blur(20px);
  padding: 16px 20px;
  border-radius: var(--radius-lg);
  border: 1px solid rgba(255, 255, 255, 0.06);
  box-shadow:
    0 0 0 1px rgba(255, 45, 123, 0.08),
    0 8px 32px rgba(0, 0, 0, 0.4);
  display: flex;
  flex-direction: column;
  gap: 12px;
  animation: control-fade-in 1.5s ease-out both;
}

@keyframes control-fade-in {
  0% {
    opacity: 0;
    transform: translateY(-12px);
  }
  100% {
    opacity: 1;
    transform: translateY(0);
  }
}

.control-bar__brand {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.band-name-row {
  min-height: 20px;
}

.band-name {
  font-family: var(--font-display);
  font-size: 22px;
  font-weight: 800;
  color: var(--text-primary);
  letter-spacing: 0.08em;
  text-transform: uppercase;
  line-height: 1;
}

.band-name.editable {
  cursor: pointer;
  transition: color var(--duration-fast);
}

.band-name.editable:hover {
  color: var(--text-primary);
}

.band-name-pencil {
  font-size: 14px;
  margin-left: 6px;
  color: var(--text-muted);
  transition: color var(--duration-fast);
}

.band-name.editable:hover .band-name-pencil {
  color: var(--accent-primary);
}

.band-name-input {
  font-family: var(--font-display);
  font-size: 22px;
  font-weight: 800;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--text-primary);
  background: var(--bg-input);
  border: 1px solid var(--accent-primary);
  border-radius: var(--radius-sm);
  padding: 2px 6px;
  outline: none;
  width: 100%;
}

.wordmark {
  display: flex;
  align-items: baseline;
  gap: 3px;
  font-family: var(--font-mono);
  font-size: 10px;
  letter-spacing: 0.2em;
  text-transform: uppercase;
  line-height: 1;
  opacity: 0.7;
}

.wordmark__world {
  font-weight: 400;
  color: var(--text-secondary);
  animation: wordmark-in 1s ease-out both;
}

.wordmark__tour {
  font-weight: 800;
  background: linear-gradient(135deg, #ff2d7b 0%, #ff6b9d 50%, #ff2d7b 100%);
  background-clip: text;
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  filter: drop-shadow(0 0 12px rgba(255, 45, 123, 0.5))
    drop-shadow(0 0 40px rgba(255, 45, 123, 0.2));
  animation:
    wordmark-in 1s ease-out 0.15s both,
    glow-pulse 4s ease-in-out 2s infinite alternate;
}

@keyframes wordmark-in {
  0% {
    opacity: 0;
    transform: translateY(-8px);
    letter-spacing: 0.5em;
  }
  100% {
    opacity: 1;
    transform: translateY(0);
    letter-spacing: 0.2em;
  }
}

@keyframes glow-pulse {
  0% {
    filter: drop-shadow(0 0 12px rgba(255, 45, 123, 0.5))
      drop-shadow(0 0 40px rgba(255, 45, 123, 0.2));
  }
  100% {
    filter: drop-shadow(0 0 18px rgba(255, 45, 123, 0.7))
      drop-shadow(0 0 60px rgba(255, 45, 123, 0.3));
  }
}

.control-bar__row {
  display: flex;
  align-items: center;
  gap: 8px;
}

/* ─── Change 3: SyntheticUserSwitcher styling ─── */
.control-bar label {
  font-size: 0;
  line-height: 0;
  display: inline-flex;
  align-items: center;
}

.control-bar select {
  font-family: var(--font-mono);
  font-size: 12px;
  color: var(--text-primary);
  background: var(--bg-elevated);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-sm);
  padding: 4px 24px 4px 8px;
  cursor: pointer;
  outline: none;
  -webkit-appearance: none;
  -moz-appearance: none;
  appearance: none;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6' fill='none'%3E%3Cpath d='M1 1l4 4 4-4' stroke='%23a09da6' stroke-width='1.5' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right 8px center;
  transition:
    border-color var(--duration-fast) var(--ease-smooth),
    box-shadow var(--duration-fast) var(--ease-smooth);
}

.control-bar select:hover {
  border-color: var(--accent-primary);
  box-shadow: 0 0 8px rgba(255, 45, 123, 0.15);
}

.control-bar select:focus {
  border-color: var(--accent-primary);
  box-shadow: 0 0 0 2px rgba(255, 45, 123, 0.2);
}

/* Hide Add / Remove buttons from SyntheticUserSwitcher */
.control-bar label + button,
.control-bar label ~ button:not(.btn-auth) {
  display: none;
}

/* ─── Public label with live dot ─── */
.public-label {
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--text-muted);
  letter-spacing: 0.04em;
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

.public-label::before {
  content: "";
  display: inline-block;
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--status-confirmed);
  box-shadow: 0 0 6px rgba(0, 221, 110, 0.5);
  flex-shrink: 0;
}

/* ─── Auth button ─── */
.btn-auth {
  font-family: var(--font-display);
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  padding: 5px 12px;
  border-radius: var(--radius-sm);
  border: 1px solid var(--border-subtle);
  background: none;
  color: var(--text-secondary);
  cursor: pointer;
  transition:
    color var(--duration-fast) var(--ease-smooth),
    border-color var(--duration-fast) var(--ease-smooth),
    box-shadow var(--duration-normal) var(--ease-smooth);
}

.btn-auth:hover {
  color: var(--accent-primary);
  border-color: var(--accent-primary);
  box-shadow: 0 0 12px rgba(255, 45, 123, 0.2);
}

/* ─── Tour button ─── */
.tour-btn {
  position: fixed;
  bottom: 84px;
  right: 24px;
  z-index: 15;
  font-family: var(--font-display);
  font-size: 13px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  padding: 10px 20px;
  border-radius: var(--radius-pill);
  border: 1px solid var(--border-subtle);
  background: var(--bg-elevated);
  color: var(--accent-secondary);
  cursor: pointer;
  transition:
    right var(--duration-normal) var(--ease-smooth),
    color var(--duration-fast),
    border-color var(--duration-fast);
}

.tour-btn.sheet-open {
  right: 484px;
}

.tour-btn:hover:not(:disabled) {
  color: var(--accent-primary);
  border-color: var(--accent-primary);
}

.tour-btn:disabled {
  opacity: 0.6;
  cursor: default;
}

.tour-btn.touring {
  color: var(--accent-primary);
  border-color: var(--accent-primary);
}

/* ─── Splash modal ─── */
.splash-overlay {
  position: fixed;
  inset: 0;
  z-index: 100;
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgba(0, 0, 0, 0.6);
  backdrop-filter: blur(6px);
  -webkit-backdrop-filter: blur(6px);
}

.splash-modal {
  background: var(--bg-elevated);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-lg);
  padding: 32px 40px;
  text-align: center;
  box-shadow: var(--shadow-elevated);
  animation: splash-in 0.4s var(--ease-smooth) both;
}

@keyframes splash-in {
  0% {
    opacity: 0;
    transform: scale(0.92) translateY(12px);
  }
  100% {
    opacity: 1;
    transform: scale(1) translateY(0);
  }
}

.splash-text {
  margin: 0 0 20px;
  font-family: var(--font-display);
  font-size: 18px;
  font-weight: 700;
  color: var(--text-primary);
  letter-spacing: 0.02em;
}

.splash-btn {
  font-family: var(--font-display);
  font-size: 13px;
  font-weight: 600;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  padding: 8px 24px;
  border-radius: var(--radius-pill);
  border: none;
  background: var(--accent-primary);
  color: var(--text-inverse);
  cursor: pointer;
  transition: opacity var(--duration-fast);
}

.splash-btn:hover {
  opacity: 0.9;
}

.splash-fade-enter-active,
.splash-fade-leave-active {
  transition: opacity 0.3s var(--ease-smooth);
}

.splash-fade-enter-from,
.splash-fade-leave-to {
  opacity: 0;
}
</style>
