<template>
  <div>
    <div class="control-bar">
      <div class="control-bar__brand">
        <span class="control-bar__title">WORLDTOUR</span>
      </div>
      <div class="control-bar__row">
        <BandLogo v-if="firstBandId" :logoUrl="logoUrl" :canEdit="canEdit" @upload="onLogoUpload" />
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
        :canEdit="canEdit"
        @selectStop="onCalendarSelectStop"
        @reschedule="onReschedule"
      />
      <StopDetail
        v-if="sheetMode === 'detail' && selectedStop"
        :stop="selectedStop"
        :canEdit="canEdit"
        @update="onStopUpdate(selectedStop!.id, $event)"
        @delete="onStopDelete(selectedStop!.id)"
      />
      <StopCreateForm
        v-if="sheetMode === 'create'"
        :lat="createLat"
        :lng="createLng"
        :venues="venuesData ?? []"
        @create="onStopCreate"
        @cancel="closeSheet"
      />
    </Sheet>

    <GeolocateFab :sheetOpen="sheetOpen" @locate="onGeolocate" />

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
import { onMounted, onUnmounted, ref, computed, watch } from "vue";
import { useAll, useDb, useSession, SyntheticUserSwitcher } from "jazz-tools/vue";
import { app } from "../schema/app.js";
import type { StopWithIncludes } from "../schema/app.js";
import { MapController, type StopMapData } from "./lib/map-controller";
import { findNearestStop } from "./lib/nearest-stop";
import type { StopWithLocation } from "./lib/nearest-stop";
import { seedIfEmpty } from "./seed-loader";
import Sheet from "./components/Sheet.vue";
import StopDetail from "./components/StopDetail.vue";
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

seedIfEmpty(db, session?.user_id).catch((err) => console.error("Failed to seed data:", err));

type StopWithVenue = StopWithIncludes<{ venue: true }>;
const selectedStop = ref<StopWithVenue | null>(null);
const sheetOpen = ref(false);
const sheetMode = ref<"detail" | "create">("detail");

const createLat = ref(0);
const createLng = ref(0);

const popoverVisible = ref(false);
const popoverX = ref(0);
const popoverY = ref(0);
const popoverLat = ref(0);
const popoverLng = ref(0);

const allStopsQuery = app.stops.include({ venue: true }).orderBy("date", "asc");
const confirmedStopsQuery = app.stops
  .where({ status: "confirmed" })
  .include({ venue: true })
  .orderBy("date", "asc");
const stopsQuery = canEdit ? allStopsQuery : confirmedStopsQuery;
const stopsData = useAll(stopsQuery);

const bandsData = useAll(app.bands.limit(1));
const bandsWithLogo = useAll(app.bands.include({ logoFile: { parts: true } }));
const venuesData = useAll(app.venues);

const firstBandId = computed(() => {
  const bands = bandsData.value;
  return bands && bands.length > 0 ? bands[0].id : null;
});

if (session) {
  watch(bandsData, (bands) => {
    if (!bands || bands.length === 0) return;
    db.all(app.members.where({ userId: session.user_id })).then((rows) => {
      if (rows.length > 0) return;
      db.insert(app.members, { bandId: bands[0].id, userId: session.user_id });
    });
  }, { immediate: true });
}

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

function onStopUpdate(id: string, data: Record<string, unknown>) {
  db.update(app.stops, id, data);
}

function onStopDelete(id: string) {
  db.delete(app.stops, id);
  closeSheet();
}

function onStopCreate(stopData: {
  venueMode: "new" | "existing";
  selectedVenueId?: string;
  newVenue?: {
    name: string;
    city: string;
    country: string;
    lat: number;
    lng: number;
    capacity?: number;
  };
  date: Date;
  status: "confirmed" | "tentative" | "cancelled";
  publicDescription: string;
  privateNotes?: string;
}) {
  const band = bandsData.value?.[0];
  if (!band) return;

  let venueId: string;
  if (stopData.venueMode === "existing" && stopData.selectedVenueId) {
    venueId = stopData.selectedVenueId;
  } else if (stopData.newVenue) {
    const venue = db.insert(app.venues, stopData.newVenue);
    venueId = venue.id;
  } else {
    return;
  }

  db.insert(app.stops, {
    bandId: band.id,
    venueId,
    date: stopData.date,
    status: stopData.status,
    publicDescription: stopData.publicDescription,
    ...(stopData.privateNotes ? { privateNotes: stopData.privateNotes } : {}),
  });

  closeSheet();
}

function onReschedule(stopId: string, newDate: Date) {
  db.update(app.stops, stopId, { date: newDate });
}

const logoUrl = ref<string | null>(null);

watch(
  () => {
    const bands = bandsWithLogo.value;
    if (!bands) return null;
    const band = bands.find((b) => b.id === firstBandId.value);
    if (!band) return null;
    return band.logoFile ?? null;
  },
  (logoFile) => {
    if (!logoFile) {
      if (logoUrl.value) {
        URL.revokeObjectURL(logoUrl.value);
        logoUrl.value = null;
      }
      return;
    }

    let isActive = true;

    (async () => {
      try {
        const blob = await db.loadFileAsBlob(app, logoFile);
        if (!isActive) return;

        const nextUrl = URL.createObjectURL(blob);
        if (logoUrl.value) {
          URL.revokeObjectURL(logoUrl.value);
        }
        logoUrl.value = nextUrl;
      } catch (err) {
        if (!isActive) return;
        console.error("Failed to load band logo:", err);
      }
    })();

    return () => {
      isActive = false;
    };
  },
  { immediate: true },
);

onUnmounted(() => {
  if (logoUrl.value) {
    URL.revokeObjectURL(logoUrl.value);
  }
});

async function onLogoUpload(file: File) {
  try {
    const insertedFile = await db.createFileFromBlob(app, file);
    const band = bandsData.value?.[0];
    if (band) db.update(app.bands, band.id, { logoFileId: insertedFile.id });
  } catch (err) {
    console.error("Failed to upload band logo:", err);
  }
}

function selectStop(stop: StopWithVenue) {
  selectedStop.value = stop;
  sheetMode.value = "detail";
  sheetOpen.value = true;

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
  --text-primary: #f5f2ed;
  --text-secondary: #a09da6;
  --text-muted: #6b6677;
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

/* Noise texture overlay for the sheet */
.noise-texture::before {
  content: "";
  position: absolute;
  inset: 0;
  opacity: 0.03;
  pointer-events: none;
  background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)'/%3E%3C/svg%3E");
}

/* Control bar */
.control-bar {
  position: absolute;
  top: 12px;
  left: 12px;
  z-index: 10;
  background: var(--bg-surface);
  backdrop-filter: blur(12px);
  padding: 10px 16px;
  border-radius: var(--radius-lg);
  border-bottom: 2px solid var(--accent-primary);
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.control-bar__brand {
  display: flex;
  align-items: center;
}

.control-bar__title {
  font-family: var(--font-display);
  font-weight: 800;
  font-size: 14px;
  letter-spacing: 0.15em;
  text-transform: uppercase;
  color: var(--accent-primary);
}

.control-bar__row {
  display: flex;
  align-items: center;
  gap: 8px;
}

.btn-auth {
  font-family: var(--font-display);
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  padding: 4px 10px;
  border-radius: var(--radius-sm);
  border: 1px solid var(--border-subtle);
  background: none;
  color: var(--text-secondary);
  cursor: pointer;
  transition:
    color var(--duration-fast),
    border-color var(--duration-fast);
}

.btn-auth:hover {
  color: var(--accent-primary);
  border-color: var(--accent-primary);
}

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

.public-label {
  font-family: var(--font-mono);
  font-size: 11px;
  color: var(--text-muted);
  letter-spacing: 0.04em;
}
</style>
