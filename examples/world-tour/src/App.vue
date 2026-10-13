<template>
  <main class="app">
    <div id="map" class="globe" />

    <header class="masthead">
      <div class="masthead__title">
        <input
          v-if="renaming"
          ref="renameInput"
          class="input heading-3"
          aria-label="Band name"
          :value="band?.name"
          @keydown.enter="rename(($event.target as HTMLInputElement).value)"
          @keydown.esc="renaming = false"
          @blur="rename(($event.target as HTMLInputElement).value)"
        />
        <h1 v-else class="heading-3">{{ band?.name ?? "World tour" }}</h1>
        <Button
          v-if="isMember && !renaming"
          variant="ghost"
          icon-only
          aria-label="Rename band"
          @click="startRename"
        >
          <Icon name="pencil" />
        </Button>
      </div>
      <p class="text-supporting">{{ roleLine }}</p>
      <div class="actions">
        <Button v-if="isMember" @click="openSheet({ kind: 'band' })"
          ><Icon name="users" />Band</Button
        >
        <Button v-else-if="userId && band" :disabled="starting" @click="startOwnTour">
          {{ starting ? "Starting…" : "Start your own tour" }}
        </Button>
      </div>
    </header>

    <div class="dock" :class="{ 'sheet-open': sheet }">
      <LocateButton @locate="selectNearest" />
      <Button :disabled="stops.length === 0" @click="touring ? stopTour() : playTour()">
        <Icon :name="touring ? 'stop' : 'play'" />{{ touring ? "Stop tour" : "Play tour" }}
      </Button>
    </div>

    <AddStopPopover
      v-if="popover"
      :x="popover.x"
      :y="popover.y"
      @confirm="openSheet({ kind: 'create', lat: popover.lat, lng: popover.lng })"
      @dismiss="popover = null"
    />

    <Sheet :open="!!sheet" :title="sheetTitle" @close="closeSheet" @closed="shownSheet = null">
      <template v-if="shownSheet?.kind === 'stop'">
        <TourCalendar
          :band-id="shownSheet.bandId"
          :anchor="selectedStop?.date ?? stops[0]?.date ?? today"
          :selected-stop-id="shownSheet.stopId"
          @select-stop="selectStop"
        />
        <StopDetail
          v-if="selectedStop"
          :key="selectedStop.id"
          :stop="selectedStop"
          @deleted="closeSheet"
        />
      </template>
      <StopCreateForm
        v-else-if="shownSheet?.kind === 'create' && band && userId"
        :lat="shownSheet.lat"
        :lng="shownSheet.lng"
        :band-id="band.id"
        :user-id="userId"
        @created="selectStop"
        @cancel="closeSheet"
      />
      <BandPanel
        v-else-if="shownSheet?.kind === 'band' && band && userId"
        :band="band"
        :user-id="userId"
      />
    </Sheet>

    <JoinBand
      v-if="route.inviteCode && band && userId && memberships && !isMember"
      :band-id="band.id"
      :band-name="band.name"
      :code="route.inviteCode"
      :user-id="userId"
      @done="goToBand(band.id)"
    />
    <TourPoster
      v-else-if="showPoster && band && memberships && !isMember"
      :band-name="band.name"
      :stops="stops"
      @select="selectStop"
      @dismiss="showPoster = false"
    />
    <p v-if="writeError" class="toast" role="alert" @animationend="writeError = ''">
      {{ writeError }}
    </p>
    <StopPoster
      v-if="posterStop && band"
      :stop="posterStop"
      :band-name="band.name"
      @close="posterStopId = null"
    />
  </main>
</template>

<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref, shallowRef, watch } from "vue";
import { useAll, useDb, useSession } from "jazz-tools/vue";
import { app } from "../schema.js";
import { MapController } from "./lib/map-controller";
import { findNearestStop } from "./lib/nearest-stop";
import { useRoute } from "./lib/routes";
import { reportWriteError, writeError } from "./lib/write-errors";
import { claimDemoBand, startDemoTour } from "./seed-loader";
import AddStopPopover from "./components/AddStopPopover.vue";
import BandPanel from "./components/BandPanel.vue";
import JoinBand from "./components/JoinBand.vue";
import LocateButton from "./components/LocateButton.vue";
import StopCreateForm from "./components/StopCreateForm.vue";
import StopDetail from "./components/StopDetail.vue";
import StopPoster from "./components/StopPoster.vue";
import TourCalendar from "./components/TourCalendar.vue";
import TourPoster from "./components/TourPoster.vue";
import Button from "./components/ui/Button.vue";
import Icon from "./components/ui/Icon.vue";
import Sheet from "./components/ui/Sheet.vue";

const db = useDb();
const session = useSession();
const userId = computed(() => session.value?.user.account ?? null);
const { route, goToBand } = useRoute();

// Writes the server rejects later (a permission changed, a conflict) end up
// here, unless the code that made the write waits on it and reports the
// rejection itself (see write-errors.ts): each rejection is reported once.
const stopWriteErrors = db.onMutationError((event) => reportWriteError(event));

// --- Which band we're looking at, and who we are to it -----------------------

const { data: memberships } = useAll(
  () => (userId.value ? app.members.where({ userId: userId.value }) : undefined),
  { tier: "local-first", firstLoadRemoteWaitMs: 5_000 },
);
const { data: someBand } = useAll(app.bands.limit(1), {
  tier: "local-first",
  firstLoadRemoteWaitMs: 5_000,
});

const bandId = computed(
  () => route.value.bandId ?? memberships.value?.[0]?.bandId ?? someBand.value?.[0]?.id ?? null,
);
const { data: bandRows } = useAll(() =>
  bandId.value ? app.bands.where({ id: bandId.value }) : undefined,
);
const band = computed(() => bandRows.value?.[0] ?? null);
const isMember = computed(() => !!memberships.value?.some((m) => m.bandId === bandId.value));
const isOwner = computed(() => !!band.value && band.value.ownerId === userId.value);

const roleLine = computed(() => {
  if (!band.value) return seedFailed.value ? "No tour yet." : "Loading the tour…";
  if (isOwner.value) return "Your band. Click the globe to add a stop.";
  if (isMember.value) return "You're in this band. Click the globe to add a stop.";
  return "Public tour page with confirmed dates";
});

// A fresh app has no bands: start the seeded demo tour, owned by this account.
// If another first visitor wins the race, their band shows up through `someBand`.
let seeding = false;
const seedFailed = ref(false);
watch([userId, memberships, someBand], async ([id, mine, any]) => {
  if (seeding || !id || !mine || !any || mine.length > 0 || any.length > 0 || route.value.bandId)
    return;
  seeding = true;
  try {
    seedFailed.value = !(await claimDemoBand(db, {
      userId: id,
      ownerName: "Tour manager",
      onTourRejected: (error) => {
        console.error("The server did not accept the demo tour", error);
        seedFailed.value = true;
      },
    }));
  } catch (error) {
    console.error("Could not write the demo tour", error);
    seedFailed.value = true;
  } finally {
    seeding = false;
  }
});

const starting = ref(false);
async function startOwnTour() {
  if (!userId.value) return;
  starting.value = true;
  try {
    // Open the new band as soon as it's written locally. `accepted` is the
    // tour's wait, so it owns a rejection by the server (which rolls the band
    // back): it is reported here, and not again by onMutationError.
    const { bandId, accepted } = await startDemoTour(db, {
      userId: userId.value,
      ownerName: "Tour manager",
    });
    accepted.catch((cause: unknown) => reportWriteError(cause));
    goToBand(bandId);
  } finally {
    starting.value = false;
  }
}

// --- The next three weeks of stops, for the globe and the public poster -------
// One query for everybody: the stops policy returns every stop to band members
// and only confirmed ones to the public. The calendar queries its own month.

const today = ref(startOfToday());
const dayTimer = setInterval(() => {
  const now = startOfToday();
  if (now.getTime() !== today.value.getTime()) today.value = now;
}, 60_000);

const { data: stopRows } = useAll(() =>
  bandId.value
    ? app.stops
        .where({
          bandId: bandId.value,
          date: { gte: today.value, lte: new Date(today.value.getTime() + 21 * 86_400_000) },
        })
        .include({ venue: true })
        .orderBy("date", "asc")
        .limit(12)
    : undefined,
);
const stops = computed(() => (stopRows.value ?? []).filter((s) => s.venue));

// --- Sheet, popover and posters -----------------------------------------------

type SheetState =
  | { kind: "stop"; stopId: string; bandId: string }
  | { kind: "create"; lat: number; lng: number }
  | { kind: "band" };
const sheet = ref<SheetState | null>(null);
// Keeps the content rendered while the sheet slides out.
const shownSheet = ref<SheetState | null>(null);
// The selected stop can be outside the next three weeks (picked in the calendar).
const { data: selectedRows } = useAll(() => {
  const s = shownSheet.value;
  return s?.kind === "stop"
    ? app.stops.where({ id: s.stopId }).include({ venue: true })
    : undefined;
});
const selectedStop = computed(() => {
  const s = shownSheet.value;
  const stop = selectedRows.value?.[0];
  return s?.kind === "stop" && stop?.id === s.stopId && stop.venue ? stop : null;
});
const sheetTitle = computed(() => {
  const s = shownSheet.value;
  if (s?.kind === "create") return "New stop";
  if (s?.kind === "band") return band.value?.name ?? "Band";
  return "Tour dates";
});

function openSheet(next: SheetState) {
  popover.value = null;
  sheet.value = shownSheet.value = next;
}
function closeSheet() {
  sheet.value = null;
}

const popover = ref<{ x: number; y: number; lat: number; lng: number } | null>(null);
const showPoster = ref(true);
const posterStopId = ref<string | null>(null);
const posterStop = computed(() => stops.value.find((s) => s.id === posterStopId.value) ?? null);

function selectStop(stopId: string) {
  const stop = stops.value.find((s) => s.id === stopId);
  mapCtrl?.stopTour();
  touring.value = false;
  if (isMember.value && bandId.value) openSheet({ kind: "stop", stopId, bandId: bandId.value });
  else {
    showPoster.value = false;
    posterStopId.value = stopId;
  }
  if (stop?.venue) mapCtrl?.flyTo(stop.venue, { duration: 1500 });
}

// Stops picked in the calendar or just created may not be on the globe's list yet.
watch(
  () => selectedStop.value?.venue,
  (venue, previous) => {
    if (
      venue &&
      venue.id !== previous?.id &&
      !stops.value.some((s) => s.id === selectedStop.value?.id)
    )
      mapCtrl?.flyTo(venue, { duration: 1500 });
  },
);

function selectNearest(point: { lat: number; lng: number }) {
  const nearest = findNearestStop(
    point,
    stops.value.map((s) => ({ id: s.id, lat: s.venue!.lat, lng: s.venue!.lng })),
  );
  if (nearest) selectStop(nearest.id);
}

// --- Rename --------------------------------------------------------------------

const renaming = ref(false);
const renameInput = ref<HTMLInputElement | null>(null);
function startRename() {
  renaming.value = true;
  nextTick(() => renameInput.value?.select());
}
function rename(value: string) {
  renaming.value = false;
  const name = value.trim();
  if (band.value && name && name !== band.value.name) db.update(app.bands, band.value.id, { name });
}

// --- Globe ---------------------------------------------------------------------

let mapCtrl: MapController | null = null;
const touring = shallowRef(false);

async function playTour() {
  if (!mapCtrl) return;
  closeSheet();
  touring.value = true;
  await mapCtrl.tour(
    stops.value.map((s) => ({ id: s.id, name: s.venue!.name, ...latLng(s.venue!) })),
  );
  touring.value = false;
}
function stopTour() {
  mapCtrl?.stopTour();
  touring.value = false;
}

function renderStops() {
  mapCtrl?.setStops(
    stops.value.map((s) => ({ id: s.id, name: s.venue!.city, ...latLng(s.venue!) })),
    stops.value.map((s) => s.date),
  );
}

onMounted(async () => {
  mapCtrl = new MapController({ container: "map" });
  mapCtrl.on("stopClick", ({ stopId }) => selectStop(stopId));
  mapCtrl.on("mapClick", ({ lat, lng, x, y }) => {
    popover.value = null;
    if (Math.abs(lat) > 90 || Math.abs(lng) > 180) return;
    if (isMember.value) popover.value = { x, y, lat, lng };
    else selectNearest({ lat, lng });
  });
  await mapCtrl.whenReady();
  renderStops();
  mapCtrl.startRotation();
});

watch(stops, renderStops, { deep: true });

// Face the first stop once the tour has loaded.
watch(
  () => stops.value[0]?.venue,
  (venue, previous) => {
    if (venue && !previous) mapCtrl?.flyTo(venue, { duration: 2000 });
  },
);

onUnmounted(() => {
  stopWriteErrors();
  clearInterval(dayTimer);
  mapCtrl?.destroy();
  mapCtrl = null;
});

function startOfToday() {
  const d = new Date();
  d.setHours(0, 0, 0, 0);
  return d;
}

function latLng(venue: { lat: number; lng: number }) {
  return { lat: venue.lat, lng: venue.lng };
}
</script>
