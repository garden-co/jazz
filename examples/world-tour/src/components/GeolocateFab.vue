<template>
  <button
    v-if="supported"
    class="geolocate-fab"
    :class="{ shifted: sheetOpen, loading }"
    :disabled="loading"
    @click="requestLocation"
    aria-label="Find nearest stop to my location"
  >
    <span v-if="loading" class="spinner" />
    <span v-else class="icon">◎</span>
  </button>
  <div v-if="error" class="geo-toast" @animationend="error = ''">
    {{ error }}
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from "vue";

defineProps<{
  sheetOpen: boolean;
}>();

const emit = defineEmits<{
  locate: [coords: { lat: number; lng: number }];
}>();

const supported = ref(false);
const loading = ref(false);
const error = ref("");

onMounted(() => {
  supported.value = "geolocation" in navigator;
});

function requestLocation() {
  if (loading.value) return;

  loading.value = true;
  error.value = "";

  navigator.geolocation.getCurrentPosition(
    (position) => {
      loading.value = false;
      emit("locate", {
        lat: position.coords.latitude,
        lng: position.coords.longitude,
      });
    },
    () => {
      loading.value = false;
      error.value = "Location unavailable";
    },
    { enableHighAccuracy: false, timeout: 10000, maximumAge: 60000 },
  );
}
</script>

<style scoped>
@import "../styles/geolocate-fab.css";
</style>
