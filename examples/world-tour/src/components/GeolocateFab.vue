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
.geolocate-fab {
  position: fixed;
  bottom: 24px;
  right: 24px;
  width: 48px;
  height: 48px;
  border-radius: 50%;
  border: 1px solid var(--border-subtle);
  background: var(--bg-elevated);
  color: var(--accent-secondary);
  font-size: 22px;
  cursor: pointer;
  z-index: 15;
  display: flex;
  align-items: center;
  justify-content: center;
  box-shadow: var(--shadow-elevated);
  transition:
    right var(--duration-normal) var(--ease-smooth),
    background var(--duration-fast),
    box-shadow var(--duration-fast),
    transform var(--duration-fast);
}

.geolocate-fab.shifted {
  right: 484px;
}

.geolocate-fab:hover {
  background: var(--bg-surface);
  box-shadow:
    0 0 0 2px var(--accent-primary-muted),
    var(--shadow-elevated);
  transform: scale(1.05);
}

.geolocate-fab:active {
  transform: scale(0.95);
}

.geolocate-fab:disabled {
  cursor: wait;
}

.icon {
  line-height: 1;
}

.spinner {
  width: 20px;
  height: 20px;
  border: 2px solid rgba(255, 45, 123, 0.3);
  border-top-color: var(--accent-primary);
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

.geo-toast {
  position: fixed;
  bottom: 80px;
  right: 24px;
  background: var(--bg-elevated);
  color: var(--status-cancelled);
  padding: 8px 16px;
  border-radius: var(--radius-md);
  font-family: var(--font-body);
  font-size: 13px;
  z-index: 15;
  animation: toast-fade 2.5s ease-out forwards;
  pointer-events: none;
}

@keyframes toast-fade {
  0% {
    opacity: 1;
  }
  70% {
    opacity: 1;
  }
  100% {
    opacity: 0;
  }
}
</style>
