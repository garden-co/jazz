<template>
  <Button
    v-if="supported"
    icon-only
    :disabled="loading"
    aria-label="Find the stop nearest to me"
    title="Find the stop nearest to me"
    @click="locate"
  >
    <Icon name="locate" />
  </Button>
  <p v-if="error" class="toast" role="status" @animationend="error = ''">{{ error }}</p>
</template>

<script setup lang="ts">
import { ref } from "vue";
import Button from "./ui/Button.vue";
import Icon from "./ui/Icon.vue";

const emit = defineEmits<{ locate: [coords: { lat: number; lng: number }] }>();

const supported = "geolocation" in navigator;
const loading = ref(false);
const error = ref("");

function locate() {
  loading.value = true;
  error.value = "";
  navigator.geolocation.getCurrentPosition(
    ({ coords }) => {
      loading.value = false;
      emit("locate", { lat: coords.latitude, lng: coords.longitude });
    },
    () => {
      loading.value = false;
      error.value = "Your location isn't available";
    },
    { enableHighAccuracy: false, timeout: 10000, maximumAge: 60000 },
  );
}
</script>
