<template>
  <div class="band-logo" :class="{ 'has-logo': !!logoUrl }">
    <img v-if="logoUrl" :src="logoUrl" alt="Band logo" class="band-logo__image" />
    <div v-if="canEdit && logoUrl" class="band-logo__overlay" @click="triggerFileInput">Change</div>
    <button
      v-if="canEdit && !logoUrl"
      class="band-logo__upload-btn"
      type="button"
      @click="triggerFileInput"
    >
      +
    </button>
    <input
      ref="fileInput"
      type="file"
      accept="image/*"
      style="display: none"
      @change="onFileSelected"
    />
  </div>
</template>

<script setup lang="ts">
import { ref, watch, onUnmounted } from "vue";
import { useDb, useAll, useSession } from "jazz-tools/vue";
import { app } from "../../schema.js";

const props = defineProps<{
  bandId: string;
}>();

const db = useDb();
const session = useSession();
const canEdit = !!session;

const bandsWithLogo = useAll(app.bands.include({ logoFile: { parts: true } }).limit(1));
const logoUrl = ref<string | null>(null);

watch(
  () => {
    const bands = bandsWithLogo.value;
    if (!bands) return null;
    const band = bands.find((b) => b.id === props.bandId);
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

const fileInput = ref<HTMLInputElement | null>(null);

function triggerFileInput() {
  fileInput.value?.click();
}

async function onFileSelected(event: Event) {
  const input = event.target as HTMLInputElement;
  const file = input.files?.[0];
  if (!file) return;

  try {
    const insertedFile = await db.createFileFromBlob(app, file);
    db.update(app.bands, props.bandId, { logoFileId: insertedFile.id });
  } catch (err) {
    console.error("Failed to upload band logo:", err);
  }

  input.value = "";
}
</script>

<style scoped>
@import "../styles/band-logo.css";
</style>
