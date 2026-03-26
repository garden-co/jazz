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
import { app } from "../../schema/app.js";

const props = defineProps<{
  bandId: string;
}>();

const db = useDb();
const session = useSession();
const canEdit = !!session;

const bandsWithLogo = useAll(app.bands.include({ logoFile: { parts: true } }));
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
.band-logo {
  position: relative;
  width: 40px;
  height: 40px;
  border-radius: var(--radius-md);
  overflow: hidden;
  flex-shrink: 0;
  background: rgba(255, 255, 255, 0.08);
}

.band-logo__image {
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
}

.band-logo__overlay {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgba(255, 45, 123, 0.5);
  color: var(--text-primary);
  font-family: var(--font-body);
  font-size: 10px;
  cursor: pointer;
  opacity: 0;
  transition: opacity var(--duration-fast) ease;
}

.band-logo:hover .band-logo__overlay {
  opacity: 1;
}

.band-logo__upload-btn {
  width: 100%;
  height: 100%;
  border: 1.5px dashed var(--text-muted);
  border-radius: var(--radius-md);
  background: transparent;
  color: var(--text-muted);
  font-family: var(--font-display);
  font-size: 18px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition:
    border-color var(--duration-fast) ease,
    color var(--duration-fast) ease;
}

.band-logo__upload-btn:hover {
  border-color: var(--accent-primary);
  color: var(--accent-primary);
}
</style>
