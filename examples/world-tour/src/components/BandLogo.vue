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
import { ref } from "vue";

defineProps<{
  logoUrl: string | null;
  canEdit: boolean;
}>();

const emit = defineEmits<{
  upload: [file: File];
}>();

const fileInput = ref<HTMLInputElement | null>(null);

function triggerFileInput() {
  fileInput.value?.click();
}

function onFileSelected(event: Event) {
  const input = event.target as HTMLInputElement;
  const file = input.files?.[0];
  if (!file) return;

  emit("upload", file);

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
