<template>
  <div class="navigation-breadcrumbs">
    <button
      v-if="canGoBack"
      @click="$emit('go-back')"
      class="back-button"
      title="Go back"
    >
      ← Back
    </button>
    
    <div class="breadcrumb-path">
      <span
        v-for="(page, index) in path"
        :key="page.id"
        class="breadcrumb-item"
      >
        <button
          v-if="index < path.length - 1"
          @click="$emit('navigate-to-index', index)"
          class="breadcrumb-link"
        >
          {{ page.title }}
        </button>
        <span v-else class="breadcrumb-current">
          {{ page.title }}
        </span>
        <span v-if="index < path.length - 1" class="breadcrumb-separator">
          /
        </span>
      </span>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { PageInfo } from "../composables/useJazzInspector.js";

interface Props {
  path: PageInfo[];
  canGoBack: boolean;
}

defineProps<Props>();

defineEmits<{
  "go-back": [];
  "navigate-to-index": [index: number];
}>();
</script>

<style scoped>
.navigation-breadcrumbs {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding: 0.75rem;
  border-bottom: 1px solid var(--j-border-color);
  background: var(--j-foreground);
  font-size: 0.875rem;
}

.back-button {
  background: var(--j-neutral-600);
  color: white;
  border: none;
  border-radius: var(--j-radius-md);
  padding: 0.375rem 0.75rem;
  cursor: pointer;
  font-size: 0.875rem;
  font-weight: 500;
  transition: background-color 0.2s;
}

.back-button:hover {
  background: var(--j-neutral-700);
}

.breadcrumb-path {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  flex: 1;
  min-width: 0;
}

.breadcrumb-item {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.breadcrumb-link {
  background: none;
  border: none;
  color: var(--j-primary-color);
  cursor: pointer;
  padding: 0.25rem 0.5rem;
  border-radius: var(--j-radius-sm);
  font-size: 0.875rem;
  text-decoration: none;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 150px;
}

.breadcrumb-link:hover {
  background: var(--j-foreground);
  text-decoration: underline;
}

.breadcrumb-current {
  color: var(--j-text-color-strong);
  font-weight: 500;
  padding: 0.25rem 0.5rem;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 200px;
}

.breadcrumb-separator {
  color: var(--j-text-color);
  font-weight: normal;
}
</style>
