<template>
  <button
    :type="type"
    class="button"
    :data-variant="variant"
    :data-icon-only="iconOnly || undefined"
  >
    <slot />
  </button>
</template>

<script setup lang="ts">
// Mirrors the design system's Button: primary, secondary, ghost and destructive.
withDefaults(
  defineProps<{
    variant?: "primary" | "secondary" | "ghost" | "destructive";
    type?: "button" | "submit";
    iconOnly?: boolean;
  }>(),
  { variant: "secondary", type: "button", iconOnly: false },
);
</script>
