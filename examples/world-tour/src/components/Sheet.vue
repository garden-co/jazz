<template>
  <div class="sheet noise-texture" :class="{ open }" @transitionend="onTransitionEnd">
    <button class="sheet-close" @click="$emit('close')">×</button>
    <div class="sheet-content">
      <slot />
    </div>
  </div>
</template>

<script setup lang="ts">
const props = defineProps<{
  open: boolean;
}>();

const emit = defineEmits<{
  close: [];
  closed: [];
}>();

function onTransitionEnd() {
  if (!props.open) {
    emit("closed");
  }
}
</script>

<style scoped>
@import "../styles/sheet.css";
</style>
