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
.sheet {
  position: fixed;
  top: 0;
  right: 0;
  width: 460px;
  height: 100vh;
  background: var(--bg-surface);
  color: var(--text-primary);
  z-index: 20;
  transform: translateX(100%);
  transition: transform var(--duration-normal) var(--ease-smooth);
  border-left: 1px solid var(--border-subtle);
  display: flex;
  flex-direction: column;
  font-family: var(--font-body);
}

.sheet.open {
  transform: translateX(0);
}

.sheet-close {
  position: absolute;
  top: 14px;
  right: 14px;
  background: none;
  border: none;
  color: var(--text-muted);
  font-family: var(--font-display);
  font-size: 28px;
  cursor: pointer;
  padding: 4px 10px;
  line-height: 1;
  border-radius: var(--radius-sm);
  transition: color var(--duration-fast);
  z-index: 1;
}

.sheet-close:hover {
  color: var(--accent-primary);
}

.sheet-content {
  flex: 1;
  overflow-y: auto;
  padding: 48px 24px 28px;
}
</style>
