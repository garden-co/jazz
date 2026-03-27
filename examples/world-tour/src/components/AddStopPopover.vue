<template>
  <div v-if="visible" class="popover" :style="{ left: x + 'px', top: y + 'px' }">
    <p class="popover-label">Add stop here?</p>
    <div class="popover-actions">
      <button class="popover-btn confirm" @click="$emit('confirm')">Add</button>
      <button class="popover-btn dismiss" @click="$emit('dismiss')">Cancel</button>
    </div>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  x: number;
  y: number;
  visible: boolean;
}>();

defineEmits<{
  confirm: [];
  dismiss: [];
}>();
</script>

<style scoped>
@import "../styles/add-stop-popover.css";
</style>
