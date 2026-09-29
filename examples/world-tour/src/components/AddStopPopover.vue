<template>
  <div class="popover" role="dialog" aria-label="Add a stop" :style="{ left: `${x}px`, top: `${y}px` }">
    <p>Add a stop here?</p>
    <div class="actions">
      <Button variant="primary" @click="emit('confirm')">Add stop</Button>
      <Button variant="ghost" @click="emit('dismiss')">Cancel</Button>
    </div>
  </div>
</template>

<script setup lang="ts">
import Button from "./ui/Button.vue";

defineProps<{ x: number; y: number }>();
const emit = defineEmits<{ confirm: []; dismiss: [] }>();
</script>
