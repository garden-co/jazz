<template>
  <aside
    class="sheet"
    :class="{ open }"
    :inert="!open"
    @transitionend.self="!open && emit('closed')"
  >
    <header class="sheet__header">
      <h2 class="sheet__title">{{ title }}</h2>
      <Button variant="ghost" icon-only aria-label="Close" @click="emit('close')">
        <Icon name="close" />
      </Button>
    </header>
    <div class="sheet__body">
      <slot />
    </div>
  </aside>
</template>

<script setup lang="ts">
import Button from "./Button.vue";
import Icon from "./Icon.vue";

defineProps<{ open: boolean; title: string }>();
const emit = defineEmits<{ close: []; closed: [] }>();
</script>
