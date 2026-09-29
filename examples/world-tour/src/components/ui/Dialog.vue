<template>
  <div class="dialog-backdrop" @click.self="emit('close')" @keydown.esc="emit('close')">
    <section class="dialog" role="dialog" aria-modal="true" :aria-label="title">
      <header class="dialog__header">
        <h2 class="dialog__title">{{ title }}</h2>
        <Button variant="ghost" icon-only aria-label="Close" @click="emit('close')">
          <Icon name="close" />
        </Button>
      </header>
      <slot />
    </section>
  </div>
</template>

<script setup lang="ts">
import Button from "./Button.vue";
import Icon from "./Icon.vue";

defineProps<{ title: string }>();
const emit = defineEmits<{ close: [] }>();
</script>
