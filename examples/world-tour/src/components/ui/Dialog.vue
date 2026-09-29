<template>
  <!-- A native modal dialog: showModal() traps focus, handles Esc and focuses the
       first focusable element (or the one marked autofocus). -->
  <dialog
    ref="dialog"
    class="dialog"
    :aria-label="title"
    @cancel.prevent="emit('close')"
    @click="closeOnBackdrop"
  >
    <div class="dialog__body">
      <header class="dialog__header">
        <h2 class="dialog__title">{{ title }}</h2>
        <Button variant="ghost" icon-only aria-label="Close" @click="emit('close')">
          <Icon name="close" />
        </Button>
      </header>
      <slot />
    </div>
  </dialog>
</template>

<script setup lang="ts">
import { onMounted, useTemplateRef } from "vue";
import Button from "./Button.vue";
import Icon from "./Icon.vue";

defineProps<{ title: string }>();
const emit = defineEmits<{ close: [] }>();

const dialog = useTemplateRef<HTMLDialogElement>("dialog");
onMounted(() => dialog.value?.showModal());

// The dialog element itself only receives clicks on its backdrop; the content
// sits in `.dialog__body`.
function closeOnBackdrop(event: MouseEvent) {
  if (event.target === dialog.value) emit("close");
}
</script>
