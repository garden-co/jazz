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
    <!-- On a short screen only the content scrolls: the title and the actions
         stay in view, and focusing an action never scrolls the title away. -->
    <header class="dialog__header">
      <h2 class="dialog__title">{{ title }}</h2>
      <Button variant="ghost" icon-only aria-label="Close" @click="emit('close')">
        <Icon name="close" />
      </Button>
    </header>
    <div class="dialog__body">
      <slot />
    </div>
    <footer v-if="$slots.actions" class="dialog__footer actions">
      <slot name="actions" />
    </footer>
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
// sits in its header, body and footer.
function closeOnBackdrop(event: MouseEvent) {
  if (event.target === dialog.value) emit("close");
}
</script>
