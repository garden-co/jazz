<template>
  <div>
    <button
      type="button"
      aria-label="Send image"
      title="Send image"
      @click="onUploadClick"
      class="text-stone-500 p-1.5 rounded-full hover:bg-stone-100 hover:text-stone-800 dark:hover:bg-stone-800 dark:hover:text-stone-200 transition-colors"
    >
      <svg
        width="24"
        height="24"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        stroke-width="1.5"
        stroke-linecap="round"
        stroke-linejoin="round"
      >
        <rect width="18" height="18" x="3" y="3" rx="2" ry="2" />
        <circle cx="9" cy="9" r="2" />
        <path d="m21 15-3.086-3.086a2 2 0 0 0-2.828 0L6 21" />
      </svg>
    </button>

    <label class="sr-only">
      Image
      <input
        ref="inputRef"
        type="file"
        accept="image/png, image/jpeg, image/gif"
        @change="onImageChange"
      />
    </label>
  </div>
</template>

<script lang="ts">
import { defineComponent, ref } from "vue";

export default defineComponent({
  name: "ImageInput",
  emits: ["imageChange"],
  setup(_, { emit }) {
    const inputRef = ref<HTMLInputElement>();

    function onUploadClick() {
      inputRef.value?.click();
    }

    function onImageChange(event: Event) {
      const target = event.target as HTMLInputElement;
      emit("imageChange", target.files?.[0]);
    }

    return {
      inputRef,
      onUploadClick,
      onImageChange,
    };
  },
});
</script>
