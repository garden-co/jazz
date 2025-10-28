<template>
  <div
    class="p-3 bg-white border-t shadow-2xl mt-auto dark:bg-transparent dark:border-stone-800 flex gap-1"
  >
    <ImageInput @image-change="handleImageChange" />
    
    <div class="flex-1">
      <label class="sr-only" :for="inputId">Type a message and press Enter</label>
      <input
        :id="inputId"
        v-model="inputValue"
        class="rounded-full py-2 px-4 text-sm border block w-full dark:bg-black dark:text-white dark:border-stone-700"
        placeholder="Type a message and press Enter"
        maxlength="2048"
        @keydown.enter.prevent="submitMessage"
      />
    </div>
  </div>
</template>

<script lang="ts">
import { defineComponent, ref } from "vue";
import ImageInput from "./ImageInput.vue";

export default defineComponent({
  name: "ChatInput",
  components: {
    ImageInput,
  },
  emits: ["submit", "imageSubmit"],
  setup(_, { emit }) {
    const inputId = `input-${Math.random().toString(36).substr(2, 9)}`;
    const inputValue = ref("");

    function submitMessage() {
      if (!inputValue.value) return;
      emit("submit", inputValue.value);
      inputValue.value = "";
    }

    function handleImageChange(file: File | undefined) {
      if (file) {
        emit("imageSubmit", file);
      }
    }

    return {
      inputId,
      inputValue,
      submitMessage,
      handleImageChange,
    };
  },
});
</script>
