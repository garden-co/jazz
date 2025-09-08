<template>
  <div class="h-auto max-w-full rounded-t-xl mb-1">
    <Image
      :image-id="image.$jazz.id"
      alt="Uploaded image"
      class-names="h-full rounded-t-xl"
      width="original"
      height="original"
    />
  </div>
</template>

<script lang="ts">
import { defineComponent } from "vue";
import { type ImageDefinition } from "jazz-tools";
import { Image } from "community-jazz-vue";

export default defineComponent({
  name: "BubbleImage",
  components: {
    Image,
  },
  props: {
    image: {
      type: Object as () => ImageDefinition,
      required: true,
    },
  },
});
</script>
