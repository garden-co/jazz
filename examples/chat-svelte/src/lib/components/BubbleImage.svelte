<script lang="ts">
  import { type ImageDefinition } from 'jazz-tools';
  import { Image } from 'jazz-tools/svelte';
  let { image }: { image: ImageDefinition } = $props();
</script>

<Image
  imageId={image.$jazz.id}
  alt=""
  class="h-auto max-h-[20rem] max-w-full rounded-t-xl mb-1"
/>
