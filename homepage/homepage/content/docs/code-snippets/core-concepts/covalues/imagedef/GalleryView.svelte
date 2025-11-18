<script lang="ts">
  import { ImageDefinition, co } from 'jazz-tools';
  import { Image } from 'jazz-tools/svelte';
  let { image }: { image: co.loaded<typeof ImageDefinition> } = $props();
</script>

<Image
  imageId={image.$jazz.id}
  alt="Profile"
  class="h-auto max-h-[20rem] max-w-full rounded-t-xl mb-1"
/>
