<script lang="ts">
  import type { ID } from 'jazz-tools';
  import type { ImageDefinition } from 'jazz-tools';
  import { loadImage } from 'jazz-tools/media';
  import { Image } from 'jazz-tools/svelte';
  import { Download } from 'lucide-svelte';
  import { onMount } from 'svelte';
  import { Button } from '@/components/ui/button';
  import { downloadBlob, formatBytes } from '@/lib/utils';
  import { toast } from 'svelte-sonner';

  interface Props {
    imageId: ID<typeof ImageDefinition>;
  }

  let { imageId }: Props = $props();

  let imageMeta = $state<Awaited<ReturnType<typeof loadImage>> | undefined>(undefined);

  onMount(() => {
    loadImage(imageId).then((img) => {
      if (img) imageMeta = img;
    });
  });

  async function handleDownload() {
    const img = await loadImage(imageId);
    if (!img) {
      toast.error('Could not load image');
      return;
    }
    const blob = img.image.toBlob();
    if (!blob) {
      toast.error('File was corrupted');
      return;
    }
    const metadata = img.image.getMetadata();
    const ext = metadata?.mimeType?.split('/')[1] || 'png';
    downloadBlob(blob, `image-${imageId}.${ext}`);
  }
</script>

<div class="text-foreground my-2 flex flex-col rounded-xl">
  <div class="mb-2 overflow-hidden rounded-xl">
    <Image {imageId} class="max-w-[50vw]" height="original" />
  </div>
  <Button variant="secondary" onclick={handleDownload}>
    <Download class="size-4" />
    Download
    {#if imageMeta?.image}
      ({formatBytes(imageMeta.image.getMetadata()?.totalSizeBytes ?? 0)})
    {/if}
  </Button>
</div>
