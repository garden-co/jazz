<script lang="ts">
import { type FileStream, ImageDefinition } from "jazz-tools";
import { highestResAvailable } from "jazz-tools/media";
import { onDestroy } from "svelte";
import type { HTMLImgAttributes } from "svelte/elements";
import { CoState } from "../jazz.class.svelte";

interface ImageProps extends Omit<HTMLImgAttributes, "width" | "height"> {
  imageId: string;
  width?: number | "original";
  height?: number | "original";
}

const { imageId, width, height, ...rest }: ImageProps = $props();

// Create reactive state for the image
const imageState = new CoState(ImageDefinition, imageId);

let src: string | undefined = $state(imageState.current?.placeholderDataURL);

const dimensions = $derived.by<{
  width: number | undefined;
  height: number | undefined;
}>(() => {
  const w =
    width === "original" ? imageState.current?.originalSize?.[0] : width;
  const h =
    height === "original" ? imageState.current?.originalSize?.[1] : height;

  return { width: w, height: h };
});

$effect(() => {
  const image = imageState.current;
  if (!image) return;

  src = image.placeholderDataURL;
  let lastBestImage: FileStream | null = null;

  const unsub = image.subscribe({}, (update) => {
    const bestImage = highestResAvailable(
      update,
      dimensions.width || dimensions.height || 9999,
      dimensions.height || dimensions.width || 9999,
    );

    if (!bestImage) return;

    if (lastBestImage === bestImage.image) return;

    const blob = bestImage.image.toBlob();

    if (blob) {
      if (src) URL.revokeObjectURL(src);
      src = URL.createObjectURL(blob);
      lastBestImage = bestImage.image;
    }
  });

  return unsub;
});

// Cleanup object URL on component destroy
onDestroy(() => {
  if (src) {
    URL.revokeObjectURL(src);
  }
});
</script>

{#if imageState.current}
  <img
    {src}
    width={dimensions.width}
    height={dimensions.height}
    {...rest}
  />
{/if}
