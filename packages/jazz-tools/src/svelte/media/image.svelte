<script lang="ts">
import { type FileStream, ImageDefinition } from "jazz-tools";
import { getBestImage } from "jazz-tools/media";
import { onDestroy } from "svelte";
import type { HTMLImgAttributes } from "svelte/elements";
import { CoState } from "../jazz.class.svelte";

interface ImageProps extends HTMLImgAttributes {
  imageId: string;
  alt: string;
  width?: number;
  height?: number;
}

const { imageId, alt, width, height, ...rest }: ImageProps = $props();

// Create reactive state for the image
const imageState = new CoState(ImageDefinition, imageId);

let src: string | undefined = $state(imageState.current?.placeholderDataURL);

const dimensions = $derived.by<{
  width: number | undefined;
  height: number | undefined;
}>(() => {
  if (width || height) {
    return { width, height };
  }
  return {
    width: imageState.current?.originalSize?.[0],
    height: imageState.current?.originalSize?.[1],
  };
});

$effect(() => {
  const image = imageState.current;
  if (!image) return;

  src = image.placeholderDataURL;
  let lastBestImage: FileStream | null = null;

  const unsub = image.subscribe({}, (update) => {
    const bestImage = getBestImage(
      update,
      dimensions.width || dimensions.height || Infinity,
      dimensions.height || dimensions.width || Infinity,
    );

    console.log("Found best image", bestImage);

    if (!bestImage) return;

    if (lastBestImage === bestImage) return;

    const blob = bestImage.toBlob();
    console.log("Blob", blob);

    if (blob) {
      if (src) URL.revokeObjectURL(src);
      src = URL.createObjectURL(blob);
      lastBestImage = bestImage;
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
    {alt}
    width={dimensions.width}
    height={dimensions.height}
    {...rest}
  />
{/if}
