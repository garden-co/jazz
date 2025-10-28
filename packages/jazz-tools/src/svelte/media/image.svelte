<script lang="ts">
  import { ImageDefinition } from "jazz-tools";
  import { highestResAvailable } from "jazz-tools/media";
  import { onDestroy } from "svelte";
  import { CoState } from "../jazz.class.svelte";
  import type { ImageProps } from "./image.types.js";

  const { imageId, width, height, placeholder, ...rest }: ImageProps = $props();

  const imageState = new CoState(ImageDefinition, () => imageId);
  let lastBestImage: [string, string] | null = null;

  /**
   * For lazy loading, we use the browser's strategy for images with loading="lazy".
   * We use an empty image, and when the browser triggers the load event, we load the best available image.
   * On page loading, if the image url is already in browser's cache, the load event is triggered immediately.
   * This is why we need to use a different blob url for every image.
   */
  let waitingLazyLoading = $state(rest.loading === "lazy");
  const lazyPlaceholder = $derived.by(() =>
    waitingLazyLoading ? URL.createObjectURL(emptyPixelBlob) : undefined,
  );

  const dimensions = $derived.by<{
    width: number | undefined;
    height: number | undefined;
  }>(() => {
    const originalWidth = imageState.current?.originalSize?.[0];
    const originalHeight = imageState.current?.originalSize?.[1];

    // Both width and height are "original"
    if (width === "original" && height === "original") {
      return { width: originalWidth, height: originalHeight };
    }

    // Width is "original", height is a number
    if (width === "original" && typeof height === "number") {
      if (originalWidth && originalHeight) {
        return {
          width: Math.round((height * originalWidth) / originalHeight),
          height,
        };
      }
      return { width: undefined, height };
    }

    // Height is "original", width is a number
    if (height === "original" && typeof width === "number") {
      if (originalWidth && originalHeight) {
        return {
          width,
          height: Math.round((width * originalHeight) / originalWidth),
        };
      }
      return { width, height: undefined };
    }

    // In all other cases, use the property value:
    return {
      width: width === "original" ? originalWidth : width,
      height: height === "original" ? originalHeight : height,
    };
  });

  const src = $derived.by(() => {
    if (waitingLazyLoading) {
      return lazyPlaceholder;
    }

    const image = imageState.current;
    if (image === undefined)
      return (
        placeholder ??
        "data:image/gif;base64,R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw=="
      );

    if (!image) return undefined;

    const bestImage = highestResAvailable(
      image,
      dimensions.width || dimensions.height || 9999,
      dimensions.height || dimensions.width || 9999,
    );

    if (!bestImage) return image.placeholderDataURL ?? placeholder;
    if (lastBestImage?.[0] === bestImage.image.$jazz.id)
      return lastBestImage?.[1];

    const blob = bestImage.image.toBlob();

    if (blob) {
      const url = URL.createObjectURL(blob);
      revokeObjectURL(lastBestImage?.[1]);
      lastBestImage = [bestImage.image.$jazz.id, url];
      return url;
    }

    return image.placeholderDataURL ?? placeholder;
  });

  // Cleanup object URL on component destroy
  onDestroy(() => {
    revokeObjectURL(lastBestImage?.[1]);
  });

  function revokeObjectURL(url: string | undefined) {
    if (url && url.startsWith("blob:")) {
      URL.revokeObjectURL(url);
    }
  }

  const emptyPixelBlob = new Blob(
    [
      Uint8Array.from(
        atob(
          "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg==",
        ),
        (c) => c.charCodeAt(0),
      ),
    ],
    { type: "image/png" },
  );
</script>

<img
  {src}
  width={dimensions.width}
  height={dimensions.height}
  alt={rest.alt}
  onload={() => {
    waitingLazyLoading = false;
  }}
  {...rest}
/>
