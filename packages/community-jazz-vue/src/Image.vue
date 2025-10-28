<script setup lang="ts">
import { ImageDefinition } from "jazz-tools";
import { highestResAvailable } from "jazz-tools/media";
import { onUnmounted, ref, watch, computed } from "vue";
import { useCoState } from "./composables.js";

export interface ImageProps {
  /** The ID of the ImageDefinition to display */
  imageId: string;
  /**
   * The desired width of the image. Can be a number in pixels or "original" to use the image's original width.
   * When set to a number, the component will select the best available resolution and maintain aspect ratio.
   */
  width?: number | "original";
  /**
   * The desired height of the image. Can be a number in pixels or "original" to use the image's original height.
   * When set to a number, the component will select the best available resolution and maintain aspect ratio.
   */
  height?: number | "original";
  /** Alt text for the image */
  alt?: string;
  /** CSS classes to apply to the image */
  classNames?: string;
  /** CSS styles to apply to the image */
  style?: string | Record<string, string>;
  /** Loading strategy for the image */
  loading?: "lazy" | "eager";
}

const props = withDefaults(defineProps<ImageProps>(), {
  loading: "eager",
});

const image = useCoState(ImageDefinition, props.imageId, {});
let lastBestImage: [string, string] | null = null;

/**
 * For lazy loading, we use the browser's strategy for images with loading="lazy".
 * We use an empty image, and when the browser triggers the load event, we load the best available image.
 */
const waitingLazyLoading = ref(props.loading === "lazy");
const lazyPlaceholder = computed(() =>
  waitingLazyLoading.value ? URL.createObjectURL(emptyPixelBlob) : undefined,
);

const dimensions = computed(() => {
  const originalWidth = image.value?.originalSize?.[0];
  const originalHeight = image.value?.originalSize?.[1];

  // Both width and height are "original"
  if (props.width === "original" && props.height === "original") {
    return { width: originalWidth, height: originalHeight };
  }

  // Width is "original", height is a number
  if (props.width === "original" && typeof props.height === "number") {
    if (originalWidth && originalHeight) {
      return {
        width: Math.round((props.height * originalWidth) / originalHeight),
        height: props.height,
      };
    }
    return { width: undefined, height: props.height };
  }

  // Height is "original", width is a number
  if (props.height === "original" && typeof props.width === "number") {
    if (originalWidth && originalHeight) {
      return {
        width: props.width,
        height: Math.round((props.width * originalHeight) / originalWidth),
      };
    }
    return { width: props.width, height: undefined };
  }

  // In all other cases, use the property value:
  return {
    width: props.width === "original" ? originalWidth : props.width,
    height: props.height === "original" ? originalHeight : props.height,
  };
});

const src = computed(() => {
  if (waitingLazyLoading.value) {
    return lazyPlaceholder.value;
  }

  if (!image.value) return undefined;

  const bestImage = highestResAvailable(
    image.value,
    dimensions.value.width || dimensions.value.height || 9999,
    dimensions.value.height || dimensions.value.width || 9999,
  );

  if (!bestImage) return image.value.placeholderDataURL;
  if (lastBestImage?.[0] === bestImage.image.$jazz.id)
    return lastBestImage?.[1];

  const blob = bestImage.image.toBlob();

  if (blob) {
    const url = URL.createObjectURL(blob);
    revokeObjectURL(lastBestImage?.[1]);
    lastBestImage = [bestImage.image.$jazz.id, url];
    return url;
  }

  return image.value.placeholderDataURL;
});

const onThresholdReached = () => {
  waitingLazyLoading.value = false;
};

// Cleanup object URL on component destroy
onUnmounted(() => {
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

<template>
  <img
    :src="src"
    :width="dimensions.width"
    :height="dimensions.height"
    :alt="alt"
    :class="classNames"
    :style="style"
    :loading="loading"
    @load="waitingLazyLoading ? onThresholdReached() : undefined"
  />
</template>
