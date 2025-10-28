import { CoValueLoadingState, ImageDefinition } from "jazz-tools";
import {
  type JSX,
  forwardRef,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { highestResAvailable } from "../../media/index.js";
import { useCoState } from "../hooks.js";
import e from "cors";

export type ImageProps = Omit<
  JSX.IntrinsicElements["img"],
  "src" | "srcSet" | "width" | "height"
> & {
  /** The ID of the ImageDefinition to display */
  imageId: string;
  /**
   * The desired width of the image. Can be a number in pixels or "original" to use the image's original width.
   * When set to a number, the component will select the best available resolution and maintain aspect ratio.
   *
   * @example
   * ```tsx
   * // Use original width
   * <Image imageId="123" width="original" />
   *
   * // Set width to 600px, height will be calculated to maintain aspect ratio
   * <Image imageId="123" width={600} />
   *
   * // Set both width and height to maintain aspect ratio
   * <Image imageId="123" width={600} height={400} />
   * ```
   */
  width?: number | "original";
  /**
   * The desired height of the image. Can be a number in pixels or "original" to use the image's original height.
   * When set to a number, the component will select the best available resolution and maintain aspect ratio.
   *
   * @example
   * ```tsx
   * // Use original height
   * <Image imageId="123" height="original" />
   *
   * // Set height to 400px, width will be calculated to maintain aspect ratio
   * <Image imageId="123" height={400} />
   *
   * // Set both width and height to maintain aspect ratio
   * <Image imageId="123" width={600} height={400} />
   * ```
   */
  height?: number | "original";
  /**
   * A custom placeholder to display while an image is loading. This will
   * be passed as the src of the img tag, so a data URL works well here.
   * This will be used as a fallback if no images are ready and no placeholder
   * is available otherwise.
   */
  placeholder?: string;
};

/**
 * A React component for displaying images stored as ImageDefinition CoValues.
 *
 * @example
 * ```tsx
 * import { Image } from "jazz-tools/react";
 *
 * // Force specific dimensions (may crop or stretch)
 * function Avatar({ imageId }: { imageId: string }) {
 *   return (
 *     <Image
 *       imageId={imageId}
 *       width={100}
 *       height={100}
 *       alt="Avatar"
 *       style={{ borderRadius: "50%", objectFit: "cover" }}
         placeholder={myPlaceholder}
 *     />
 *   );
 * }
 * ```
 */

export const Image = forwardRef<HTMLImageElement, ImageProps>(function Image(
  { imageId, width, height, ...props },
  ref,
) {
  const image = useCoState(ImageDefinition, imageId, {
    select: (image) => {
      if (image.$isLoaded) {
        return image;
      } else if (image.$jazz.loadingState === CoValueLoadingState.LOADING) {
        return undefined;
      } else return null;
    },
  });
  const lastBestImage = useRef<[string, string] | null>(null);

  /**
   * For lazy loading, we use the browser's strategy for images with loading="lazy".
   * We use an empty image, and when the browser triggers the load event, we load the best available image.
   * On page loading, if the image url is already in browser's cache, the load event is triggered immediately.
   * This is why we need to use a different blob url for every image.
   */
  const [waitingLazyLoading, setWaitingLazyLoading] = useState(
    props.loading === "lazy",
  );
  const lazyPlaceholder = useMemo(
    () =>
      waitingLazyLoading ? URL.createObjectURL(getEmptyPixelBlob()) : undefined,
    [waitingLazyLoading],
  );

  const dimensions: { width: number | undefined; height: number | undefined } =
    useMemo(() => {
      const originalWidth = image?.originalSize?.[0];
      const originalHeight = image?.originalSize?.[1];

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
    }, [image?.originalSize, width, height]);

  const src = useMemo(() => {
    if (waitingLazyLoading) {
      return lazyPlaceholder;
    }

    if (image === undefined)
      return "data:image/gif;base64,R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==";
    if (!image) return undefined;

    const bestImage = highestResAvailable(
      image,
      dimensions.width || dimensions.height || 9999,
      dimensions.height || dimensions.width || 9999,
    );

    if (!bestImage) return image.placeholderDataURL ?? props?.placeholder;
    if (lastBestImage.current?.[0] === bestImage.image.$jazz.id)
      return lastBestImage.current?.[1];

    const blob = bestImage.image.toBlob();

    if (blob) {
      const url = URL.createObjectURL(blob);
      revokeObjectURL(lastBestImage.current?.[1]);
      lastBestImage.current = [bestImage.image.$jazz.id, url];
      return url;
    }

    return image.placeholderDataURL ?? props?.placeholder;
  }, [image, dimensions.width, dimensions.height, waitingLazyLoading]);

  const onThresholdReached = useCallback(() => {
    setWaitingLazyLoading(false);
  }, []);

  // Revoke object URL when component unmounts
  useEffect(
    () => () => {
      // In development mode we don't revokeObjectURL on unmount because
      // it triggers twice under StrictMode.
      if (process.env.NODE_ENV === "development") return;
      revokeObjectURL(lastBestImage.current?.[1]);
    },
    [],
  );

  return (
    <img
      ref={ref}
      src={src}
      width={dimensions.width}
      height={dimensions.height}
      onLoad={waitingLazyLoading ? onThresholdReached : undefined}
      {...props}
    />
  );
});

function revokeObjectURL(url: string | undefined) {
  if (url && url.startsWith("blob:")) {
    URL.revokeObjectURL(url);
  }
}

let emptyPixelBlob: Blob | undefined;
function getEmptyPixelBlob() {
  if (!emptyPixelBlob) {
    emptyPixelBlob = new Blob(
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
  }
  return emptyPixelBlob;
}
