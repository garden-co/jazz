import { FileStream, ImageDefinition } from "jazz-tools";
import { highestResAvailable } from "jazz-tools/media";
import { forwardRef, useEffect, useMemo, useState } from "react";
import { Image as RNImage, ImageProps as RNImageProps } from "react-native";
import { useCoStateWithSelector } from "../hooks.js";

export type ImageProps = Omit<RNImageProps, "width" | "height" | "source"> & {
  /** The ID of the ImageDefinition to display */
  imageId: string;
  /**
   * Width of the image. Can be a number or "original" to use the original image width.
   * When set to "original", the component will calculate the appropriate height to maintain aspect ratio.
   *
   * @example
   * ```tsx
   * // Fixed width, auto-calculated height
   * <Image imageId="123" width={600} />
   *
   * // Original width
   * <Image imageId="123" width="original" />
   * ```
   */
  width?: number | "original";
  /**
   * Height of the image. Can be a number or "original" to use the original image height.
   * When set to "original", the component will calculate the appropriate width to maintain aspect ratio.
   *
   * @example
   * ```tsx
   * // Fixed height, auto-calculated width
   * <Image imageId="123" height={400} />
   *
   * // Original height
   * <Image imageId="123" height="original" />
   * ```
   */
  height?: number | "original";
};

/**
 * A React Native Image component that integrates with Jazz's ImageDefinition system.
 *
 * @example
 * ```tsx
 * import { Image } from "jazz-tools/react-native";
 * import { StyleSheet } from "react-native";
 *
 * function ProfilePicture({ imageId }) {
 *   return (
 *     <Image
 *       imageId={imageId}
 *       style={styles.profilePic}
 *       width={100}
 *       height={100}
 *       resizeMode="cover"
 *     />
 *   );
 * }
 *
 * const styles = StyleSheet.create({
 *   profilePic: {
 *     borderRadius: 50,
 *   }
 * });
 * ```
 */
export const Image = forwardRef<RNImage, ImageProps>(function Image(
  { imageId, width, height, ...props },
  ref,
) {
  const image = useCoStateWithSelector(ImageDefinition, imageId, {
    select: (image) => (image.$isLoaded ? image : null),
  });
  const [src, setSrc] = useState<string | undefined>(
    image?.placeholderDataURL ??
      "data:image/gif;base64,R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==",
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

  useEffect(() => {
    if (!image) return;

    let lastBestImage: FileStream | string | undefined =
      image.placeholderDataURL;

    const unsub = image.$jazz.subscribe({}, (update) => {
      if (lastBestImage === undefined && update.placeholderDataURL) {
        setSrc(update.placeholderDataURL);
        lastBestImage = update.placeholderDataURL;
      }

      const bestImage = highestResAvailable(
        update,
        dimensions.width || dimensions.height || 9999,
        dimensions.height || dimensions.width || 9999,
      );

      if (!bestImage) return;

      if (lastBestImage === bestImage.image) return;

      const url = bestImage.image.asBase64({ dataURL: true });

      if (url) {
        setSrc(url);
        lastBestImage = bestImage.image;
      }
    });

    return unsub;
  }, [image]);

  if (!image) {
    return null;
  }

  return (
    <RNImage
      ref={ref}
      source={{ uri: src }}
      width={dimensions.width}
      height={dimensions.height}
      {...props}
    />
  );
});
