import { ImageDefinition } from "jazz-tools";
import { highestResAvailable } from "jazz-tools/media";
import { type JSX, forwardRef, useEffect, useMemo, useRef } from "react";
import { useCoState } from "../hooks.js";

export type ImageProps = Omit<
  JSX.IntrinsicElements["img"],
  "src" | "srcSet" | "width" | "height"
> & {
  imageId: string;
  width?: number | "original";
  height?: number | "original";
};

export const Image = forwardRef<HTMLImageElement, ImageProps>(function Image(
  { imageId, width, height, ...props },
  ref,
) {
  const image = useCoState(ImageDefinition, imageId);
  const lastBestImage = useRef<[string, string] | null>(null);

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
    if (!image) return undefined;

    const bestImage = highestResAvailable(
      image,
      dimensions.width || dimensions.height || 9999,
      dimensions.height || dimensions.width || 9999,
    );

    if (!bestImage) return image.placeholderDataURL;
    if (lastBestImage.current?.[0] === bestImage.image.id)
      return lastBestImage.current?.[1];

    const blob = bestImage.image.toBlob();

    if (blob) {
      const url = URL.createObjectURL(blob);
      lastBestImage.current = [bestImage.image.id, url];
      return url;
    }

    return image.placeholderDataURL;
  }, [image, dimensions.width, dimensions.height]);

  // Revoke object URL when src changes or component unmounts
  useEffect(
    () => () => {
      revokeObjectURL(src);
    },
    [src],
  );

  if (!image) {
    return null;
  }

  return (
    <img
      ref={ref}
      src={src}
      width={dimensions.width}
      height={dimensions.height}
      {...props}
    />
  );
});

function revokeObjectURL(url: string | undefined) {
  if (url && url.startsWith("blob:")) {
    URL.revokeObjectURL(url);
  }
}
