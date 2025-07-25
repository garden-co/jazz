import { FileStream, ImageDefinition } from "jazz-tools";
import { highestResAvailable } from "jazz-tools/media";
import {
  type JSX,
  forwardRef,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { useCoState } from "../hooks";

export type ImageProps = Omit<
  JSX.IntrinsicElements["img"],
  "src" | "srcSet" | "width" | "height"
> & {
  imageId: string;
  width?: number | "original";
  height?: number | "original";
};

const Image = forwardRef<HTMLImageElement, ImageProps>(function Image(
  { imageId, width, height, ...props },
  ref,
) {
  const image = useCoState(ImageDefinition, imageId);

  const [src, setSrc] = useState<string | undefined>(image?.placeholderDataURL);
  const objectUrl = useRef<string | undefined>(undefined);

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

    setSrc(image.placeholderDataURL);
    let lastBestImage: FileStream | null = null;

    const unsub = image.subscribe({}, (update) => {
      const bestImage = highestResAvailable(
        update,
        dimensions.width || dimensions.height || 9999,
        dimensions.height || dimensions.width || 9999,
      );

      console.log("Found best image", { bestImage, ...dimensions });

      if (!bestImage) return;

      if (lastBestImage === bestImage) return;

      const blob = bestImage.toBlob();
      console.log("Blob", blob);

      if (blob) {
        objectUrl.current && URL.revokeObjectURL(objectUrl.current);
        objectUrl.current = URL.createObjectURL(blob);
        setSrc(objectUrl.current);
        lastBestImage = bestImage;
      }
    });

    return unsub;
  }, [image]);

  useEffect(
    () => () => {
      if (objectUrl.current) URL.revokeObjectURL(objectUrl.current);
    },
    [],
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

export default Image;
