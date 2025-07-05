import { FileStream, ImageDefinition } from "jazz-tools";
import { getBestImage } from "jazz-tools/media";
import { type JSX, useEffect, useMemo, useRef, useState } from "react";
import { useCoState } from "../hooks";

export type ImageProps = Omit<
  JSX.IntrinsicElements["img"],
  "src" | "srcSet" | "ref" | "alt" | "width" | "height" | "loading"
> & {
  imageId: string;
  alt: string;
  width?: number;
  height?: number;
  priority?: boolean;
};

export default function Image({
  imageId,
  alt,
  width,
  height,
  priority,
  ...props
}: ImageProps) {
  const image = useCoState(ImageDefinition, imageId);

  const [src, setSrc] = useState<string | undefined>(image?.placeholderDataURL);
  const objectUrl = useRef<string | undefined>(undefined);

  const dimensions: Pick<ImageProps, "width" | "height"> = useMemo(() => {
    if (width || height) {
      return { width, height };
    }

    if (image) {
      return {
        width: image.originalSize[0],
        height: image.originalSize[1],
      };
    }

    return { width: 0, height: 0 };
  }, [image?.originalSize, width, height]);

  useEffect(() => {
    if (!image) return;

    setSrc(image.placeholderDataURL);
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
      {...props}
      src={src}
      alt={alt}
      width={dimensions.width}
      height={dimensions.height}
    />
  );
}
