import { FileStream, ImageDefinition } from "jazz-tools";
import { getBestImage } from "jazz-tools/media";
import { useCoState } from "jazz-tools/react-native-core";
import { useEffect, useMemo, useState } from "react";
import { Image as RNImage, ImageProps as RNImageProps } from "react-native";

export type ImageProps = Omit<
  RNImageProps,
  "alt" | "width" | "height" | "loading" | "source"
> & {
  imageId: string;
  alt: string;
  width?: number;
  height?: number;
};

export function Image({ imageId, alt, width, height, ...props }: ImageProps) {
  const image = useCoState(ImageDefinition, imageId);
  const [src, setSrc] = useState<string | undefined>(image?.placeholderDataURL);

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

    let lastBestImage: FileStream | string | undefined =
      image.placeholderDataURL;

    const unsub = image.subscribe({}, (update) => {
      if (lastBestImage === undefined && update.placeholderDataURL) {
        setSrc(update.placeholderDataURL);
        lastBestImage = update.placeholderDataURL;
      }

      const bestImage = getBestImage(
        update,
        dimensions.width || dimensions.height || Infinity,
        dimensions.height || dimensions.width || Infinity,
      );
      console.log("Found best image", bestImage);

      if (!bestImage) return;

      if (lastBestImage === bestImage) return;

      const url = bestImage.asBase64({ dataURL: true });

      if (url) {
        setSrc(url);
        lastBestImage = bestImage;
      }
    });

    return unsub;
  }, [image]);

  if (!image) {
    return null;
  }

  return (
    <RNImage
      source={{ uri: src }}
      width={dimensions.width}
      height={dimensions.height}
      style={{ backgroundColor: "red" }}
      alt={alt}
      {...props}
    />
  );
}
