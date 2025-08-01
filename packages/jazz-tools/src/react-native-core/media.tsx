import { ImageDefinition, Loaded } from "jazz-tools";
import React, { useEffect, useState } from "react";

/** @category Media */
export function useProgressiveImgNative({
  image,
  maxWidth,
  targetWidth,
}: {
  image: Loaded<typeof ImageDefinition> | null | undefined;
  maxWidth?: number;
  targetWidth?: number;
}) {
  const [current, setCurrent] = useState<
    { src?: string; res?: `${number}x${number}` | "placeholder" } | undefined
  >(undefined);

  useEffect(() => {
    let lastHighestRes: string | undefined;
    if (!image) return;
    const unsub = image.$jazz.subscribe({}, (update) => {
      const highestRes = ImageDefinition.highestResAvailable(update, {
        maxWidth,
        targetWidth,
      });
      if (highestRes && highestRes.res !== lastHighestRes) {
        lastHighestRes = highestRes.res;
        // use the base64 data directly
        const dataUrl = highestRes.stream.asBase64({ dataURL: true });
        if (dataUrl) {
          setCurrent({
            src: dataUrl,
            res: highestRes.res,
          });
        } else {
          // Fallback to placeholder if chunks aren't available
          console.warn("No chunks available for image", image.id);
          setCurrent({
            src: update?.placeholderDataURL,
            res: "placeholder",
          });
        }
      } else if (!highestRes) {
        setCurrent({
          src: update?.placeholderDataURL,
          res: "placeholder",
        });
      }
    });

    return unsub;
  }, [image?.id, maxWidth]);

  return {
    src: current?.src,
    res: current?.res,
    originalSize: image?.originalSize,
  };
}

/** @category Media */
export function ProgressiveImgNative({
  children,
  image,
  maxWidth,
  targetWidth,
}: {
  children: (result: {
    src: string | undefined;
    res: `${number}x${number}` | "placeholder" | undefined;
    originalSize: readonly [number, number] | undefined;
  }) => React.ReactNode;
  image: Loaded<typeof ImageDefinition> | null | undefined;
  maxWidth?: number;
  targetWidth?: number;
}) {
  const result = useProgressiveImgNative({ image, maxWidth, targetWidth });
  return result && children(result);
}
