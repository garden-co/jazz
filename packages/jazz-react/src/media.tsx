import { ImageDefinition, Loaded } from "jazz-tools";
import React, { useEffect, useState } from "react";

/** @category Media */
export function useProgressiveImg({
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
    const unsub = image.subscribe({}, (update) => {
      const highestRes = ImageDefinition.highestResAvailable(update, {
        maxWidth,
        targetWidth,
      });
      if (highestRes) {
        if (highestRes.res !== lastHighestRes) {
          lastHighestRes = highestRes.res;
          const blob = highestRes.stream.toBlob();
          if (blob) {
            const blobURI = URL.createObjectURL(blob);
            setCurrent({ src: blobURI, res: highestRes.res });
            return () => {
              setTimeout(() => URL.revokeObjectURL(blobURI), 200);
            };
          }
        }
      } else {
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
export function ProgressiveImg({
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
  const result = useProgressiveImg({ image, maxWidth, targetWidth });
  return result && children(result);
}
