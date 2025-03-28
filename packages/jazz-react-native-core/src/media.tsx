import { ImageDefinition } from "jazz-tools";
import React, { useEffect, useState } from "react";

/** @category Media */
export function useProgressiveImg({
  image,
  maxWidth,
  targetWidth,
}: {
  image: ImageDefinition | null | undefined;
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
      const highestRes = update?.highestResAvailable({ maxWidth, targetWidth });
      if (highestRes && highestRes.res !== lastHighestRes) {
        lastHighestRes = highestRes.res;
        // use the base64 data directly
        const chunks = highestRes.stream.getChunks();
        if (chunks?.chunks && chunks.chunks.length > 0) {
          // convert chunks to base64
          const totalLength = chunks.chunks.reduce(
            (acc, chunk) => acc + chunk.length,
            0,
          );
          const combinedArray = new Uint8Array(totalLength);
          let offset = 0;
          chunks.chunks.forEach((chunk) => {
            combinedArray.set(chunk, offset);
            offset += chunk.length;
          });

          // Create data URL
          const base64 = Buffer.from(combinedArray).toString("base64");
          const dataUrl = `data:${chunks.mimeType};base64,${base64}`;

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
  image: ImageDefinition | null | undefined;
  maxWidth?: number;
  targetWidth?: number;
}) {
  const result = useProgressiveImg({ image, maxWidth, targetWidth });
  return result && children(result);
}
