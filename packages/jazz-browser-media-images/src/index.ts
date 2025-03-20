import ImageBlobReduce from "image-blob-reduce";
import { Account, FileStream, Group, ImageDefinition } from "jazz-tools";
import Pica from "pica";

const pica = new Pica();

/** @category Image creation */
/**
 * Creates an ImageDefinition from a Blob or File.
 *
 * @param imageBlobOrFile - The Blob or File to create the ImageDefinition from.
 * @param options - Optional options for the ImageDefinition.
 * @returns A Promise that resolves to the created ImageDefinition.
 */
export async function createImage(
  imageBlobOrFile: Blob | File,
  options?: {
    owner?: Group | Account;
    maxSize?: 256 | 1024 | 2048;
  },
): Promise<ImageDefinition> {
  const owner = options?.owner;

  let originalWidth!: number;
  let originalHeight!: number;
  const Reducer = new ImageBlobReduce({ pica });
  Reducer.after("_blob_to_image", (env) => {
    originalWidth =
      (env as unknown as { orientation: number }).orientation & 4
        ? env.image.height
        : env.image.width;
    originalHeight =
      (env as unknown as { orientation: number }).orientation & 4
        ? env.image.width
        : env.image.height;
    return Promise.resolve(env);
  });

  const placeholderDataURL = (
    await Reducer.toCanvas(imageBlobOrFile, { max: 8 })
  ).toDataURL("image/png");

  const imageDefinition = ImageDefinition.create(
    {
      originalSize: [originalWidth, originalHeight],
      placeholderDataURL,
    },
    owner,
  );

  return new Promise((resolve) => {
    setTimeout(async () => {
      const max256 = await Reducer.toBlob(imageBlobOrFile, { max: 256 });

      if (originalWidth > 256 || originalHeight > 256) {
        const width =
          originalWidth > originalHeight
            ? 256
            : Math.round(256 * (originalWidth / originalHeight));
        const height =
          originalHeight > originalWidth
            ? 256
            : Math.round(256 * (originalHeight / originalWidth));

        const binaryStream = await FileStream.createFromBlob(max256, owner);

        imageDefinition[`${width}x${height}`] = binaryStream;
      }

      await new Promise((r) => setTimeout(r, 0));

      if (options?.maxSize === 256) {
        resolve(imageDefinition);
        return;
      }

      const max1024 = await Reducer.toBlob(imageBlobOrFile, { max: 1024 });

      if (originalWidth > 1024 || originalHeight > 1024) {
        const width =
          originalWidth > originalHeight
            ? 1024
            : Math.round(1024 * (originalWidth / originalHeight));
        const height =
          originalHeight > originalWidth
            ? 1024
            : Math.round(1024 * (originalHeight / originalWidth));

        const binaryStream = await FileStream.createFromBlob(max1024, owner);

        imageDefinition[`${width}x${height}`] = binaryStream;
      }

      await new Promise((r) => setTimeout(r, 0));

      if (options?.maxSize === 1024) {
        resolve(imageDefinition);
        return;
      }

      const max2048 = await Reducer.toBlob(imageBlobOrFile, { max: 2048 });

      if (originalWidth > 2048 || originalHeight > 2048) {
        const width =
          originalWidth > originalHeight
            ? 2048
            : Math.round(2048 * (originalWidth / originalHeight));
        const height =
          originalHeight > originalWidth
            ? 2048
            : Math.round(2048 * (originalHeight / originalWidth));

        const binaryStream = await FileStream.createFromBlob(max2048, owner);

        imageDefinition[`${width}x${height}`] = binaryStream;
      }

      await new Promise((r) => setTimeout(r, 0));

      if (options?.maxSize === 2048) {
        resolve(imageDefinition);
        return;
      }

      const originalBinaryStream = await FileStream.createFromBlob(
        imageBlobOrFile,
        owner,
      );

      imageDefinition[`${originalWidth}x${originalHeight}`] =
        originalBinaryStream;

      resolve(imageDefinition);
    }, 0);
  });
}
