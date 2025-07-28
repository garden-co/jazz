import {
  Account,
  FileStream,
  Group,
  ImageDefinition,
  type Loaded,
} from "jazz-tools";

export type SourceType = Blob | File | string;

export type CreateImageOptions = {
  owner?: Group | Account;
  placeholder?: "blur" | false; // default "blur"
  maxSize?: number; // | [number, number];
  progressive?: boolean;
};

export type CreateImageImpl = {
  createFileStreamFromSource: (
    imageBlobOrFile: SourceType,
    owner?: Group | Account,
  ) => Promise<FileStream>;
  getImageSize: (
    imageBlobOrFile: SourceType,
  ) => Promise<{ width: number; height: number }>;
  getPlaceholderBase64: (imageBlobOrFile: SourceType) => Promise<string>;
  resize: (
    imageBlobOrFile: SourceType,
    width: number,
    height: number,
  ) => Promise<Blob | string>;
};

export function createImageFactory(impl: CreateImageImpl) {
  return (source: SourceType, options: CreateImageOptions) =>
    createImage(source, options, impl);
}

async function createImage(
  imageBlobOrFile: SourceType,
  options: CreateImageOptions,
  impl: CreateImageImpl,
): Promise<Loaded<typeof ImageDefinition, { $each: true }>> {
  // Get the original size of the image
  const { width: originalWidth, height: originalHeight } =
    await impl.getImageSize(imageBlobOrFile);

  const def: {
    originalSize: [number, number];
    progressive: boolean;
    placeholderDataURL: string | undefined;
    original?: FileStream;
    files: Record<string, FileStream>;
  } = {
    originalSize: [originalWidth, originalHeight],
    progressive: false,
    placeholderDataURL: undefined,
    files: {},
  };

  // Placeholder
  if (options?.placeholder === "blur") {
    def.placeholderDataURL = await impl.getPlaceholderBase64(imageBlobOrFile);
  }

  /**
   * Original
   *
   * Save the original image.
   * If the maxSize is set, resize the image to the maxSize if needed
   */
  if (options?.maxSize === undefined) {
    def.original = await impl.createFileStreamFromSource(
      imageBlobOrFile,
      options?.owner,
    );
    def.files[`${originalWidth}x${originalHeight}`] = def.original;
  } else if (
    options?.maxSize >= originalWidth &&
    options?.maxSize >= originalHeight
  ) {
    // no resizes required, just return the original image
    def.original = await impl.createFileStreamFromSource(
      imageBlobOrFile,
      options?.owner,
    );
    def.files[`${originalWidth}x${originalHeight}`] = def.original;
  } else {
    const { width, height } = getNewDimensions(
      originalWidth,
      originalHeight,
      options.maxSize,
    );

    const blob = await impl.resize(imageBlobOrFile, width, height);
    def.originalSize = [width, height];
    def.original = await impl.createFileStreamFromSource(blob, options?.owner);
    def.files[`${width}x${height}`] = def.original;
  }

  const imageCoValue = ImageDefinition.create(
    {
      originalSize: def.originalSize,
      progressive: def.progressive,
      placeholderDataURL: def.placeholderDataURL,
      original: def.original,
      ...def.files,
    },
    options?.owner,
  );

  /**
   * Progressive loading
   *
   * Save a set of resized images using three sizes: 256, 1024, 2048
   *
   * On the client side, the image will be loaded progressively, starting from the smallest size and increasing the size until the original size is reached.
   */
  if (options?.progressive) {
    imageCoValue.progressive = true;

    const resizes = ([256, 1024, 2048] as const).filter(
      (s) =>
        s <
        Math.max(imageCoValue.originalSize[0], imageCoValue.originalSize[1]),
    );

    for (const size of resizes) {
      const { width, height } = getNewDimensions(
        originalWidth,
        originalHeight,
        size,
      );

      const blob = await impl.resize(imageBlobOrFile, width, height);
      imageCoValue[`${width}x${height}`] =
        await impl.createFileStreamFromSource(blob, options?.owner);
    }
  }

  return imageCoValue;
}

const getNewDimensions = (
  originalWidth: number,
  originalHeight: number,
  maxSize: number,
) => {
  if (originalWidth > originalHeight) {
    return {
      width: maxSize,
      height: Math.round(maxSize * (originalHeight / originalWidth)),
    };
  }

  return {
    width: Math.round(maxSize * (originalWidth / originalHeight)),
    height: maxSize,
  };
};
