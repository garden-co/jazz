import {
  Account,
  FileStream,
  Group,
  ImageDefinition,
  type Loaded,
} from "jazz-tools";

export type SourceType = Blob | File | string;

export type ResizeOutput = Blob | string;

export type CreateImageOptions = {
  /** The owner of the image. Can be either a Group or Account. If not specified, the current user will be the owner. */
  owner?: Group | Account;
  /**
   * Controls placeholder generation for the image.
   * - `"blur"`: Generates a blurred placeholder image (default)
   * - `false`: No placeholder is generated
   * @default "blur"
   */
  placeholder?: "blur" | false;
  /**
   * Maximum size constraint for the image. The image will be resized to fit within this size while maintaining aspect ratio.
   * If the image is smaller than maxSize in both dimensions, no resizing occurs.
   * @example 1024 // Resizes image to fit within 1024px in the largest dimension
   */
  maxSize?: number; // | [number, number];
  /**
   * The progressive loading pattern is a technique that allows images to load incrementally, starting with a small version and gradually replacing it with a larger version as it becomes available.
   * This is useful for improving the user experience by showing a placeholder while the image is loading.
   *
   * Passing progressive: true to createImage() will create internal smaller versions of the image for future uses.
   *
   * @default false
   */
  progressive?: boolean;
};

export type CreateImageReturnType = Loaded<
  typeof ImageDefinition,
  { original: true }
>;

export type CreateImageImpl<
  TSourceType = SourceType,
  TResizeOutput = ResizeOutput,
> = {
  createFileStreamFromSource: (
    imageBlobOrFile: TSourceType | TResizeOutput,
    owner?: Group | Account,
  ) => Promise<FileStream>;
  getImageSize: (
    imageBlobOrFile: TSourceType,
  ) => Promise<{ width: number; height: number }>;
  getPlaceholderBase64: (imageBlobOrFile: TSourceType) => Promise<string>;
  resize: (
    imageBlobOrFile: TSourceType,
    width: number,
    height: number,
  ) => Promise<TResizeOutput>;
};

export function createImageFactory<TSourceType, TResizeOutput>(
  impl: CreateImageImpl<TSourceType, TResizeOutput>,
  imageTypeGuard?: (imageBlobOrFile: TSourceType) => void,
) {
  return (source: TSourceType, options?: CreateImageOptions) => {
    imageTypeGuard?.(source);
    return createImage(source, options ?? {}, impl);
  };
}

async function createImage<TSourceType, TResizeOutput>(
  imageBlobOrFile: TSourceType,
  options: CreateImageOptions,
  impl: CreateImageImpl<TSourceType, TResizeOutput>,
): Promise<CreateImageReturnType> {
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
    imageCoValue.$jazz.set("progressive", true);

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
      imageCoValue.$jazz.set(
        `${width}x${height}`,
        await impl.createFileStreamFromSource(blob, options?.owner),
      );
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
