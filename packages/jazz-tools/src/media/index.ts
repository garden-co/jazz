import type { ImageDefinition, Loaded } from "jazz-tools";
import type {
  CreateImageOptions,
  CreateImageReturnType,
} from "./create-image-factory";

export * from "./exports";

/**
 * Creates an ImageDefinition from an image file or blob with built-in UX features.
 *
 * This function creates a specialized CoValue for managing images in Jazz applications.
 * It supports blurry placeholders, built-in resizing, and progressive loading patterns.
 *
 * @returns Promise that resolves to an ImageDefinition
 *
 * @example
 * ```ts
 * import { createImage } from "jazz-tools/media";
 *
 * // Create an image from a file input
 * async function handleFileUpload(event: React.ChangeEvent<HTMLInputElement>) {
 *   const file = event.target.files?.[0];
 *   if (file) {
 *     // Creates ImageDefinition with a blurry placeholder, limited to 1024px
 *     // on the longest side, and multiple resolutions automatically
 *     const image = await createImage(file, {
 *       owner: me.$jazz.owner,
 *       maxSize: 1024,
 *       placeholder: "blur",
 *       progressive: true,
 *     });
 *
 *     // Store the image in your application data
 *     me.profile.image = image;
 *   }
 * }
 * ```
 * ```
 */
export declare function createImage(
  imageBlobOrFile: Blob | File,
  options?: CreateImageOptions,
): Promise<CreateImageReturnType>;

/**
 * Creates an ImageDefinition from an image file path with built-in UX features.
 *
 * This function creates a specialized CoValue for managing images in Jazz applications.
 * It supports blurry placeholders, built-in resizing, and progressive loading patterns.
 *
 * @returns Promise that resolves to an ImageDefinition
 * @example
 * ```ts
 * // React Native example
 * import { createImage } from "jazz-tools/media";
 *
 * async function uploadImageFromCamera(imagePath: string) {
 *   const image = await createImage(imagePath, {
 *     maxSize: 800,
 *     placeholder: "blur",
 *     progressive: false,
 *   });
 *
 *   return image;
 * }
 * ```
 */
export declare function createImage(
  filePath: string,
  options?: CreateImageOptions,
): Promise<CreateImageReturnType>;
