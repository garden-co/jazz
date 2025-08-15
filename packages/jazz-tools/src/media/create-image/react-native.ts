import ImageResizer from "@bam.tech/react-native-image-resizer";
import type { Account, Group } from "jazz-tools";
import { FileStream } from "jazz-tools";
import { Image } from "react-native";
import { createImageFactory } from "../create-image-factory";

/**
 * Creates an ImageDefinition from an image file path with built-in UX features.
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
export const createImage = createImageFactory(
  {
    getImageSize,
    getPlaceholderBase64,
    createFileStreamFromSource,
    resize,
  },
  (filePath) => {
    if (typeof filePath !== "string") {
      throw new Error(
        "createImage(Blob | File) is not supported on this platform",
      );
    }
  },
);

async function getImageSize(
  filePath: string,
): Promise<{ width: number; height: number }> {
  const { width, height } = await Image.getSize(filePath);

  return { width, height };
}

async function getPlaceholderBase64(filePath: string): Promise<string> {
  if (typeof ImageResizer === "undefined" || ImageResizer === null) {
    throw new Error(
      "ImageResizer is not installed, please run `npm install @bam.tech/react-native-image-resizer`",
    );
  }

  const { uri } = await ImageResizer.createResizedImage(
    filePath,
    8,
    8,
    "PNG",
    100,
  );

  return imageUrlToBase64(uri);
}

async function resize(
  filePath: string,
  width: number,
  height: number,
): Promise<string> {
  if (typeof ImageResizer === "undefined" || ImageResizer === null) {
    throw new Error(
      "ImageResizer is not installed, please run `npm install @bam.tech/react-native-image-resizer`",
    );
  }

  const mimeType = await getMimeType(filePath);

  const { uri } = await ImageResizer.createResizedImage(
    filePath,
    width,
    height,
    contentTypeToFormat(mimeType),
    80,
  );

  return uri;
}

function getMimeType(filePath: string): Promise<string> {
  return fetch(filePath)
    .then((res) => res.blob())
    .then((blob) => blob.type);
}

function contentTypeToFormat(contentType: string) {
  if (contentType.includes("image/png")) return "PNG";
  if (contentType.includes("image/jpeg")) return "JPEG";
  if (contentType.includes("image/webp")) return "WEBP";
  return "PNG";
}

export async function createFileStreamFromSource(
  filePath: string,
  owner?: Account | Group,
): Promise<FileStream> {
  const blob = await fetch(filePath).then((res) => res.blob());
  const arrayBuffer = await toArrayBuffer(blob);

  return FileStream.createFromArrayBuffer(arrayBuffer, blob.type, undefined, {
    owner,
  });
}

// TODO: look for more efficient way to do this as React Native hasn't blob.arrayBuffer()
function toArrayBuffer(blob: Blob): Promise<ArrayBuffer> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onloadend = () => {
      resolve(reader.result as ArrayBuffer);
    };
    reader.onerror = (error) => {
      reject(error);
    };
    reader.readAsArrayBuffer(blob);
  });
}

async function imageUrlToBase64(url: string): Promise<string> {
  const response = await fetch(url);
  const blob = await response.blob();
  return new Promise((onSuccess, onError) => {
    try {
      const reader = new FileReader();
      reader.onload = function () {
        onSuccess(reader.result as string);
      };
      reader.readAsDataURL(blob);
    } catch (e) {
      onError(e);
    }
  });
}
