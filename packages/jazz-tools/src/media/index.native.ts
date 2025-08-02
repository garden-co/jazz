import ImageResizer from "@bam.tech/react-native-image-resizer";
import type { Account, Group, ImageDefinition } from "jazz-tools";
import { FileStream } from "jazz-tools";
import { Image } from "react-native";
import {
  CreateImageOptions,
  SourceType,
  createImageFactory,
} from "./create-image.js";

export { highestResAvailable, loadImage, loadImageBySize } from "./utils.js";
export { createImageFactory };

export async function createImage(
  imageBlobOrFile: Blob | File | string,
  options?: CreateImageOptions,
) {
  return createImageFactory({
    getImageSize,
    getPlaceholderBase64,
    createFileStreamFromSource,
    resize,
  })(imageBlobOrFile, options || {});
}

async function getImageSize(
  filePath: SourceType,
): Promise<{ width: number; height: number }> {
  if (typeof filePath !== "string") {
    throw new Error(
      "createImage(Blob | File) is not supported on this platform",
    );
  }

  const { width, height } = await Image.getSize(filePath);

  return { width, height };
}

async function getPlaceholderBase64(filePath: SourceType): Promise<string> {
  if (typeof filePath !== "string") {
    throw new Error(
      "createImage(Blob | File) is not supported on this platform",
    );
  }

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
  filePath: SourceType,
  width: number,
  height: number,
): Promise<string> {
  if (typeof filePath !== "string") {
    throw new Error(
      "createImage(Blob | File) is not supported on this platform",
    );
  }

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
  filePath: SourceType,
  owner?: Account | Group,
): Promise<FileStream> {
  if (typeof filePath !== "string") {
    throw new Error(
      "createImage(Blob | File) is not supported on this platform",
    );
  }

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
