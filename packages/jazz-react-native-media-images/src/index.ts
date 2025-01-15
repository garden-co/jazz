import ImageResizer from "@bam.tech/react-native-image-resizer";
import * as FileSystem from "expo-file-system";
import { Account, FileStream, Group, ImageDefinition } from "jazz-tools";
import { Image } from "react-native";

function arrayBuffer(blob: Blob): Promise<ArrayBuffer> {
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

async function fileUriToBlob(uri: string): Promise<Blob> {
  try {
    const response = await fetch(uri);
    const blob = await response.blob();
    blob.arrayBuffer = () => arrayBuffer(blob);
    return blob;
  } catch (error) {
    console.error("Failed to convert file URI to Blob:", error);
    throw new Error("Failed to convert file URI to Blob");
  }
}

async function convertFileContentsToBase64DataURI(
  fileUri: string,
  contentType: string,
) {
  try {
    const base64 = await FileSystem.readAsStringAsync(fileUri, {
      encoding: FileSystem.EncodingType.Base64,
    });
    return `data:${contentType};base64,${base64}`;
  } catch (error) {
    console.error("Failed to convert file to base64:", error);
    return null;
  }
}

function base64DataURIToParts(base64Data: string) {
  const parts = base64Data.split(",");
  const contentType = parts[0]?.split(":")?.[1]?.split(";")?.[0] || "";
  const data = parts[1] || "";
  return { contentType, data };
}

function contentTypeToFormat(contentType: string) {
  if (contentType.includes("image/png")) return "PNG";
  if (contentType.includes("image/jpeg")) return "JPEG";
  if (contentType.includes("image/webp")) return "WEBP";
  return "PNG";
}

async function base64DataURIToBlob(base64Data: string) {
  const { contentType, data } = base64DataURIToParts(base64Data);
  const byteCharacters = atob(data);

  const byteNumbers = new Array(byteCharacters.length);
  for (let i = 0; i < byteCharacters.length; i++) {
    byteNumbers[i] = byteCharacters.charCodeAt(i);
  }
  const byteArray = new Uint8Array(byteNumbers);

  const buffer = Buffer.from(byteArray);
  // @ts-expect-error buffer has data
  const blob = new Blob([buffer.data], { type: contentType });
  blob.arrayBuffer = () => arrayBuffer(blob);
  return blob;
}

async function getImageDimensions(
  uri: string,
): Promise<{ width: number; height: number }> {
  return new Promise((resolve, reject) => {
    Image.getSize(
      uri,
      (width, height) => resolve({ width, height }),
      (error) => {
        console.error("Failed to get image dimensions:", error);
        reject(new Error("Failed to get image dimensions"));
      },
    );
  });
}

/** @category Image creation */
export async function createImage(
  base64ImageDataURI: string,
  options: {
    owner?: Group | Account;
    maxSize?: 256 | 1024 | 2048;
  } = {},
): Promise<ImageDefinition> {
  try {
    const { contentType } = base64DataURIToParts(base64ImageDataURI);
    const format = contentTypeToFormat(contentType);

    let originalWidth, originalHeight;
    try {
      ({ width: originalWidth, height: originalHeight } =
        await getImageDimensions(base64ImageDataURI));
    } catch (error) {
      console.error("Error getting image dimensions:", error);
      throw new Error("Failed to get image dimensions");
    }

    let placeholderImage;
    try {
      placeholderImage = await ImageResizer.createResizedImage(
        base64ImageDataURI,
        8,
        8,
        format,
        100,
        0,
      );
    } catch (error) {
      console.error("Error creating placeholder image:", error);
      throw new Error("Failed to create placeholder image");
    }

    const placeholderDataURL = await convertFileContentsToBase64DataURI(
      placeholderImage.uri,
      contentType,
    );

    if (!placeholderDataURL) {
      throw new Error("Failed to create placeholder data URL");
    }

    const imageDefinition = ImageDefinition.create(
      {
        originalSize: [originalWidth, originalHeight],
        placeholderDataURL,
      },
      options.owner,
    );

    const addImageStream = async (
      width: number,
      height: number,
      label: string,
    ) => {
      try {
        const resizedImage = await ImageResizer.createResizedImage(
          base64ImageDataURI,
          width,
          height,
          format,
          80,
          0,
        );

        const binaryStream = await FileStream.createFromBlob(
          await fileUriToBlob(resizedImage.uri),
          { owner: options.owner },
        );

        // @ts-expect-error types
        imageDefinition[label] = binaryStream;
      } catch (error) {
        console.error(`Error adding image stream for ${label}:`, error);
        throw new Error(`Failed to add image stream for ${label}`);
      }
    };

    if (originalWidth > 256 || originalHeight > 256) {
      const width =
        originalWidth > originalHeight
          ? 256
          : Math.round(256 * (originalWidth / originalHeight));
      const height =
        originalHeight > originalWidth
          ? 256
          : Math.round(256 * (originalHeight / originalWidth));
      await addImageStream(width, height, `${width}x${height}`);
    }

    if (options.maxSize === 256) return imageDefinition;

    if (originalWidth > 1024 || originalHeight > 1024) {
      const width =
        originalWidth > originalHeight
          ? 1024
          : Math.round(1024 * (originalWidth / originalHeight));
      const height =
        originalHeight > originalWidth
          ? 1024
          : Math.round(1024 * (originalHeight / originalWidth));
      await addImageStream(width, height, `${width}x${height}`);
    }

    if (options.maxSize === 1024) return imageDefinition;

    if (originalWidth > 2048 || originalHeight > 2048) {
      const width =
        originalWidth > originalHeight
          ? 2048
          : Math.round(2048 * (originalWidth / originalHeight));
      const height =
        originalHeight > originalWidth
          ? 2048
          : Math.round(2048 * (originalHeight / originalWidth));
      await addImageStream(width, height, `${width}x${height}`);
    }

    if (options.maxSize === 2048) return imageDefinition;

    if (options.maxSize === undefined || options.maxSize > 2048) {
      try {
        const originalBinaryStream = await FileStream.createFromBlob(
          await base64DataURIToBlob(base64ImageDataURI),
          { owner: options.owner },
        );
        imageDefinition[`${originalWidth}x${originalHeight}`] =
          originalBinaryStream;
      } catch (error) {
        console.error("Error adding original image stream:", error);
        throw new Error("Failed to add original image stream");
      }
    }

    return imageDefinition;
  } catch (error) {
    console.error("Error in createImage:", error);
    throw error;
  }
}
