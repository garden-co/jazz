import { Group, Account, Loaded, ImageDefinition } from "jazz-tools";

// #region CreateImageDeclaration
declare function createImage(
  image: Blob | File | string,
  options?: {
    owner?: Group | Account;
    placeholder?: false | "blur";
    maxSize?: number;
    progressive?: boolean;
  },
): Promise<Loaded<typeof ImageDefinition, { original: true }>>;
// #endregion

// #region BrowserImageProps
// @ts-expect-error ImageProps duplicate
export type ImageProps = Omit<
  // @ts-expect-error This is not a real type
  HTMLImgAttributes,
  "src" | "srcset" | "width" | "height"
> & {
  imageId: string;
  width?: number | "original";
  height?: number | "original";
  placeholder?: string;
  loading?: "lazy" | "eager";
};
// #endregion

// #region RNImageProps
// @ts-expect-error ImageProps duplicate, RNImageProps doesn't exist
export type ImageProps = Omit<RNImageProps, "width" | "height" | "source"> & {
  imageId: string;
  width?: number | "original";
  height?: number | "original";
  placeholder?: string;
};
// #endregion
