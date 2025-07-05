import type { Account, Group, ImageDefinition } from "jazz-tools";

export { getBestImage } from "./utils";

export declare function createImage(
  imageBlobOrFile: Blob | File | string,
  options?: {
    owner?: Group | Account;
    placeholder?: "blur" | false; // default "blur"
    maxSize?: number; // | [number, number];
    progressive?: boolean;
    // resizer?: (
    //   originalImage: Blob | File,
    //   w: number,
    //   h: number,
    // ) => Promise<{ width: number; height: number; image: Blob }>;
  },
): Promise<ImageDefinition>;
