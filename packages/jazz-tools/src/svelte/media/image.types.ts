import type { HTMLImgAttributes } from "svelte/elements";

export interface ImageProps extends Omit<HTMLImgAttributes, "width" | "height"> {
  imageId: string;
  width?: number | "original";
  height?: number | "original";
  customPlaceholder?: string;
}
