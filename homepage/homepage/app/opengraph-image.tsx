import { marketingCopy } from "@/content/marketingCopy";
import {
  OpenGraphImage,
  imageSize,
  imageContentType,
} from "../components/OpenGraphImage";

export const title = marketingCopy.headline;
export const size = imageSize;
export const contentType = imageContentType;
export const alt = marketingCopy.headline;

export default async function Image() {
  return OpenGraphImage({ title });
}
