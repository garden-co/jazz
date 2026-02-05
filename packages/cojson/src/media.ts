import { RawCoMap } from "./coValues/coMap.js";
import { RawBinaryCoStream } from "./coValues/binaryCoStream.js";

export type ImageDefinition = RawCoMap<{
  originalSize: [number, number];
  placeholderDataURL?: string;
  [res: `${number}x${number}`]: RawBinaryCoStream["id"];
}>;
