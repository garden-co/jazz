/* -eslint-disable import/order */

import "@azure/core-asynciterator-polyfill";

import "@bacons/text-decoder/install";

import "react-native-get-random-values";

// @ts-expect-error - @types/readable-stream doesn't have ReadableStream type
import { ReadableStream } from "readable-stream";
if (!globalThis.ReadableStream) {
  globalThis.ReadableStream = ReadableStream;
}
