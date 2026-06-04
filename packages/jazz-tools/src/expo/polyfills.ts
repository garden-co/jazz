/// <reference path="./vendor.d.ts" />

import { polyfillGlobal } from "react-native/Libraries/Utilities/PolyfillFunctions";

import { getRandomValues } from "expo-crypto";
import { ReadableStream as PonyfillReadableStream } from "web-streams-polyfill";

const readableStreamCtor = globalThis.ReadableStream ?? PonyfillReadableStream;
const cryptoObject = globalThis.crypto;

polyfillGlobal("ReadableStream", () => readableStreamCtor);

if (cryptoObject) {
  if (typeof cryptoObject.getRandomValues !== "function") {
    Object.defineProperty(cryptoObject, "getRandomValues", {
      configurable: true,
      value: getRandomValues,
      writable: true,
    });
  }
} else {
  polyfillGlobal("crypto", () => ({ getRandomValues }));
}
