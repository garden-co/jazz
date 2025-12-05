import { ReadableStream } from "readable-stream"; // polyfill ReadableStream
import "@azure/core-asynciterator-polyfill"; // polyfill Async Iterator
import "react-native-get-random-values"; // polyfill getRandomValues
import FastTextEncoder from "react-native-fast-encoder"; // polyfill TextEncoder/TextDecoder

// Add encodeInto method which the fast encoder doesn't provide
if (!FastTextEncoder.prototype.encodeInto) {
  /**
   * @param {string} source
   * @param {Uint8Array} destination
   */
  FastTextEncoder.prototype.encodeInto = function (source, destination) {
    const encoded = this.encode(source);
    const writeLength = Math.min(encoded.length, destination.length);
    for (let i = 0; i < writeLength; i++) {
      destination[i] = encoded[i];
    }
    return { read: source.length, written: writeLength };
  };
}

// Install polyfills
globalThis.TextEncoder = FastTextEncoder;
globalThis.TextDecoder = FastTextEncoder;
globalThis.ReadableStream = ReadableStream;
if (__DEV__) {
  console.log("[Jazz] - Polyfills successfully installed");
}
