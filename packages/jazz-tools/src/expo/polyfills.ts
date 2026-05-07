/// <reference path="./vendor.d.ts" />

import { polyfillGlobal } from "react-native/Libraries/Utilities/PolyfillFunctions";

import { ReadableStream as PonyfillReadableStream } from "web-streams-polyfill";

const readableStreamCtor = globalThis.ReadableStream ?? PonyfillReadableStream;

polyfillGlobal("ReadableStream", () => readableStreamCtor);
