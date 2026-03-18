/// <reference path="./vendor.d.ts" />

import { polyfillGlobal } from "react-native/Libraries/Utilities/PolyfillFunctions";
import {
  Headers as ReactNativeHeaders,
  Request as ReactNativeRequest,
  Response as ReactNativeResponse,
} from "react-native/Libraries/Network/fetch";
import { fetch as expoFetch } from "expo/fetch";

import { ReadableStream as PonyfillReadableStream } from "web-streams-polyfill";

const readableStreamCtor = globalThis.ReadableStream ?? PonyfillReadableStream;

polyfillGlobal("fetch", () => expoFetch);
polyfillGlobal("Headers", () => ReactNativeHeaders ?? globalThis.Headers);
polyfillGlobal("Request", () => ReactNativeRequest ?? globalThis.Request);
polyfillGlobal("Response", () => ReactNativeResponse ?? globalThis.Response);
polyfillGlobal("ReadableStream", () => readableStreamCtor);
