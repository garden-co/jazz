/// <reference path="./vendor.d.ts" />

import { polyfillGlobal } from "react-native/Libraries/Utilities/PolyfillFunctions";
import { fetch as expoFetch } from "expo/fetch";

import { ReadableStream as PonyfillReadableStream } from "web-streams-polyfill";

const readableStreamCtor = globalThis.ReadableStream ?? PonyfillReadableStream;

// expo/fetch's native bridge only accepts a string for `input`, but the
// WHATWG fetch spec also accepts URL and Request. Normalise here so callers
// that pass a URL object (e.g. better-auth's client) don't blow up with
// "The 2nd argument cannot be cast to type URL" inside the native bridge.
const fetchSpecCompliant: typeof globalThis.fetch = async (input, init) => {
  if (typeof input === "string") return expoFetch(input, init);
  if (input instanceof URL) return expoFetch(input.toString(), init);
  const req = input as Request;
  const body = req.body ? await req.arrayBuffer() : undefined;
  return expoFetch(req.url, {
    method: req.method,
    headers: req.headers,
    credentials: req.credentials,
    redirect: req.redirect,
    signal: req.signal,
    body,
    ...init,
  });
};

polyfillGlobal("fetch", () => fetchSpecCompliant);
polyfillGlobal("ReadableStream", () => readableStreamCtor);
