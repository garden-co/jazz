import { getRandomValues as expoGetRandomValues } from "expo-crypto";
import { polyfillGlobal } from "react-native/Libraries/Utilities/PolyfillFunctions";
import { ReadableStream as PonyfillReadableStream } from "web-streams-polyfill";

const readableStreamCtor = globalThis.ReadableStream ?? PonyfillReadableStream;
const cryptoValue = globalThis.crypto ?? { getRandomValues: expoGetRandomValues };

polyfillGlobal("ReadableStream", () => readableStreamCtor);
polyfillGlobal("crypto", () => cryptoValue);
