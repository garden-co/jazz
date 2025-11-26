import { polyfillGlobal } from 'react-native/Libraries/Utilities/PolyfillFunctions';
import { ReadableStream } from "readable-stream";
polyfillGlobal("ReadableStream", () => ReadableStream); // polyfill ReadableStream
import TextEncoder from 'react-native-fast-encoder'
import "@azure/core-asynciterator-polyfill"; // polyfill Async Iterator
import 'react-native-get-random-values'; // polyfill getRandomValues
polyfillGlobal('TextDecoder', () => TextEncoder); // polyfill TextDecoder
polyfillGlobal('TextEncoder', () => TextEncoder); // polyfill TextEncoder