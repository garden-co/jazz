import { fetch as expoFetch } from "expo/fetch";

// Expo fetch provides stream-capable response bodies in React Native.
globalThis.fetch = expoFetch;
