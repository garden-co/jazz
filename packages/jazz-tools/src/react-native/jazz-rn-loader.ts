import { createReactNativeCoreUnsupportedError } from "./runtime-module.js";

type JazzRnDefault = never;

export async function loadJazzRn(): Promise<JazzRnDefault> {
  throw createReactNativeCoreUnsupportedError();
}

export function getJazzRnSync(): JazzRnDefault {
  throw createReactNativeCoreUnsupportedError();
}
