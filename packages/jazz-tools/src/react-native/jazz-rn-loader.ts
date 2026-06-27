import { createReactNativeDirectCoreAlphaUnsupportedError } from "./runtime-module.js";

type JazzRnDefault = never;

export async function loadJazzRn(): Promise<JazzRnDefault> {
  throw createReactNativeDirectCoreAlphaUnsupportedError();
}

export function getJazzRnSync(): JazzRnDefault {
  throw createReactNativeDirectCoreAlphaUnsupportedError();
}
