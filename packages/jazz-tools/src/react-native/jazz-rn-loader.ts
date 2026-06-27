import { createReactNativeDirectCoreUnsupportedError } from "./runtime-module.js";

type JazzRnDefault = never;

export async function loadJazzRn(): Promise<JazzRnDefault> {
  throw createReactNativeDirectCoreUnsupportedError();
}

export function getJazzRnSync(): JazzRnDefault {
  throw createReactNativeDirectCoreUnsupportedError();
}
