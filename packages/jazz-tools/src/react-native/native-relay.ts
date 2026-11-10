// A namespace import: a host or test stub without Platform is then native, not a link error.
import * as ReactNative from "react-native";
import { REACT_NATIVE_WEB_UNSUPPORTED_ERROR } from "./errors.js";

/**
 * Load jazz-rn's relay, refusing the web target first. react-native-web has no
 * TurboModuleRegistry and no native build, so the relay cannot load there; the
 * lookup alone would fail with an unrelated TypeError.
 */
export function importNativeRelay(): Promise<typeof import("jazz-rn/relay")> {
  if (ReactNative.Platform?.OS === "web")
    return Promise.reject(new Error(REACT_NATIVE_WEB_UNSUPPORTED_ERROR));
  return import("jazz-rn/relay");
}
