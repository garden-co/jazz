import { Paths } from "expo-file-system";
import type { ReactNativeDbConfig } from "../react-native/runtime-source.js";

/** Resolve Expo's document URI to the absolute filesystem path expected by jazz-rn. */
export function expoDataDirectory(documentUri: string = Paths.document.uri): string {
  if (!documentUri.startsWith("file://")) {
    throw new Error(
      `Expo document directory must be a file URI, received ${JSON.stringify(documentUri)}`,
    );
  }

  const path = decodeURI(documentUri.slice("file://".length));
  if (!path.startsWith("/")) {
    throw new Error(
      `Expo document directory did not resolve to an absolute path: ${JSON.stringify(documentUri)}`,
    );
  }
  return path.replace(/\/$/, "") || "/";
}

/** Add Expo's document directory unless the app supplied an explicit directory. */
export function withExpoDataDirectory<Config extends ReactNativeDbConfig>(
  config: Config,
): Config & { dataDirectory: string } {
  return {
    ...config,
    dataDirectory: config.dataDirectory ?? expoDataDirectory(),
  };
}
