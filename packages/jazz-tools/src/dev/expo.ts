import { ManagedDevRuntime } from "./managed-runtime.js";
import type { JazzPluginOptions, JazzServerOptions } from "./vite.js";

export type { JazzPluginOptions, JazzServerOptions };

export interface ExpoConfigLike {
  extra?: Record<string, unknown>;
  [key: string]: unknown;
}

const runtime = new ManagedDevRuntime({
  appId: "EXPO_PUBLIC_JAZZ_APP_ID",
  serverUrl: "EXPO_PUBLIC_JAZZ_SERVER_URL",
});

export async function withJazz(
  expoConfig: ExpoConfigLike,
  options: JazzPluginOptions = {},
): Promise<ExpoConfigLike> {
  if (process.env.NODE_ENV === "production" || options.server === false) {
    return expoConfig;
  }

  const managed = await runtime.initialize(options);

  return {
    ...expoConfig,
    extra: {
      ...expoConfig.extra,
      jazzAppId: managed.appId,
      jazzServerUrl: managed.serverUrl,
    },
  };
}

export async function __resetJazzPluginForTests(): Promise<void> {
  await runtime.resetForTests();
}
