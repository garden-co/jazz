import { ManagedDevRuntime } from "./managed-runtime.js";
import type { JazzPluginOptions, JazzServerOptions } from "./vite.js";

export interface NextJazzServerOptions extends JazzServerOptions {
  backendSecret?: string;
}

export interface NextConfigLike {
  env?: Record<string, string | undefined>;
  serverExternalPackages?: string[];
  [key: string]: unknown;
}

interface NextConfigContextLike {
  defaultConfig: NextConfigLike;
}

type NextConfigFactory = (
  phase: string,
  context: NextConfigContextLike,
) => NextConfigLike | Promise<NextConfigLike>;

type NextConfigInput = NextConfigLike | NextConfigFactory;

export interface NextJazzPluginOptions extends JazzPluginOptions {
  server?: boolean | string | NextJazzServerOptions;
}

const DEVELOPMENT_PHASE = "phase-development-server";
const PUBLIC_APP_ID_ENV = "NEXT_PUBLIC_JAZZ_APP_ID";
const PUBLIC_SERVER_URL_ENV = "NEXT_PUBLIC_JAZZ_SERVER_URL";

const runtime = new ManagedDevRuntime({
  appId: PUBLIC_APP_ID_ENV,
  serverUrl: PUBLIC_SERVER_URL_ENV,
});

function mergeServerExternalPackages(existing: string[] | undefined): string[] {
  return Array.from(new Set([...(existing ?? []), "jazz-tools", "jazz-napi"]));
}

async function resolveConfig(
  input: NextConfigInput | undefined,
  phase: string,
  context: NextConfigContextLike,
): Promise<NextConfigLike> {
  if (!input) return {};
  if (typeof input === "function") {
    return (await input(phase, context)) ?? {};
  }
  return input;
}

export function withJazz(
  nextConfig?: NextConfigInput,
  options: NextJazzPluginOptions = {},
): NextConfigFactory {
  return async (phase, context) => {
    const resolved = await resolveConfig(nextConfig, phase, context);
    const merged: NextConfigLike = {
      ...resolved,
      serverExternalPackages: mergeServerExternalPackages(resolved.serverExternalPackages),
    };

    if (phase !== DEVELOPMENT_PHASE || options.server === false) {
      return merged;
    }

    const serverOpt = options.server;
    const backendSecret =
      typeof serverOpt === "object" && serverOpt !== null && "backendSecret" in serverOpt
        ? serverOpt.backendSecret
        : undefined;

    const managed = await runtime.initialize({ ...options, backendSecret });

    return {
      ...merged,
      env: {
        ...merged.env,
        [PUBLIC_APP_ID_ENV]: managed.appId,
        [PUBLIC_SERVER_URL_ENV]: managed.serverUrl,
        ...(managed.backendSecret ? { BACKEND_SECRET: managed.backendSecret } : {}),
      },
    };
  };
}

export async function __resetJazzNextPluginForTests(): Promise<void> {
  await runtime.resetForTests();
}
