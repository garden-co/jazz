export interface JazzServerOptions {
  port?: number;
  adminSecret?: string;
  appId?: string;
  allowAnonymous?: boolean;
  allowDemo?: boolean;
  dataDir?: string;
  inMemory?: boolean;
  jwksUrl?: string;
  catalogueAuthority?: "local" | "forward";
  catalogueAuthorityUrl?: string;
  catalogueAuthorityAdminSecret?: string;
}

export interface JazzPluginOptions {
  server?: boolean | string | JazzServerOptions;
  adminSecret?: string;
  schemaDir?: string;
  appId?: string;
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

const DEVELOPMENT_PHASE = "phase-development-server";

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
  _options: JazzPluginOptions = {},
): NextConfigFactory {
  return async (phase, context) => {
    const resolved = await resolveConfig(nextConfig, phase, context);

    const merged: NextConfigLike = {
      ...resolved,
      serverExternalPackages: mergeServerExternalPackages(resolved.serverExternalPackages),
    };

    if (phase !== DEVELOPMENT_PHASE) {
      return merged;
    }

    return merged;
  };
}

export async function __resetJazzNextPluginForTests(): Promise<void> {}
