import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import { buildInspectorLink } from "./inspector-link.js";
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
  appRoot?: string;
}

const DEVELOPMENT_PHASE = "phase-development-server";
const PRODUCTION_BUILD_PHASE = "phase-production-build";
const PUBLIC_APP_ID_ENV = "NEXT_PUBLIC_JAZZ_APP_ID";
const PUBLIC_SERVER_URL_ENV = "NEXT_PUBLIC_JAZZ_SERVER_URL";
const PUBLIC_TELEMETRY_COLLECTOR_URL_ENV = "NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL";
const SCHEMA_HASH_STUB_SUBPATH = join("node_modules", ".cache", "jazz", "schema-hash.js");
const SCHEMA_HASH_ALIAS = "jazz-tools/_dev/schema-hash";
const WASM_PACKAGE_ALIAS = "jazz-wasm";

function sealedWasmAliases() {
  const sealedWasmPackage = process.env.JAZZ_CORRECTNESS_WASM_PACKAGE;
  if (process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN === "1" && !sealedWasmPackage)
    throw new Error("sealed correctness consumer is missing its admitted WASM package");
  if (!sealedWasmPackage) return undefined;

  const entry = resolve(sealedWasmPackage, "jazz_wasm.js");
  // Turbopack interprets absolute alias targets as server-relative paths. Its
  // project-relative spelling and Webpack's absolute spelling name the same
  // immutable snapshot for every Next runtime import.
  const fromProject = relative(process.cwd(), entry);
  return {
    webpack: entry,
    turbopack:
      fromProject === "" ? "." : fromProject.startsWith(".") ? fromProject : `./${fromProject}`,
  };
}

async function writeSchemaHashStub(appRoot: string, hash: string): Promise<void> {
  const stubPath = join(appRoot, SCHEMA_HASH_STUB_SUBPATH);
  await mkdir(dirname(stubPath), { recursive: true });
  await writeFile(stubPath, `export const HASH = ${JSON.stringify(hash)};\n`);
}

const runtime = new ManagedDevRuntime({
  appId: PUBLIC_APP_ID_ENV,
  serverUrl: PUBLIC_SERVER_URL_ENV,
  telemetryCollectorUrl: PUBLIC_TELEMETRY_COLLECTOR_URL_ENV,
});

function mergeServerExternalPackages(existing: string[] | undefined): string[] {
  return Array.from(new Set([...(existing ?? []), "jazz-napi"]));
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
  let hasLoggedInspectorLink = false;

  return async (phase, context) => {
    const resolved = await resolveConfig(nextConfig, phase, context);
    const sealedWasm = sealedWasmAliases();
    const merged: NextConfigLike = {
      ...resolved,
      serverExternalPackages: mergeServerExternalPackages(resolved.serverExternalPackages),
    };

    const previousWebpack = merged.webpack as
      | ((config: WebpackConfig, ctx: unknown) => WebpackConfig)
      | undefined;
    const previousTurbopack = (merged.turbopack as TurbopackConfig | undefined) ?? {};

    const withSealedWasm = (config: NextConfigLike): NextConfigLike => {
      if (!sealedWasm) return config;
      const configuredWebpack = config.webpack as
        | ((config: WebpackConfig, ctx: unknown) => WebpackConfig)
        | undefined;
      const configuredTurbopack = config.turbopack as TurbopackConfig | undefined;
      return {
        ...config,
        turbopack: {
          ...previousTurbopack,
          ...configuredTurbopack,
          resolveAlias: {
            ...previousTurbopack.resolveAlias,
            ...configuredTurbopack?.resolveAlias,
            [WASM_PACKAGE_ALIAS]: sealedWasm.turbopack,
          },
        },
        webpack: (config: WebpackConfig, ctx: unknown) => {
          const next = configuredWebpack ? configuredWebpack(config, ctx) : config;
          next.resolve = next.resolve ?? {};
          next.resolve.alias = {
            ...next.resolve.alias,
            [WASM_PACKAGE_ALIAS]: sealedWasm.webpack,
          };
          return next;
        },
      };
    };

    // Everything below is dev-only: managed server, APP_ID/SERVER_URL
    // injection. In production the host app supplies those via its own env.
    if (phase !== DEVELOPMENT_PHASE || options.server === false) {
      return withSealedWasm(merged);
    }

    const serverOpt = options.server;
    const explicitBackendSecret =
      typeof serverOpt === "object" && serverOpt !== null && "backendSecret" in serverOpt
        ? serverOpt.backendSecret
        : undefined;
    const backendSecret = explicitBackendSecret ?? process.env.BACKEND_SECRET;

    const resolvedAppRoot = options.appRoot ?? process.cwd();
    const managed = await runtime.initialize({
      ...options,
      backendSecret,
      onSchemaPush: (hash) => writeSchemaHashStub(resolvedAppRoot, hash),
    });
    if (!hasLoggedInspectorLink) {
      console.log(
        `[jazz] Open the inspector: ${buildInspectorLink(
          managed.serverUrl,
          managed.appId,
          managed.adminSecret,
        )}`,
      );
      hasLoggedInspectorLink = true;
    }

    const stubPath = join(resolvedAppRoot, SCHEMA_HASH_STUB_SUBPATH);
    // Turbopack interprets absolute alias targets as server-relative paths and
    // refuses to resolve them. Use the project-root-relative form there. Webpack
    // is happy with either, so feed it the absolute path for clarity.
    const turbopackStubPath = `./${SCHEMA_HASH_STUB_SUBPATH}`;
    return withSealedWasm({
      ...merged,
      env: {
        ...merged.env,
        [PUBLIC_APP_ID_ENV]: managed.appId,
        [PUBLIC_SERVER_URL_ENV]: managed.serverUrl,
        ...(managed.telemetryCollectorUrl
          ? { [PUBLIC_TELEMETRY_COLLECTOR_URL_ENV]: managed.telemetryCollectorUrl }
          : {}),
        ...(managed.backendSecret ? { BACKEND_SECRET: managed.backendSecret } : {}),
      },
      turbopack: {
        ...previousTurbopack,
        resolveAlias: {
          ...previousTurbopack.resolveAlias,
          [SCHEMA_HASH_ALIAS]: turbopackStubPath,
        },
      },
      webpack: (config: WebpackConfig, ctx: unknown) => {
        const next = previousWebpack ? previousWebpack(config, ctx) : config;
        next.resolve = next.resolve ?? {};
        next.resolve.alias = {
          ...next.resolve.alias,
          [SCHEMA_HASH_ALIAS]: stubPath,
        };
        return next;
      },
    });
  };
}

interface WebpackConfig {
  resolve?: { alias?: Record<string, string> };
  [key: string]: unknown;
}

interface TurbopackConfig {
  resolveAlias?: Record<string, string>;
  [key: string]: unknown;
}

export async function __resetJazzNextPluginForTests(): Promise<void> {
  await runtime.resetForTests();
}
