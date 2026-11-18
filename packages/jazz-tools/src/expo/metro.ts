/**
 * `jazz-tools/expo/metro`: Metro configuration for Jazz's browser client on Expo web.
 *
 * Metro does not follow `new URL(..., import.meta.url)`, which is how other
 * bundlers find Jazz's WASM and SharedWorker, and Expo rewrites
 * `new SharedWorker(new URL(...))` into a dedicated Worker. `withJazz`
 * routes both assets through Metro's own graph instead, on the web platform
 * only. iOS and Android bundles are not changed.
 *
 * It needs Expo's Metro config (`expo/metro-config`): the worker is an Expo
 * worker chunk, which Metro alone does not build.
 */
import { existsSync } from "node:fs";

type Resolution = { type: string; filePath?: string };
type ResolveRequest = (
  // Metro's resolution context; only `resolveRequest`, its default resolver, is used.
  context: any,
  moduleName: string,
  platform: string | null,
) => Resolution;

type MetroResolverConfig = {
  assetExts?: readonly string[];
  resolveRequest?: ResolveRequest | null;
};

const STANDARD_ASSETS = /([\\/]runtime[\\/]browser-worker-assets)\.js$/;

/** The Metro counterpart of Jazz's `browser-worker-assets.js`, when `filePath` is that module. */
function metroAssetsModule(filePath: string | undefined): string | null {
  if (!filePath || !STANDARD_ASSETS.test(filePath)) return null;
  const metroPath = filePath.replace(STANDARD_ASSETS, "$1.metro.js");
  return existsSync(metroPath) ? metroPath : null;
}

/**
 * Wrap a Metro config so Jazz's local-first client runs on Expo web.
 *
 * ```js
 * // metro.config.js
 * const { getDefaultConfig } = require("expo/metro-config");
 * const { withJazz } = require("jazz-tools/expo/metro");
 *
 * module.exports = withJazz(getDefaultConfig(__dirname));
 * ```
 *
 * It adds `wasm` to `resolver.assetExts`, so Metro serves and exports Jazz's
 * WASM binary, and on the web platform resolves Jazz's browser asset module to
 * the one that takes the worker and the binary from Metro. A `resolveRequest`
 * already on the config keeps running first.
 */
export function withJazz<Config extends { resolver?: object }>(config: Config): Config {
  const resolver: MetroResolverConfig = config.resolver ?? {};
  const upstream = resolver.resolveRequest;
  const assetExts = resolver.assetExts ?? [];

  const resolveRequest: ResolveRequest = (context, moduleName, platform) => {
    const resolution = (upstream ?? context.resolveRequest)(context, moduleName, platform);
    if (platform !== "web" || resolution.type !== "sourceFile") return resolution;
    const metroPath = metroAssetsModule(resolution.filePath);
    return metroPath ? { ...resolution, filePath: metroPath } : resolution;
  };

  return {
    ...config,
    resolver: {
      ...resolver,
      assetExts: assetExts.includes("wasm") ? assetExts : [...assetExts, "wasm"],
      resolveRequest,
    },
  };
}
