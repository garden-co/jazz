import {
  onVercelDeployment,
  openVercelCore,
  defaultCoreName,
  refuseVercelAppId,
  type VercelStoreOptions,
} from "./vercel-store.js";
import type { JazzCoreProvider } from "./core-provider.js";
import { openRemoteCore } from "./remote.js";
import { sqlite } from "./sqlite.js";
export type { VercelStoreOptions } from "./vercel-store.js";
/** Provision a Vercel core, or open the supplied local provider outside Vercel. */
export function vercel(
  options: VercelStoreOptions & { local?: JazzCoreProvider } = {},
): JazzCoreProvider {
  refuseVercelAppId(options);
  const { local = sqlite({ path: ".jazz/local" }), ...settings } = options;
  if (!onVercelDeployment()) return local;
  const project = settings.credentials?.projectId ?? process.env.VERCEL_PROJECT_ID;
  let name = settings.name;
  try {
    name ??= project ? defaultCoreName(project, process.env) : undefined;
  } catch {
    // A preview without a branch: report it when opening the provider.
  }
  return {
    ...(name ? { reuse: { key: `vercel:${name}`, settings } } : {}),
    async open() {
      return openVercelCore({
        options: (this.reuse?.settings ?? settings) as VercelStoreOptions,
        openRemote: ({ appId, ...target }) => openRemoteCore(appId, target),
      });
    },
  };
}
