import { files } from "./blobs.js";
import { isDeepStrictEqual } from "node:util";
import { vercelWebSocketUpgrade } from "./vercel-host.js";
import {
  checkNamespaceOption,
  checkRegionOption,
  coreName,
  onVercelDeployment,
  openVercelCore,
  refuseVercelAppId,
  refuseVercelName,
  type VercelStoreOptions,
} from "./vercel-store.js";

import { backendNative } from "./backend-native.js";
import { sqlite, serverlessHost, type memory } from "./native-provider.js";
import { remote } from "./remote-provider.js";
import type { JazzCoreOpenContext, JazzProvider } from "./core-provider.js";

/**
 * The store for apps deployed on Vercel. Use it in dev too: in local Node development (`next dev`, `vercel dev`, tests) it runs
 * `local`, by default `sqlite({ path: ".jazz/local", blobs: files() })`.
 *
 * Deployed, it runs a core in a persistent Vercel Sandbox in your own Vercel
 * project, with its data on a Drive, in `region` (required, e.g. `"iad1"`,
 * where earlier versions created every core; fixed at creation, and a
 * different region for an existing core throws). `namespace` (required) says which: the
 * Sandbox is `jazz-<project>-<namespace>`, e.g. one core per environment
 * with `namespace: ({ environment }) => environment`, or production's core
 * and one per preview branch with
 * `namespace: ({ environment, branch }) => environment === "production" ? "production" : perBranch(branch)`.
 * It is created on first use, wakes when a request needs it (about a second)
 * and stops after `idleMs` without requests or sockets. The function's Vercel
 * OIDC token is its only credential, so there are no secrets and no env vars:
 * the core admits only tokens naming the workload its first start pinned
 * (team, project and environment by name), and a deployment of another
 * environment refuses to start or retarget it. All previews share one
 * workload, so preview branches' cores are kept apart, not secured from each
 * other. Anyone with Sandbox access to the project controls its cores. The
 * token comes with each request (it can't be read at module scope), so the
 * core opens on the first request, not when `createJazz` is called; work
 * between requests uses the freshest one seen, and open sockets outlive its
 * expiry. A team or project rename (or
 * a change of OIDC issuer mode) changes the workload: delete `/data/trust` in
 * the Sandbox to re-pin the core. Install `@vercel/sandbox` and
 * `@vercel/functions` next to `jazz-tools`. A deployment of a newer jazz-tools
 * restarts the core on its version, and an older deployment never moves it
 * back. Branch cores are not deleted when a branch merges yet: each keeps its
 * Sandbox and a Drive of up to `maxDriveBytes` until you remove them.
 */
export function vercel(
  options: VercelStoreOptions & { local?: ReturnType<typeof sqlite | typeof memory> },
): JazzProvider {
  // Refused here, not on the first deployed request: in dev the local
  // store runs and would never read them.
  refuseVercelAppId(options ?? {});
  refuseVercelName(options ?? {});
  checkNamespaceOption(options ?? {});
  checkRegionOption(options ?? {});
  if (!options.blobs) throw new Error("vercel(): blobs is required");
  const { local, ...rest } = options;
  if (backendNative().host === "node" && !serverlessHost()) {
    return local ?? sqlite({ path: ".jazz/local", blobs: files() });
  }
  return new VercelProvider(rest);
}

function fixedOptions({ sdk: _sdk, namespace, backendCredential, ...fixed }: VercelStoreOptions) {
  // The key already identifies the Sandbox selected by a namespace callback.
  return {
    ...fixed,
    namespace: typeof namespace === "string" ? namespace : undefined,
    customCredential: backendCredential !== undefined,
  };
}

class VercelProvider implements JazzProvider {
  assertSupportedEnvironment() {
    if (backendNative().host !== "node" || !onVercelDeployment()) {
      throw new Error(
        "vercel() requires a Vercel production or preview deployment, or local Node development. Use the provider for this host or remote({ url, appId, secret }).",
      );
    }
  }
  getWebSocketUpgrade(handler: "fetch" | "next") {
    return vercelWebSocketUpgrade(handler);
  }
  constructor(readonly options: VercelStoreOptions) {
    this.current = options;
  }
  get key() {
    const project = this.options.credentials?.projectId ?? process.env.VERCEL_PROJECT_ID;
    try {
      return project
        ? `vercel:${coreName(project, this.options.namespace, process.env).name}`
        : undefined;
    } catch {
      return undefined; // open reports invalid deployment settings
    }
  }
  reload(next: VercelProvider) {
    if (
      !next.options ||
      this.options.sdk !== next.options.sdk ||
      !isDeepStrictEqual(fixedOptions(this.options), fixedOptions(next.options))
    ) {
      throw new Error(
        `createJazz(): ${this.key} is already open in this process with other store settings; restart the process to change them`,
      );
    }
    this.current = next.options;
  }
  private current: VercelStoreOptions;
  open(context: JazzCoreOpenContext) {
    const options = this.options.backendCredential
      ? { ...this.options, backendCredential: () => this.current.backendCredential!() }
      : this.options;
    return openVercelCore({
      options,
      routeClient: context.routeClient,
      openRemote: (target) => remote(target).open(context),
    });
  }
}
