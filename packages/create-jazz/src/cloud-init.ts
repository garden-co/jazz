import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { provisionHostedApp } from "./cloud-provision.js";
import { writeHostedEnv } from "./cloud-env.js";

export interface RunHostedInitOptions {
  /** Absolute path to the directory containing .env (typically the starter root). */
  dir: string;
  /** Sync server URL written to the serverUrl env key. */
  cloudSyncUrl: string;
  /** Map of logical → environment variable names. Lets each framework choose its own prefix. */
  envKeys: {
    appId: string;
    serverUrl: string;
    adminSecret: string;
    backendSecret: string;
  };
  /** Override the provisioning endpoint (defaults to the production cloud dashboard). */
  apiUrl?: string;
}

function readEnvValues(envPath: string): Record<string, string> {
  if (!existsSync(envPath)) return {};
  const content = readFileSync(envPath, "utf8");
  const values: Record<string, string> = {};
  for (let line of content.split("\n")) {
    if (line.endsWith("\r")) line = line.slice(0, -1);
    if (!line || line.startsWith("#")) continue;
    const eq = line.indexOf("=");
    if (eq === -1) continue;
    values[line.slice(0, eq)] = line.slice(eq + 1);
  }
  return values;
}

/**
 * Compose the per-app server URL. The Jazz Cloud proxy routes requests
 * under `/apps/<appId>/…`, so the value we write to .env must already carry
 * the app-specific prefix — otherwise the plugin's schema-push would hit
 * the cloud root and 404.
 */
function composeAppServerUrl(cloudSyncUrl: string, appId: string): string {
  const base = cloudSyncUrl.replace(/\/+$/, "");
  const suffix = `/apps/${appId}`;
  return base.endsWith(suffix) ? `${base}/` : `${base}${suffix}/`;
}

export async function runHostedInit(options: RunHostedInitOptions): Promise<void> {
  const { dir, cloudSyncUrl, envKeys, apiUrl } = options;
  const keys = [envKeys.appId, envKeys.serverUrl, envKeys.adminSecret, envKeys.backendSecret];

  const existing = readEnvValues(join(dir, ".env"));
  if (keys.some((k) => existing[k] && existing[k].length > 0)) {
    return;
  }

  try {
    let provisioned: { appId: string; adminSecret: string; backendSecret: string } | null = null;

    try {
      provisioned = await provisionHostedApp({ apiUrl });
    } catch (err) {
      const name = err instanceof Error ? err.name : "Error";
      console.warn(
        `[jazz] Provisioning failed (${name}: ${err instanceof Error ? err.message : String(err)}). ` +
          `Writing placeholder .env — visit https://v2.dashboard.jazz.tools to provision manually.`,
      );
      writeHostedEnv({ dir, values: {}, keys });
      return;
    }

    const { appId, adminSecret, backendSecret } = provisioned;
    const appServerUrl = composeAppServerUrl(cloudSyncUrl, appId);

    writeHostedEnv({
      dir,
      values: {
        [envKeys.appId]: appId,
        [envKeys.serverUrl]: appServerUrl,
        [envKeys.adminSecret]: adminSecret,
        [envKeys.backendSecret]: backendSecret,
      },
      keys,
    });

    console.log("Jazz app provisioned successfully.");
    console.log(`  ${envKeys.appId}=${appId}`);
    console.log(`  ${envKeys.serverUrl}=${appServerUrl}`);
    console.log(`  ${envKeys.adminSecret}=${adminSecret}`);
    console.log(`  ${envKeys.backendSecret}=${backendSecret}`);
    console.log("");
    console.log("Visit https://v2.dashboard.jazz.tools to sign up and claim your app!");
  } catch (err) {
    console.warn("[jazz] init-env failed unexpectedly:", err instanceof Error ? err.message : err);
    try {
      writeHostedEnv({ dir, values: {}, keys });
    } catch {
      // best-effort: never re-throw from postinstall
    }
  }
}
