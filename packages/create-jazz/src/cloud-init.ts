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
  /**
   * Progress hook — called with a short step label before long-running work
   * (e.g. the provisioning HTTP request). Lets the caller keep an outer
   * spinner message in sync with what's actually happening.
   */
  onStep?: (label: string) => void;
  /**
   * Output hook — when provided, credential banners and warnings are routed
   * here instead of `console.log` / `console.warn`. Required when the caller
   * has an active clack spinner, since raw `console.*` calls would bleed
   * onto the spinner's active line.
   */
  onLog?: (kind: "info" | "warn", message: string) => void;
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

export async function runHostedInit(options: RunHostedInitOptions): Promise<void> {
  const { dir, cloudSyncUrl, envKeys, apiUrl, onStep, onLog } = options;
  const keys = [envKeys.appId, envKeys.serverUrl, envKeys.adminSecret, envKeys.backendSecret];

  const emitInfo = (message: string) => {
    if (onLog) onLog("info", message);
    else console.log(message);
  };
  const emitWarn = (message: string) => {
    if (onLog) onLog("warn", message);
    else console.warn(message);
  };

  const existing = readEnvValues(join(dir, ".env"));
  if (keys.some((k) => existing[k] && existing[k].length > 0)) {
    return;
  }

  try {
    let provisioned: { appId: string; adminSecret: string; backendSecret: string } | null = null;

    try {
      onStep?.("Provisioning Jazz Cloud app");
      provisioned = await provisionHostedApp({ apiUrl });
    } catch (err) {
      const name = err instanceof Error ? err.name : "Error";
      emitWarn(
        `[jazz] Provisioning failed (${name}: ${err instanceof Error ? err.message : String(err)}). ` +
          `Writing placeholder .env — visit https://v2.dashboard.jazz.tools to provision manually.`,
      );
      writeHostedEnv({ dir, values: {}, keys });
      return;
    }

    const { appId, adminSecret, backendSecret } = provisioned;

    writeHostedEnv({
      dir,
      values: {
        [envKeys.appId]: appId,
        [envKeys.serverUrl]: cloudSyncUrl,
        [envKeys.adminSecret]: adminSecret,
        [envKeys.backendSecret]: backendSecret,
      },
      keys,
    });

    emitInfo(
      [
        "Jazz app provisioned successfully and written to .env:",
        `  ${envKeys.appId}=${appId}`,
        `  ${envKeys.serverUrl}=${cloudSyncUrl}`,
        `  ${envKeys.adminSecret}=${adminSecret}`,
        `  ${envKeys.backendSecret}=${backendSecret}`,
        "",
        "Visit https://v2.dashboard.jazz.tools to sign up and claim your app!",
      ].join("\n"),
    );
  } catch (err) {
    emitWarn(
      `[jazz] init-env failed unexpectedly: ${err instanceof Error ? err.message : String(err)}`,
    );
    try {
      writeHostedEnv({ dir, values: {}, keys });
    } catch {
      // best-effort: never re-throw from postinstall
    }
  }
}
