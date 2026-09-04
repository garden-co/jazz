import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import {
  ProvisionHttpError,
  ProvisionNetworkError,
  ProvisionParseError,
  provisionHostedApp,
} from "./cloud-provision.js";
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

/**
 * The CLI and deferred spinner logger share this boundary. Credentials belong
 * in .env, never in terminal output (including a future diagnostic that
 * accidentally interpolates a provision response).
 */
function redactHostedCredentials(message: string, credentials: readonly string[] = []): string {
  let redacted = message;
  for (const credential of credentials) {
    if (credential) redacted = redacted.split(credential).join("[REDACTED]");
  }
  return redacted.replace(
    /\b(?:JAZZ_ADMIN_SECRET|BACKEND_SECRET|adminSecret|backendSecret)\s*[:=]\s*[^\s,;]+/g,
    (match) => `${match.slice(0, match.search(/[:=]/) + 1)}[REDACTED]`,
  );
}

function provisioningDiagnostic(error: unknown): string {
  // Do not stringify an error: network stacks and upstream responses can
  // contain credentials. The type remains useful without exposing its text.
  if (error instanceof ProvisionHttpError) return `HTTP ${error.status} provisioning error`;
  if (error instanceof ProvisionNetworkError) return "network provisioning error";
  if (error instanceof ProvisionParseError) return "invalid provisioning response";
  return "unexpected provisioning error";
}

export async function runHostedInit(options: RunHostedInitOptions): Promise<void> {
  const { dir, cloudSyncUrl, envKeys, apiUrl, onStep, onLog } = options;
  const keys = [envKeys.appId, envKeys.serverUrl, envKeys.adminSecret, envKeys.backendSecret];

  const emit = (kind: "info" | "warn", message: string, credentials?: readonly string[]) => {
    const safeMessage = redactHostedCredentials(message, credentials);
    if (onLog) onLog(kind, safeMessage);
    else if (kind === "info") console.log(safeMessage);
    else console.warn(safeMessage);
  };
  const emitInfo = (message: string, credentials?: readonly string[]) =>
    emit("info", message, credentials);
  const emitWarn = (message: string, credentials?: readonly string[]) =>
    emit("warn", message, credentials);

  const existing = readEnvValues(join(dir, ".env"));
  // A partial .env can be left by an interrupted legacy write. It is not a
  // completed configuration: provision again so writeHostedEnv can fill only
  // the missing/empty placeholders while retaining deliberate user values.
  if (keys.every((key) => existing[key] && existing[key].length > 0)) {
    try {
      // This is normally a content no-op, but it tightens an older managed
      // file's POSIX permissions through the same writer used for updates.
      writeHostedEnv({ dir, values: existing, keys });
    } catch {
      emitWarn("[jazz] Could not secure existing hosted .env permissions.");
    }
    return;
  }

  try {
    let provisioned: { appId: string; adminSecret: string; backendSecret: string } | null = null;

    try {
      onStep?.("Provisioning Jazz Cloud app");
      provisioned = await provisionHostedApp({ apiUrl });
    } catch (err) {
      emitWarn(
        `[jazz] Provisioning failed (${provisioningDiagnostic(err)}). ` +
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
        "  Admin and backend credentials were saved to .env and are not shown here.",
        "",
        "Claim this app in the dashboard within 14 days: https://v2.dashboard.jazz.tools",
        "Unclaimed apps are automatically deleted after 14 days.",
      ].join("\n"),
      [adminSecret, backendSecret],
    );
  } catch (err) {
    emitWarn(`[jazz] init-env failed unexpectedly (${provisioningDiagnostic(err)}).`);
    try {
      writeHostedEnv({ dir, values: {}, keys });
    } catch {
      // best-effort: never re-throw from postinstall
    }
  }
}
