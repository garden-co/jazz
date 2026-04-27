import { chmod, mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

export interface CliConfig {
  appId: string;
  serverUrl: string;
  verifierUrl?: string;
  localFirstSecret: string;
}

export function configPath(cwd: string): string {
  return join(cwd, ".skill-issues", "config.json");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

async function readConfigFile(cwd: string): Promise<Record<string, unknown>> {
  const raw = await readFile(configPath(cwd), "utf8").catch(() => "{}");
  const parsed: unknown = JSON.parse(raw);

  return isRecord(parsed) ? parsed : {};
}

export async function readConfig(cwd: string, env: NodeJS.ProcessEnv): Promise<CliConfig> {
  const fromFile = await readConfigFile(cwd);
  const appId = env.SKILL_ISSUES_APP_ID ?? optionalString(fromFile.appId);
  const serverUrl = env.SKILL_ISSUES_SERVER_URL ?? optionalString(fromFile.serverUrl);
  const verifierUrl = env.SKILL_ISSUES_VERIFIER_URL ?? optionalString(fromFile.verifierUrl);
  const localFirstSecret =
    env.SKILL_ISSUES_LOCAL_FIRST_SECRET ?? optionalString(fromFile.localFirstSecret);

  if (!appId) throw new Error("SKILL_ISSUES_APP_ID is required.");
  if (!serverUrl) throw new Error("SKILL_ISSUES_SERVER_URL is required.");
  if (!localFirstSecret) throw new Error("Run issues auth init before using this command.");

  return { appId, serverUrl, verifierUrl, localFirstSecret };
}

export async function writeConfig(cwd: string, config: CliConfig): Promise<void> {
  await mkdir(join(cwd, ".skill-issues"), { recursive: true });
  const path = configPath(cwd);
  await writeFile(path, `${JSON.stringify(config, null, 2)}\n`, {
    mode: 0o600,
  });
  await chmod(path, 0o600);
}
