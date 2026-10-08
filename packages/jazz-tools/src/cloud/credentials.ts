// Local storage for Jazz Cloud CLI sessions.
//
// One JSON file per user, mode 0600 inside a 0700 directory, written by atomic
// rename. It holds the WorkOS refresh token (the long-lived part of a login)
// and the current short-lived access token, keyed by dashboard origin so
// staging and production logins do not overwrite each other. Admin secrets are
// never stored here.

import { chmod, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { randomBytes } from "node:crypto";

export const CREDENTIALS_FILE_VERSION = 1;

export interface WorkosClientConfig {
  clientId: string;
  apiBaseUrl: string;
}

export interface CloudProfile {
  cloudUrl: string;
  workos: WorkosClientConfig;
  accessToken: string;
  /** Unix seconds, from the access token's `exp` claim. */
  accessTokenExpiresAt: number;
  refreshToken: string;
  user: { id: string; email: string };
  createdAt: string;
}

interface CredentialsFile {
  version: typeof CREDENTIALS_FILE_VERSION;
  profiles: Record<string, CloudProfile>;
}

export function configDir(env: NodeJS.ProcessEnv = process.env): string {
  if (env.JAZZ_CONFIG_DIR?.trim()) return env.JAZZ_CONFIG_DIR.trim();
  if (process.platform === "win32" && env.APPDATA) return join(env.APPDATA, "jazz");
  const base = env.XDG_CONFIG_HOME?.trim() || join(homedir(), ".config");
  return join(base, "jazz");
}

export function credentialsPath(env: NodeJS.ProcessEnv = process.env): string {
  return join(configDir(env), "credentials.json");
}

/** Profiles are keyed by origin so trailing slashes or paths cannot split them. */
export function profileKey(cloudUrl: string): string {
  return new URL(cloudUrl).origin;
}

async function readCredentialsFile(path: string): Promise<CredentialsFile> {
  let text: string;
  try {
    text = await readFile(path, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return { version: CREDENTIALS_FILE_VERSION, profiles: {} };
    }
    throw error;
  }
  const parsed = JSON.parse(text) as Partial<CredentialsFile>;
  if (parsed.version !== CREDENTIALS_FILE_VERSION || typeof parsed.profiles !== "object") {
    throw new Error(
      `Unsupported credentials file at ${path}. Run \`jazz-tools logout\` and log in again.`,
    );
  }
  return parsed as CredentialsFile;
}

async function writeCredentialsFile(path: string, file: CredentialsFile): Promise<void> {
  const dir = dirname(path);
  await mkdir(dir, { recursive: true, mode: 0o700 });
  const temp = join(dir, `.credentials.${randomBytes(6).toString("hex")}.tmp`);
  await writeFile(temp, `${JSON.stringify(file, null, 2)}\n`, { mode: 0o600 });
  // writeFile's mode is masked by umask; make the intent explicit.
  await chmod(temp, 0o600).catch(() => {});
  await rename(temp, path);
}

export async function readProfile(
  cloudUrl: string,
  env: NodeJS.ProcessEnv = process.env,
): Promise<CloudProfile | undefined> {
  const file = await readCredentialsFile(credentialsPath(env));
  return file.profiles[profileKey(cloudUrl)];
}

export async function saveProfile(
  profile: CloudProfile,
  env: NodeJS.ProcessEnv = process.env,
): Promise<void> {
  const path = credentialsPath(env);
  const file = await readCredentialsFile(path);
  file.profiles[profileKey(profile.cloudUrl)] = profile;
  await writeCredentialsFile(path, file);
}

export async function deleteProfile(
  cloudUrl: string,
  env: NodeJS.ProcessEnv = process.env,
): Promise<boolean> {
  const path = credentialsPath(env);
  const file = await readCredentialsFile(path);
  const key = profileKey(cloudUrl);
  if (!file.profiles[key]) return false;
  delete file.profiles[key];
  if (Object.keys(file.profiles).length === 0) {
    await rm(path, { force: true });
  } else {
    await writeCredentialsFile(path, file);
  }
  return true;
}
