// Local storage for Jazz Cloud CLI sessions.
//
// One JSON file per user, mode 0600 inside a 0700 directory, written by atomic
// rename. It holds the WorkOS refresh token (the long-lived part of a login)
// and the current short-lived access token, keyed by dashboard origin so
// staging and production logins do not overwrite each other. Admin secrets are
// never stored here.
//
// Every read-modify-write of the file happens under an exclusive lock file
// (`credentials.lock`, created with O_EXCL) so parallel jazz-tools processes
// cannot drop each other's profiles or race a refresh-token rotation.

import { chmod, link, mkdir, open, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
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
  let parsed: Partial<CredentialsFile>;
  try {
    parsed = JSON.parse(text) as Partial<CredentialsFile>;
  } catch {
    // JSON.parse messages quote the offending text, which could be token material.
    throw new Error(
      `Unreadable credentials file at ${path}. Delete it and run \`jazz-tools login\` again.`,
    );
  }
  if (
    !parsed ||
    typeof parsed !== "object" ||
    parsed.version !== CREDENTIALS_FILE_VERSION ||
    typeof parsed.profiles !== "object" ||
    parsed.profiles === null
  ) {
    throw new Error(
      `Unsupported credentials file at ${path}. Delete it and run \`jazz-tools login\` again.`,
    );
  }
  return parsed as CredentialsFile;
}

async function ensurePrivateDir(dir: string): Promise<void> {
  await mkdir(dir, { recursive: true, mode: 0o700 });
  // mkdir's mode neither survives umask nor tightens an existing directory.
  if (process.platform !== "win32") await chmod(dir, 0o700);
}

async function writeCredentialsFile(path: string, file: CredentialsFile): Promise<void> {
  const dir = dirname(path);
  await ensurePrivateDir(dir);
  const temp = join(dir, `.credentials.${randomBytes(6).toString("hex")}.tmp`);
  try {
    await writeFile(temp, `${JSON.stringify(file, null, 2)}\n`, { mode: 0o600 });
    // writeFile's mode is masked by umask; make the intent explicit.
    await chmod(temp, 0o600).catch(() => {});
    await rename(temp, path);
  } catch (error) {
    await rm(temp, { force: true }).catch(() => {});
    throw error;
  }
}

/**
 * A lock older than this is from a process that hung or died while holding
 * it. Holders keep well under it: the only network call made under the lock
 * (token refresh) has a shorter timeout.
 */
export const STALE_LOCK_MS = 60_000;
const LOCK_RETRY_MS = 25;

export interface CredentialsLockOptions {
  sleep?: (ms: number) => Promise<void>;
  /** Give up waiting for another process after this long (default: STALE_LOCK_MS). */
  timeoutMs?: number;
}

/** Profile access while holding the credentials lock. */
export interface LockedCredentials {
  read(cloudUrl: string): Promise<CloudProfile | undefined>;
  save(profile: CloudProfile): Promise<void>;
  delete(cloudUrl: string): Promise<boolean>;
}

/** `<pid> <nonce>`: the pid detects dead holders, the nonce identifies this acquisition. */
function lockContent(nonce: string): string {
  return `${process.pid} ${nonce}\n`;
}

async function readLock(
  lockPath: string,
): Promise<{ content: string; pid: number; mtimeMs: number } | undefined> {
  try {
    const [content, info] = await Promise.all([readFile(lockPath, "utf8"), stat(lockPath)]);
    return { content, pid: Number.parseInt(content, 10), mtimeMs: info.mtimeMs };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
}

function processIsGone(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return false;
  } catch (error) {
    // EPERM means it exists but belongs to someone else.
    return (error as NodeJS.ErrnoException).code === "ESRCH";
  }
}

/**
 * Removes a lock judged abandoned, but only if it is still the exact lock that
 * was judged: it is renamed away atomically (one breaker wins), and if the
 * renamed file turns out to be a newer, live lock it is linked back.
 */
async function breakLock(lockPath: string, judged: string, nonce: string): Promise<void> {
  const aside = `${lockPath}.${nonce}.stale`;
  try {
    await rename(lockPath, aside);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return; // someone else broke it
    throw error;
  }
  try {
    if ((await readFile(aside, "utf8")) !== judged) {
      // Another waiter broke the stale lock and took a fresh one in between:
      // put it back. link() never replaces an existing file.
      await link(aside, lockPath).catch(() => {});
    }
  } finally {
    await rm(aside, { force: true });
  }
}

/**
 * Runs `fn` while holding an exclusive lock on the credentials file. Not
 * reentrant: `fn` must use the `LockedCredentials` it is given, not the
 * top-level helpers below.
 */
export async function withCredentialsLock<T>(
  fn: (credentials: LockedCredentials) => Promise<T>,
  env: NodeJS.ProcessEnv = process.env,
  options: CredentialsLockOptions = {},
): Promise<T> {
  const path = credentialsPath(env);
  const lockPath = join(dirname(path), "credentials.lock");
  const sleep =
    options.sleep ?? ((ms: number) => new Promise((resolve) => setTimeout(resolve, ms)));
  const deadline = Date.now() + (options.timeoutMs ?? STALE_LOCK_MS);
  const nonce = randomBytes(8).toString("hex");
  const ours = lockContent(nonce);
  await ensurePrivateDir(dirname(path));

  for (;;) {
    try {
      const handle = await open(lockPath, "wx", 0o600);
      await handle.writeFile(ours).finally(() => handle.close());
      break;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
    const held = await readLock(lockPath);
    if (!held) continue;
    if (processIsGone(held.pid) || Date.now() - held.mtimeMs > STALE_LOCK_MS) {
      await breakLock(lockPath, held.content, nonce);
      continue;
    }
    if (Date.now() > deadline) {
      throw new Error(`Timed out waiting for another jazz-tools process to release ${lockPath}.`);
    }
    await sleep(LOCK_RETRY_MS);
  }

  try {
    return await fn({
      read: async (cloudUrl) => (await readCredentialsFile(path)).profiles[profileKey(cloudUrl)],
      save: async (profile) => {
        const file = await readCredentialsFile(path);
        file.profiles[profileKey(profile.cloudUrl)] = profile;
        await writeCredentialsFile(path, file);
      },
      delete: async (cloudUrl) => {
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
      },
    });
  } finally {
    // Only release our own acquisition: if the lock was broken as stale and
    // taken by another process, it is theirs now.
    if ((await readLock(lockPath))?.content === ours) {
      await rm(lockPath, { force: true });
    }
  }
}

export async function readProfile(
  cloudUrl: string,
  env: NodeJS.ProcessEnv = process.env,
): Promise<CloudProfile | undefined> {
  // Writes are atomic renames, so an unlocked read sees a whole file.
  const file = await readCredentialsFile(credentialsPath(env));
  return file.profiles[profileKey(cloudUrl)];
}

export async function saveProfile(
  profile: CloudProfile,
  env: NodeJS.ProcessEnv = process.env,
): Promise<void> {
  await withCredentialsLock((credentials) => credentials.save(profile), env);
}

export async function deleteProfile(
  cloudUrl: string,
  env: NodeJS.ProcessEnv = process.env,
): Promise<boolean> {
  return withCredentialsLock((credentials) => credentials.delete(cloudUrl), env);
}
