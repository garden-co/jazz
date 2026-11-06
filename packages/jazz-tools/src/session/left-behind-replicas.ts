import { rememberBackendUser, type RememberedBackendUser } from "../accounts/enrollment.js";
import type { AccountHandle } from "../accounts/state.js";
import type { BackendUser } from "./backend-watch.js";

/**
 * With `clearOnSignOut`, the backend users whose local-first replica this
 * browser holds. A sign-out Jazz sees in the page wipes the replica, but one
 * the page doesn't see (a server-side logout followed by a reload, or
 * another user signing in through the app before a reload) leaves it
 * behind: `clearOnSignOut` never reopens the previous user, so nothing would
 * wipe it. This record is what the next page load wipes. It holds what the
 * device remembers of a backend user (account, identity, claims), never a
 * credential.
 */
export function leftBehindReplicas(registry: string) {
  const key = `jazz-clear-on-sign-out-v1:${encodeURIComponent(registry)}`;
  const read = (): RememberedBackendUser[] => {
    try {
      const value = JSON.parse(globalThis.localStorage?.getItem(key) ?? "[]");
      return Array.isArray(value) ? value.filter(isRemembered) : [];
    } catch {
      return [];
    }
  };
  const write = (users: RememberedBackendUser[]) => {
    try {
      if (users.length === 0) globalThis.localStorage?.removeItem(key);
      else globalThis.localStorage?.setItem(key, JSON.stringify(users));
    } catch {
      // Without storage there is no next load to wipe on either.
    }
  };
  return {
    read,
    /** `account` now has a local-first replica on this device. */
    remember(account: AccountHandle) {
      const user = rememberBackendUser(account);
      if (!user) return;
      write([...read().filter((u) => u.account !== user.account), user]);
    },
    forget(account: string) {
      write(read().filter((u) => u.account !== account));
    },
  };
}

function isRemembered(value: unknown): value is RememberedBackendUser {
  const user = value as Partial<RememberedBackendUser> | null;
  return (
    typeof user?.account === "string" &&
    typeof user.identity?.issuer === "string" &&
    typeof user.identity.subject === "string" &&
    !!user.claims &&
    typeof user.claims === "object" &&
    !Array.isArray(user.claims)
  );
}

/**
 * Wipe each replica `record` holds whose user the backend no longer names:
 * someone else signed in, or no one. The user it names keeps theirs (it is
 * their own device's copy, unsynced writes included). A backend that can't be
 * asked wipes nothing: the next load asks again.
 */
export async function wipeLeftBehindReplicas(options: {
  record: ReturnType<typeof leftBehindReplicas>;
  signedIn(): Promise<BackendUser | null>;
  wipe(user: RememberedBackendUser): Promise<void>;
}): Promise<void> {
  const users = options.record.read();
  if (users.length === 0) return;
  let current: BackendUser | null;
  try {
    current = await options.signedIn();
  } catch {
    return;
  }
  for (const user of users) {
    if (
      current &&
      current.issuer === user.identity.issuer &&
      current.subject === user.identity.subject
    )
      continue;
    try {
      await options.wipe(user);
      options.record.forget(user.account);
    } catch (error) {
      console.warn(
        "Jazz could not wipe the local data a signed-out user left on this device (clearOnSignOut); it tries again on the next load.",
        error,
      );
    }
  }
}
