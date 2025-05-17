import { AgentSecret } from "cojson";
import { Account, ID } from "../internal.js";
import { AuthenticateAccountFunction } from "../types.js";
import { AuthSecretStorage } from "./AuthSecretStorage.js";
import { KvStore, KvStoreContext } from "./KvStoreContext.js";

type StorageData = {
  accountID: ID<Account>;
  accountSecret: AgentSecret;
  secretSeed?: number[];
};

/**
 * `DemoAuth` provides a `JazzAuth` object for demo authentication.
 *
 * Demo authentication is useful for quickly testing your app, as it allows you to create new accounts and log in as existing ones.
 *
 * ```
 * import { DemoAuth } from "jazz-tools";
 *
 * const auth = new DemoAuth(jazzContext.authenticate, new AuthSecretStorage());
 * ```
 *
 * @category Auth Providers
 */
export class DemoAuth {
  constructor(
    private authenticate: AuthenticateAccountFunction,
    private authSecretStorage: AuthSecretStorage,
  ) {}

  logIn = async (username: string) => {
    const existingUsers = await this.getExisitingUsersWithData();
    const storageData = existingUsers[username];

    if (!storageData?.accountID) {
      throw new Error("User not found");
    }

    await this.authenticate({
      accountID: storageData.accountID,
      accountSecret: storageData.accountSecret,
    });

    await this.authSecretStorage.set({
      accountID: storageData.accountID,
      accountSecret: storageData.accountSecret,
      secretSeed: storageData.secretSeed
        ? new Uint8Array(storageData.secretSeed)
        : undefined,
      provider: "demo",
    });
  };

  signUp = async (username: string) => {
    const existingUsers = await this.getExistingUsers();
    if (existingUsers.includes(username)) {
      throw new Error("User already registered");
    }

    const credentials = await this.authSecretStorage.get();

    if (!credentials) {
      throw new Error("No credentials found");
    }

    const currentAccount = await Account.getMe().ensureLoaded({
      resolve: {
        profile: true,
      },
    });

    currentAccount.profile.name = username;

    await this.authSecretStorage.set({
      accountID: credentials.accountID,
      accountSecret: credentials.accountSecret,
      secretSeed: credentials.secretSeed
        ? new Uint8Array(credentials.secretSeed)
        : undefined,
      provider: "demo",
    });

    await this.addToExistingUsers(username, {
      accountID: credentials.accountID,
      accountSecret: credentials.accountSecret,
      secretSeed: credentials.secretSeed
        ? Array.from(credentials.secretSeed)
        : undefined,
    });
  };

  private async addToExistingUsers(username: string, data: StorageData) {
    const existingUsers = await this.getExisitingUsersWithData();

    if (existingUsers[username]) {
      return;
    }

    existingUsers[username] = data;

    const kvStore = KvStoreContext.getInstance().getStorage();
    await kvStore.set("demo-auth-users", JSON.stringify(existingUsers));
  }

  private async getExisitingUsersWithData() {
    const kvStore = KvStoreContext.getInstance().getStorage();
    await migrateExistingUsers(kvStore);

    const existingUsers = await kvStore.get("demo-auth-users");
    return existingUsers ? JSON.parse(existingUsers) : {};
  }

  getExistingUsers = async () => {
    return Object.keys(await this.getExisitingUsersWithData());
  };
}

export function encodeUsername(username: string) {
  return btoa(username)
    .replace(/=/g, "-")
    .replace(/\+/g, "_")
    .replace(/\//g, ".");
}

async function getStorageVersion(kvStore: KvStore) {
  try {
    const version = await kvStore.get("demo-auth-storage-version");
    return version ? parseInt(version) : 1;
  } catch (error) {
    return 1;
  }
}

async function setStorageVersion(kvStore: KvStore, version: number) {
  await kvStore.set("demo-auth-storage-version", version.toString());
}

async function getExistingUsersList(kvStore: KvStore) {
  const existingUsers = await kvStore.get("demo-auth-existing-users");
  return existingUsers ? existingUsers.split(",") : [];
}

/**
 * Migrates existing users keys to work with any storage.
 */
async function migrateExistingUsers(kvStore: KvStore) {
  if ((await getStorageVersion(kvStore)) < 2) {
    const existingUsers = await getExistingUsersList(kvStore);

    for (const username of existingUsers) {
      const legacyKey = `demo-auth-existing-users-${username}`;
      const storageData = await kvStore.get(legacyKey);
      if (storageData) {
        await kvStore.set(
          `demo-auth-existing-users-${encodeUsername(username)}`,
          storageData,
        );
        await kvStore.delete(legacyKey);
      }
    }

    await setStorageVersion(kvStore, 2);
  }

  if ((await getStorageVersion(kvStore)) < 3) {
    const existingUsersList = await getExistingUsersList(kvStore);

    const existingUsers: Record<string, StorageData> = {};
    const keysToDelete: string[] = ["demo-auth-existing-users"];

    for (const username of existingUsersList) {
      const key = `demo-auth-existing-users-${encodeUsername(username)}`;
      const storageData = await kvStore.get(key);
      if (storageData) {
        existingUsers[username] = JSON.parse(storageData);
        keysToDelete.push(key);
      }
    }

    await kvStore.set("demo-auth-users", JSON.stringify(existingUsers));

    for (const key of keysToDelete) {
      await kvStore.delete(key);
    }

    await setStorageVersion(kvStore, 3);
  }
}
