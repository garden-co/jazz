// Typecheck-only port of the deleted origin/main Expo auth tests to the v2 API.
import {
  createAccountManager,
  createJazzClient,
  useAccountState,
  useSyncStatus,
  type SyncStatus,
  ExpoAuthSecretStore,
  expoAuthSecretStore,
  useLocalFirstAuth,
  type ExpoAuthSecretStoreOptions,
  type ExpoSecureStoreLike,
  type JazzClientConfig,
  type UseLocalFirstAuthOptions,
} from "./index.js";

const secureStore: ExpoSecureStoreLike = {
  async getItemAsync(_key) {
    return null;
  },
  async setItemAsync(_key, _value) {},
  async deleteItemAsync(_key) {},
};

const options: ExpoAuthSecretStoreOptions = {
  key: "expo-auth-key",
  appId: "expo-app",
  profile: "default-profile",
  secureStore,
};

const hookOptions: UseLocalFirstAuthOptions = {
  key: "expo-hook-auth-key",
  scope: "expo-hook-app",
};

const store = new ExpoAuthSecretStore(options);
store satisfies typeof expoAuthSecretStore;

function HookConsumer() {
  const auth = useLocalFirstAuth(hookOptions);
  auth.secret satisfies string | null;
  return auth;
}

void HookConsumer;

// The app's own token for its backend, passed through to the account manager.
const withBearer: JazzClientConfig = {
  serverUrl: "https://app.example.test/jazz",
  bearer: () => "app-token",
};
void createJazzClient(withBearer);

// The account and sync hooks are on the Expo entry too, not only on react-native.
declare const accounts: Awaited<ReturnType<typeof createAccountManager>>;
export function useAccountsAndSync() {
  return { account: useAccountState(accounts), sync: useSyncStatus() satisfies SyncStatus };
}
