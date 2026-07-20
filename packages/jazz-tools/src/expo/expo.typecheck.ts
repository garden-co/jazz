// Typecheck-only port of the deleted origin/main Expo auth tests to the v2 API.
import {
  ExpoAuthSecretStore,
  expoAuthSecretStore,
  useLocalFirstAuth,
  type ExpoAuthSecretStoreOptions,
  type ExpoSecureStoreLike,
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
  authSecretStorageKey: "expo-auth-key-legacy",
  appId: "expo-app",
  userId: "user-1",
  sessionId: "session-1",
  secureStore,
};

const hookOptions: UseLocalFirstAuthOptions = {
  key: "expo-hook-auth-key",
  authSecretStorageKey: "expo-hook-auth-key-legacy",
  appId: "expo-hook-app",
};

const store = new ExpoAuthSecretStore(options);
store satisfies typeof expoAuthSecretStore;

function HookConsumer() {
  const auth = useLocalFirstAuth(hookOptions);
  auth.secret satisfies string | null;
  return auth;
}

void HookConsumer;
