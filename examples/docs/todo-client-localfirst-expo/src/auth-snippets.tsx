import { Text, View } from "react-native";
import {
  JazzProvider,
  exportLocalFirstSecret,
  useAccountState,
  type AccountHandle,
} from "jazz-tools/react-native";
import { createAccountManager } from "jazz-tools/expo";
import { RecoveryPhrase } from "jazz-tools/passphrase";

type Accounts = Awaited<ReturnType<typeof createAccountManager>>;
function TodoApp() {
  return null;
}

// #region auth-localfirst-expo
// Prepare once during bootstrap; native crypto/storage need no Jazz context.
export async function prepareAccounts() {
  const accounts = await createAccountManager({
    appId: "my-app",
    serverUrl: "https://your-core.example",
  });
  if (!accounts.getLoggedIn()) accounts.createLocalFirst();
  return accounts;
}

export function LocalFirstAuthExpoApp({ accounts }: { accounts: Accounts }) {
  const { account, error } = useAccountState(accounts);
  if (error) return <Text accessibilityRole="alert">{error.message}</Text>;
  if (!account) return null;
  return (
    <JazzProvider config={{ appId: "my-app", account }}>
      <View>
        <Text>My App</Text>
        <TodoApp />
      </View>
    </JazzProvider>
  );
}
// #endregion auth-localfirst-expo

// #region auth-localfirst-expo-backup
export function getRecoveryPhrase(account: AccountHandle): string {
  return RecoveryPhrase.fromSecret(exportLocalFirstSecret(account));
}
// #endregion auth-localfirst-expo-backup

// #region auth-localfirst-expo-restore
// Restore before creating a context. If switching an existing context, first
// await client.shutdown({ waitForSync: true }); then restore and open a new one.
export function restoreRecoveryPhrase(accounts: Accounts, userInput: string): AccountHandle {
  return accounts.restoreLocalFirst(RecoveryPhrase.toSecret(userInput));
}
// #endregion auth-localfirst-expo-restore
