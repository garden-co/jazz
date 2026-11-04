import { Text, View } from "react-native";
import { exportLocalFirstSecret, type AccountHandle } from "jazz-tools/react-native";
import { JazzSessionProvider, useJazzSession } from "jazz-tools/expo";
import { RecoveryPhrase } from "jazz-tools/passphrase";

function TodoApp() {
  return null;
}

// #region auth-localfirst-expo
export function LocalFirstAuthExpoApp() {
  return (
    <JazzSessionProvider
      config={{ appId: "my-app", serverUrl: "https://your-core.example", initial: "local-first" }}
    >
      <View>
        <Text>My App</Text>
        <TodoApp />
      </View>
    </JazzSessionProvider>
  );
}
// #endregion auth-localfirst-expo

// #region auth-localfirst-expo-backup
export function getRecoveryPhrase(account: AccountHandle): string {
  return RecoveryPhrase.fromSecret(exportLocalFirstSecret(account));
}
// #endregion auth-localfirst-expo-backup

// #region auth-localfirst-expo-restore
export function useRestoreRecoveryPhrase() {
  const { restoreLocalFirst } = useJazzSession();
  return (userInput: string) => restoreLocalFirst(RecoveryPhrase.toSecret(userInput));
}
// #endregion auth-localfirst-expo-restore
