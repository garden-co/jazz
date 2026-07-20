import { Text, View } from "react-native";
import { JazzProvider } from "jazz-tools/react-native";
import { useLocalFirstAuth } from "jazz-tools/expo";
import { RecoveryPhrase } from "jazz-tools/passphrase";

function TodoApp() {
  return null;
}

// #region auth-localfirst-expo
export function LocalFirstAuthExpoApp() {
  const { secret, isLoading } = useLocalFirstAuth();

  if (isLoading || !secret) return null;

  return (
    <JazzProvider
      config={{
        appId: "my-app",
        secret,
      }}
    >
      <View>
        <Text>My App</Text>
        <TodoApp />
      </View>
    </JazzProvider>
  );
}
// #endregion auth-localfirst-expo

// #region auth-localfirst-expo-backup
export function useRecoveryPhraseBackup(): {
  isLoading: boolean;
  recoveryPhrase: string | null;
} {
  const { secret, isLoading } = useLocalFirstAuth();

  return {
    isLoading,
    recoveryPhrase: secret ? RecoveryPhrase.fromSecret(secret) : null,
  };
}
// #endregion auth-localfirst-expo-backup

// #region auth-localfirst-expo-restore
export function useRecoveryPhraseRestore(): (userInput: string) => Promise<void> {
  const { login } = useLocalFirstAuth();

  return async (userInput: string) => {
    const restoredSecret = RecoveryPhrase.toSecret(userInput);
    await login(restoredSecret);
  };
}
// #endregion auth-localfirst-expo-restore
