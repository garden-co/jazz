import { Text, View } from "react-native";
import { JazzProvider } from "jazz-tools/react-native";
import { use } from "react";
import { ExpoAuthSecretStore } from "jazz-tools/expo/auth-secret-store";

function TodoApp() {
  return null;
}

// #region auth-localfirst-expo
export function LocalFirstAuthExpoApp() {
  const secret = use(ExpoAuthSecretStore.getOrCreateSecret());

  return (
    <JazzProvider
      config={{
        appId: "my-app",
        auth: { localFirstSecret: secret },
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
export async function getRecoveryPhraseForBackup(): Promise<string | null> {
  const secret = await ExpoAuthSecretStore.loadSecret();
  if (!secret) return null;
  const { RecoveryPhrase } = await import("jazz-tools/passphrase");
  return RecoveryPhrase.fromSecret(secret);
}
// #endregion auth-localfirst-expo-backup

// #region auth-localfirst-expo-restore
export async function restoreFromRecoveryPhrase(userInput: string): Promise<void> {
  const { RecoveryPhrase } = await import("jazz-tools/passphrase");
  const secret = RecoveryPhrase.toSecret(userInput);
  await ExpoAuthSecretStore.saveSecret(secret);
}
// #endregion auth-localfirst-expo-restore
