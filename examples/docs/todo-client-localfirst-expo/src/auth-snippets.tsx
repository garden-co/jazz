import { Button, Text, View } from "react-native";
import { JazzProvider, useJazzSession } from "jazz-tools/expo";
import { RecoveryPhrase } from "jazz-tools/passphrase";

function TodoApp() {
  return null;
}

// #region auth-localfirst-expo
// Everyone starts as a guest: a device account, whose key Expo keeps in SecureStore.
export function DeviceAccountExpoApp() {
  return (
    <JazzProvider config={{ serverUrl: "https://your-app.example/jazz", initial: "device" }}>
      <View>
        <Text>My App</Text>
        <TodoApp />
      </View>
    </JazzProvider>
  );
}
// #endregion auth-localfirst-expo

// #region auth-localfirst-expo-backup
export function useRecoveryPhrase() {
  const { exportDeviceKey } = useJazzSession();
  return () => RecoveryPhrase.fromSecret(exportDeviceKey());
}
// #endregion auth-localfirst-expo-backup

// #region auth-localfirst-expo-restore
export function useRestoreRecoveryPhrase() {
  const { restoreDeviceAccount } = useJazzSession();
  return (userInput: string) => restoreDeviceAccount(RecoveryPhrase.toSecret(userInput));
}
// #endregion auth-localfirst-expo-restore

// #region auth-refresh-expo
// After your own sign-in or sign-out: Jazz asks your backend who is signed in now.
export function AfterSignIn() {
  const { refresh, pending } = useJazzSession();
  return <Button title="Continue" disabled={!!pending} onPress={() => void refresh()} />;
}
// #endregion auth-refresh-expo
