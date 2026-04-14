import { Text, View } from "react-native";
import { JazzProvider } from "jazz-tools/react-native";
import { use } from "react";
import { BrowserAuthSecretStore } from "jazz-tools";

function TodoApp() {
  return null;
}

// #region auth-localfirst-expo
export function LocalFirstAuthExpoApp() {
  const secret = use(BrowserAuthSecretStore.getOrCreateSecret());

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
