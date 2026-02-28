import { useState } from "react";
import { Pressable, Text, View } from "react-native";
import {
  createJazzClient,
  getActiveSyntheticAuth,
  JazzProvider,
  useLinkExternalIdentity,
} from "jazz-tools/react-native";

function TodoApp() {
  return null;
}

// #region auth-anon-expo
export function AnonymousAuthExpoApp() {
  const client = createJazzClient({
    appId: "my-app",
    serverUrl: "http://127.0.0.1:4200",
  });

  return (
    <JazzProvider client={client}>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-anon-expo

// #region auth-anon-token-expo
export function AnonymousAuthWithTokenExpoApp() {
  const client = createJazzClient({
    appId: "my-app",
    localAuthMode: "anonymous",
    localAuthToken: "device-token-123",
  });

  return (
    <JazzProvider client={client}>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-anon-token-expo

// #region auth-demo-expo
export function DemoAuthExpoApp() {
  const appId = "my-app";
  const active = getActiveSyntheticAuth(appId, { defaultMode: "demo" });
  const client = createJazzClient({
    appId,
    serverUrl: "http://127.0.0.1:4200",
    localAuthMode: active.localAuthMode,
    localAuthToken: active.localAuthToken,
  });

  return (
    <JazzProvider client={client}>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-demo-expo

// #region auth-external-expo
export function ExternalAuthExpoApp() {
  const [jwtToken, setJwtToken] = useState<string | undefined>();
  const appId = "my-app";
  const serverUrl = "http://127.0.0.1:4200";
  const linkExternalIdentity = useLinkExternalIdentity({
    appId,
    serverUrl,
    defaultMode: "anonymous",
  });
  const client = createJazzClient({ appId, serverUrl, jwtToken });

  async function onSignedIn(providerJwt: string) {
    await linkExternalIdentity({ jwtToken: providerJwt });
    setJwtToken(providerJwt);
  }

  return (
    <JazzProvider key={jwtToken ?? "local"} client={client}>
      <View>
        <Pressable onPress={() => void onSignedIn("<provider-jwt>")}>
          <Text>Sign in</Text>
        </Pressable>
        <TodoApp />
      </View>
    </JazzProvider>
  );
}
// #endregion auth-external-expo

// #region auth-offline-expo
export function OfflineOnlyAuthExpoApp() {
  const client = createJazzClient({ appId: "my-app" });

  return (
    <JazzProvider client={client}>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-offline-expo
