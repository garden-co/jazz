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
const anonymousAuthExpoClient = createJazzClient({
  appId: "my-app",
  serverUrl: "http://127.0.0.1:4200",
});

export function AnonymousAuthExpoApp() {
  return (
    <JazzProvider client={anonymousAuthExpoClient}>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-anon-expo

// #region auth-anon-token-expo
const anonymousAuthWithTokenExpoClient = createJazzClient({
  appId: "my-app",
  localAuthMode: "anonymous",
  localAuthToken: "device-token-123",
});

export function AnonymousAuthWithTokenExpoApp() {
  return (
    <JazzProvider client={anonymousAuthWithTokenExpoClient}>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-anon-token-expo

// #region auth-demo-expo
const demoAuthExpoAppId = "my-app";
const demoAuthExpoActive = getActiveSyntheticAuth(demoAuthExpoAppId, { defaultMode: "demo" });
const demoAuthExpoClient = createJazzClient({
  appId: demoAuthExpoAppId,
  serverUrl: "http://127.0.0.1:4200",
  localAuthMode: demoAuthExpoActive.localAuthMode,
  localAuthToken: demoAuthExpoActive.localAuthToken,
});

export function DemoAuthExpoApp() {
  return (
    <JazzProvider client={demoAuthExpoClient}>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-demo-expo

// #region auth-external-expo
const externalAuthExpoAppId = "my-app";
const externalAuthExpoServerUrl = "http://127.0.0.1:4200";
const externalAuthExpoProviderJwt = "<provider-jwt>";
const externalAuthExpoLocalClient = createJazzClient({
  appId: externalAuthExpoAppId,
  serverUrl: externalAuthExpoServerUrl,
});
const externalAuthExpoJwtClient = createJazzClient({
  appId: externalAuthExpoAppId,
  serverUrl: externalAuthExpoServerUrl,
  jwtToken: externalAuthExpoProviderJwt,
});

export function ExternalAuthExpoApp() {
  const [hasJwt, setHasJwt] = useState(false);
  const linkExternalIdentity = useLinkExternalIdentity({
    appId: externalAuthExpoAppId,
    serverUrl: externalAuthExpoServerUrl,
    defaultMode: "anonymous",
  });

  async function onSignedIn() {
    await linkExternalIdentity({ jwtToken: externalAuthExpoProviderJwt });
    setHasJwt(true);
  }

  return (
    <JazzProvider
      key={hasJwt ? "jwt" : "local"}
      client={hasJwt ? externalAuthExpoJwtClient : externalAuthExpoLocalClient}
    >
      <View>
        <Pressable onPress={() => void onSignedIn()}>
          <Text>Sign in</Text>
        </Pressable>
        <TodoApp />
      </View>
    </JazzProvider>
  );
}
// #endregion auth-external-expo

// #region auth-offline-expo
const offlineOnlyAuthExpoClient = createJazzClient({ appId: "my-app" });

export function OfflineOnlyAuthExpoApp() {
  return (
    <JazzProvider client={offlineOnlyAuthExpoClient}>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-offline-expo
