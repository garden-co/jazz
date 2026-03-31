import { useState } from "react";
import { Pressable, Text, View } from "react-native";
import {
  getActiveSyntheticAuth,
  JazzProvider,
  useLinkExternalIdentity,
} from "jazz-tools/react-native";

function TodoApp() {
  return null;
}

// #region auth-anon-expo
export function AnonymousAuthExpoApp() {
  return (
    <JazzProvider
      config={{
        appId: "my-app",
        serverUrl: "http://127.0.0.1:4200",
      }}
    >
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-anon-expo

// #region auth-anon-token-expo
export function AnonymousAuthWithTokenExpoApp() {
  return (
    <JazzProvider
      config={{
        appId: "my-app",
        localAuthMode: "anonymous",
        localAuthToken: "device-token-123",
      }}
    >
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-anon-token-expo

// #region auth-demo-expo
const demoAuthExpoAppId = "my-app";
const demoAuthExpoActive = getActiveSyntheticAuth(demoAuthExpoAppId, { defaultMode: "demo" });

export function DemoAuthExpoApp() {
  return (
    <JazzProvider
      config={{
        appId: demoAuthExpoAppId,
        serverUrl: "http://127.0.0.1:4200",
        localAuthMode: demoAuthExpoActive.localAuthMode,
        localAuthToken: demoAuthExpoActive.localAuthToken,
      }}
    >
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-demo-expo

// #region auth-external-expo
const appId = "my-app";
const jazzServerUrl = "http://127.0.0.1:4200";
const providerJwt = "<provider-jwt>";

export function ExternalAuthExpoApp() {
  const [hasJwt, setHasJwt] = useState(false);
  const linkExternalIdentity = useLinkExternalIdentity({
    appId,
    serverUrl: jazzServerUrl,
    defaultMode: "anonymous",
  });

  async function onSignedIn() {
    await linkExternalIdentity({ jwtToken: providerJwt });
    setHasJwt(true);
  }

  return (
    <JazzProvider
      key={hasJwt ? "jwt" : "local"}
      config={
        hasJwt
          ? {
              appId,
              serverUrl: jazzServerUrl,
              jwtToken: providerJwt,
            }
          : {
              appId,
              serverUrl: jazzServerUrl,
            }
      }
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
export function OfflineOnlyAuthExpoApp() {
  return (
    <JazzProvider
      config={{
        appId: "my-app",
      }}
    >
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-offline-expo
