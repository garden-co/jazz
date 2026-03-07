import { useState } from "react";
import {
  createJazzClient,
  getActiveSyntheticAuth,
  JazzProvider,
  SyntheticUserSwitcher,
  useLinkExternalIdentity,
} from "jazz-tools/react";

function TodoApp() {
  return null;
}

// #region auth-anon-react
const anonymousAuthClient = createJazzClient({
  appId: "my-app",
  env: "dev",
  userBranch: "main",
});

export function AnonymousAuthApp() {
  return (
    <JazzProvider client={anonymousAuthClient}>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-anon-react

// #region auth-anon-token-react
const anonymousAuthWithTokenClient = createJazzClient({
  appId: "my-app",
  localAuthMode: "anonymous",
  localAuthToken: "device-token-123",
});

export function AnonymousAuthWithTokenApp() {
  return (
    <JazzProvider client={anonymousAuthWithTokenClient}>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-anon-token-react

// #region auth-demo-react
const demoAuthAppId = "my-app";
const demoAuthActive = getActiveSyntheticAuth(demoAuthAppId, { defaultMode: "demo" });
const demoAuthClient = createJazzClient({
  appId: demoAuthAppId,
  serverUrl: "http://127.0.0.1:4200",
  localAuthMode: demoAuthActive.localAuthMode,
  localAuthToken: demoAuthActive.localAuthToken,
});

export function DemoAuthApp() {
  return (
    <>
      <SyntheticUserSwitcher appId={demoAuthAppId} defaultMode="demo" />
      <JazzProvider client={demoAuthClient}>
        <TodoApp />
      </JazzProvider>
    </>
  );
}
// #endregion auth-demo-react

// #region auth-external-react
const externalAuthAppId = "my-app";
const externalAuthServerUrl = "http://127.0.0.1:4200";
const externalAuthProviderJwt = "<provider-jwt>";
const externalAuthLocalClient = createJazzClient({
  appId: externalAuthAppId,
  serverUrl: externalAuthServerUrl,
});
const externalAuthJwtClient = createJazzClient({
  appId: externalAuthAppId,
  serverUrl: externalAuthServerUrl,
  jwtToken: externalAuthProviderJwt,
});

export function ExternalAuthApp() {
  const [hasJwt, setHasJwt] = useState(false);
  const linkExternalIdentity = useLinkExternalIdentity({
    appId: externalAuthAppId,
    serverUrl: externalAuthServerUrl,
    defaultMode: "anonymous",
  });

  async function onSignedIn() {
    await linkExternalIdentity({ jwtToken: externalAuthProviderJwt });
    setHasJwt(true);
  }

  return (
    <JazzProvider
      key={hasJwt ? "jwt" : "local"}
      client={hasJwt ? externalAuthJwtClient : externalAuthLocalClient}
    >
      <button onClick={() => onSignedIn()}>Sign in</button>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-external-react

// #region auth-offline-react
const offlineOnlyAuthClient = createJazzClient({ appId: "my-app" });

export function OfflineOnlyAuthApp() {
  return (
    <JazzProvider client={offlineOnlyAuthClient}>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-offline-react
