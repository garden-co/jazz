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
export function AnonymousAuthApp() {
  const client = createJazzClient({
    appId: "my-app",
    env: "dev",
    userBranch: "main",
  });

  return (
    <JazzProvider client={client}>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-anon-react

// #region auth-anon-token-react
export function AnonymousAuthWithTokenApp() {
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
// #endregion auth-anon-token-react

// #region auth-demo-react
export function DemoAuthApp() {
  const appId = "my-app";
  const active = getActiveSyntheticAuth(appId, { defaultMode: "demo" });
  const client = createJazzClient({
    appId,
    serverUrl: "http://127.0.0.1:4200",
    localAuthMode: active.localAuthMode,
    localAuthToken: active.localAuthToken,
  });

  return (
    <>
      <SyntheticUserSwitcher appId={appId} defaultMode="demo" />
      <JazzProvider client={client}>
        <TodoApp />
      </JazzProvider>
    </>
  );
}
// #endregion auth-demo-react

// #region auth-external-react
export function ExternalAuthApp() {
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
      <button onClick={() => onSignedIn("<provider-jwt>")}>Sign in</button>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-external-react

// #region auth-offline-react
export function OfflineOnlyAuthApp() {
  const client = createJazzClient({ appId: "my-app" });

  return (
    <JazzProvider client={client}>
      <TodoApp />
    </JazzProvider>
  );
}
// #endregion auth-offline-react
