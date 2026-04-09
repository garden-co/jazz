import { useEffect, useState } from "react";
import { AuthKitProvider, useAuth } from "@workos-inc/authkit-react";
import { JazzProvider } from "jazz-tools/react";

function YourApp() {
  return null;
}

// #region workos-jazz-react
function JazzWithWorkOS() {
  const { user, getAccessToken } = useAuth();
  const [token, setToken] = useState<string | undefined>();

  useEffect(() => {
    if (!user) {
      setToken(undefined);
      return;
    }

    getAccessToken().then((accessToken) => {
      setToken(accessToken ?? undefined);
    });
  }, [getAccessToken, user]);

  return (
    <JazzProvider
      config={{
        appId: "my-app",
        serverUrl: "wss://your-jazz-server.example.com",
        jwtToken: token,
      }}
    >
      <YourApp />
    </JazzProvider>
  );
}

export function App() {
  return (
    <AuthKitProvider clientId="client_01ABC...">
      <JazzWithWorkOS />
    </AuthKitProvider>
  );
}
// #endregion workos-jazz-react
