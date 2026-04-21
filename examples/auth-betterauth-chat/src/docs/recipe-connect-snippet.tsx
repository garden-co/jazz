import { useEffect, useState } from "react";
import { JazzProvider } from "jazz-tools/react";
import { authClient } from "../lib/auth-client";

function YourApp() {
  return null;
}

// #region betterauth-jazz-react
export function App() {
  const { data: session, isPending } = authClient.useSession();
  const [token, setToken] = useState<string | undefined>();

  useEffect(() => {
    if (isPending || !session?.session) {
      setToken(undefined);
      return;
    }

    authClient.token().then((res) => {
      if (res.error) return;
      setToken(res.data.token);
    });
  }, [isPending, session?.session?.id]);

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
// #endregion betterauth-jazz-react
