import { JazzSessionProvider, useJazzSession } from "jazz-tools/react";
import { authClient } from "../lib/auth-client";

function YourApp() {
  return null;
}

// #region betterauth-jazz-react
export function App() {
  return (
    <JazzSessionProvider
      config={{ appId: "my-app", serverUrl: "wss://your-jazz-server.example.com" }}
      fallback={<ConnectAccount />}
    >
      <YourApp />
    </JazzSessionProvider>
  );
}

function ConnectAccount() {
  const { loginJWT, error, status } = useJazzSession();
  // Provider sign-in happens first. Login requires an already registered or
  // linked identity; it never silently creates an account.
  return (
    <>
      {error && <p role="alert">{error.message}</p>}
      <button
        disabled={status === "transitioning"}
        onClick={() =>
          void loginJWT({
            getToken: async () => {
              const result = await authClient.token();
              if (result.error) throw new Error(result.error.message);
              return result.data.token;
            },
          }).catch(() => {})
        }
      >
        Connect signed-in account
      </button>
    </>
  );
}
// #endregion betterauth-jazz-react
