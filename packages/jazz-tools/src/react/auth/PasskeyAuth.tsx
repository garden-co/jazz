import { BrowserPasskeyAuth } from "jazz-tools/browser";
import {
  useAuthSecretStorage,
  useIsAuthenticated,
  useJazzContextValue,
} from "jazz-tools/react-core";
import { useMemo, useState } from "react";

/**
 * `usePasskeyAuth` hook provides a `JazzAuth` object for passkey authentication.
 *
 * @example
 * ```ts
 * const auth = usePasskeyAuth({ appName, appHostname });
 * ```
 *
 * @category Auth Providers
 */
export function usePasskeyAuth({
  appName,
  appHostname,
}: {
  appName: string;
  appHostname?: string;
}) {
  const context = useJazzContextValue();
  const authSecretStorage = useAuthSecretStorage();

  if ("guest" in context) {
    throw new Error("Passkey auth is not supported in guest mode");
  }

  const authMethod = useMemo(() => {
    return new BrowserPasskeyAuth(
      context.node.crypto,
      context.authenticate,
      authSecretStorage,
      appName,
      appHostname,
    );
  }, [appName, appHostname, authSecretStorage]);

  const isAuthenticated = useIsAuthenticated();

  return {
    state: isAuthenticated ? "signedIn" : "anonymous",
    logIn: authMethod.logIn,
    signUp: authMethod.signUp,
  } as const;
}

export const PasskeyAuthBasicUI = (props: {
  appName: string;
  appHostname?: string;
  children?: React.ReactNode;
}) => {
  const [username, setUsername] = useState<string>("");
  const [error, setError] = useState<string | null>(null);

  const auth = usePasskeyAuth({
    appName: props.appName,
    appHostname: props.appHostname,
  });

  if (auth.state === "signedIn") {
    return props.children ?? null;
  }

  const { logIn, signUp } = auth;

  function handleError(error: Error) {
    if (error.cause instanceof Error) {
      setError(error.cause.message);
    } else {
      setError(error.message);
    }
  }

  return (
    <div
      style={{
        width: "100vw",
        height: "100vh",
        display: "flex",
        flexWrap: "wrap",
        justifyContent: "center",
        alignItems: error ? "inherit" : "center",
      }}
    >
      {error && (
        <div
          style={{
            color: "red",
            width: "100%",
            display: "flex",
            justifyContent: "center",
            alignItems: "end",
            padding: "1rem",
          }}
        >
          {error}
        </div>
      )}
      <div
        style={{
          width: "18rem",
          display: "flex",
          flexDirection: "column",
          gap: "2rem",
        }}
      >
        <form
          style={{
            width: "18rem",
            display: "flex",
            flexDirection: "column",
            gap: "0.5rem",
          }}
          onSubmit={(e) => {
            e.preventDefault();
            setError(null);
            signUp(username).catch(handleError);
          }}
        >
          <input
            placeholder="Display name"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            autoComplete="webauthn"
            style={{
              border: "2px solid #000",
              padding: "11px 8px",
              borderRadius: "6px",
            }}
          />
          <input
            type="submit"
            value="Sign up"
            style={{
              background: "#000",
              color: "#fff",
              padding: "13px 5px",
              border: "none",
              borderRadius: "6px",
              cursor: "pointer",
            }}
          />
        </form>
        <button
          onClick={() => {
            setError(null);
            logIn().catch(handleError);
          }}
          style={{
            background: "#000",
            color: "#fff",
            padding: "13px 5px",
            border: "none",
            borderRadius: "6px",
            cursor: "pointer",
          }}
        >
          Log in with existing account
        </button>
      </div>
    </div>
  );
};
