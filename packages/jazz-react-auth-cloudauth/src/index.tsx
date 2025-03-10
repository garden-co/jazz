import {
  type AuthClient,
  CloudAuth,
  type Session,
  newAuthClient,
} from "jazz-auth-cloudauth";
import {
  useAuthSecretStorage,
  useIsAuthenticated,
  useJazzContext,
} from "jazz-react";
import { useEffect, useMemo } from "react";
import { useState } from "react";

/**
 * @category Auth Providers
 */
export function useCloudAuth(): {
  readonly state: "signedIn" | "anonymous";
  readonly logIn: (session: Pick<Session, "user">) => Promise<void>;
  readonly signIn: (session: Pick<Session, "user">) => Promise<void>;
  readonly authClient: AuthClient;
} {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();
  const authClient: AuthClient = newAuthClient("http://localhost:3000");
  const keyserver = "http://localhost:6189";

  if ("guest" in context) {
    throw new Error("Cloud auth is not supported in guest mode");
  }

  const authMethod = useMemo(() => {
    return new CloudAuth(
      context.authenticate,
      authSecretStorage,
      authClient,
      keyserver,
    );
  }, []);

  const isAuthenticated = useIsAuthenticated();

  useEffect(() => {
    authClient.useSession.subscribe((value) => {
      authMethod.onUserChange(value.data as Pick<Session, "user">);
    });
  }, []);

  return {
    state: isAuthenticated ? "signedIn" : "anonymous",
    logIn: authMethod.logIn,
    signIn: authMethod.signIn,
    authClient,
  } as const;
}

export const CloudAuthBasicUI = (props: {
  appName: string;
  children?: React.ReactNode;
}) => {
  const auth = useCloudAuth();

  const [name, setName] = useState<string>("");
  const [email, setEmail] = useState<string>("");
  const [password, setPassword] = useState<string>("");

  const darkMode =
    typeof window !== "undefined"
      ? window.matchMedia("(prefers-color-scheme: dark)").matches
      : false;

  if (auth.state === "signedIn") return props.children ?? null;

  const { authClient } = auth;

  return (
    <div
      style={{
        minHeight: "100%",
        display: "flex",
        flexDirection: "column",
        justifyContent: "center",
        width: "18rem",
        maxWidth: "calc(100vw - 2rem)",
        gap: "2rem",
        margin: "0 auto",
      }}
    >
      <h1
        style={{
          color: darkMode ? "#fff" : "#000",
          textAlign: "center",
          fontSize: "1.5rem",
          fontWeight: "bold",
        }}
      >
        {props.appName}
      </h1>
      <form
        style={{
          display: "flex",
          flexDirection: "column",
          gap: "0.5rem",
        }}
        onSubmit={async (e) => {
          const eventSubmitter =
            (e.nativeEvent as SubmitEvent).submitter?.title ?? "";
          e.preventDefault();
          if (eventSubmitter == "Sign up") {
            await authClient.signUp.email({
              email,
              password,
              name,
              accountID: "",
              accountSecret: "",
            });
          } else if (eventSubmitter == "Sign in") {
            await authClient.signIn.email({
              email,
              password,
            });
          }
        }}
      >
        <input
          placeholder="Username (for registration)"
          value={name}
          onChange={(e) => setName(e.target.value)}
          autoComplete="webauthn"
          style={{
            border: darkMode ? "1px solid #444" : "1px solid #ddd",
            padding: "11px 8px",
            borderRadius: "6px",
            background: darkMode ? "#000" : "#fff",
            color: darkMode ? "#fff" : "#000",
          }}
        />
        <input
          placeholder="Email"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          autoComplete="webauthn"
          style={{
            border: darkMode ? "1px solid #444" : "1px solid #ddd",
            padding: "11px 8px",
            borderRadius: "6px",
            background: darkMode ? "#000" : "#fff",
            color: darkMode ? "#fff" : "#000",
          }}
        />
        <input
          placeholder="Password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          autoComplete="webauthn"
          style={{
            border: darkMode ? "1px solid #444" : "1px solid #ddd",
            padding: "11px 8px",
            borderRadius: "6px",
            background: darkMode ? "#000" : "#fff",
            color: darkMode ? "#fff" : "#000",
          }}
        />
        <input
          type="submit"
          value="Sign up"
          style={{
            padding: "13px 5px",
            border: "none",
            borderRadius: "6px",
            cursor: "pointer",
            background: darkMode ? "#444" : "#ddd",
            color: darkMode ? "#fff" : "#000",
          }}
        />
        <input
          type="submit"
          value="Sign in"
          style={{
            padding: "13px 5px",
            border: "none",
            borderRadius: "6px",
            cursor: "pointer",
            background: darkMode ? "#444" : "#ddd",
            color: darkMode ? "#fff" : "#000",
          }}
        />
      </form>
    </div>
  );
};
