import { AgentSecret } from "cojson";
import { BrowserDemoAuth } from "jazz-browser";
import { Account, ID } from "jazz-tools";
import { useEffect, useMemo, useState } from "react";

type DemoAuthState = (
  | {
      state: "uninitialized";
    }
  | {
      state: "loading";
    }
  | {
      state: "ready";
      existingUsers: string[];
      signUp: (username: string) => void;
      logInAs: (existingUser: string) => void;
    }
  | {
      state: "signedIn";
      logOut: () => void;
    }
) & {
  errors: string[];
};

/** @category Auth Providers */
export function useDemoAuth({
  seedAccounts,
}: {
  seedAccounts?: {
    [name: string]: { accountID: ID<Account>; accountSecret: AgentSecret };
  };
} = {}) {
  const [state, setState] = useState<DemoAuthState>({
    state: "loading",
    errors: [],
  });

  const authMethod = useMemo(() => {
    return new BrowserDemoAuth(
      {
        onReady: ({ signUp, existingUsers, logInAs }) => {
          setState({
            state: "ready",
            signUp,
            existingUsers,
            logInAs,
            errors: [],
          });
        },
        onSignedIn: ({ logOut }) => {
          setState({ state: "signedIn", logOut, errors: [] });
        },
        onError: (error) => {
          setState((current) => ({
            ...current,
            errors: [...current.errors, error.toString()],
          }));
        },
      },
      seedAccounts,
    );
  }, [seedAccounts]);

  return [authMethod, state] as const;
}

export const DemoAuthBasicUI = ({
  appName,
  state,
  user,
}: {
  appName: string;
  state: DemoAuthState;
  user?: string;
}) => {
  const [username, setUsername] = useState<string>("");
  const darkMode =
    typeof window !== "undefined"
      ? window.matchMedia("(prefers-color-scheme: dark)").matches
      : false;

  const isAutoLogin = !!(user && state.state === "ready");

  useEffect(() => {
    if (!isAutoLogin) return;

    if (state.existingUsers.includes(user)) {
      state.logInAs(user);
    } else {
      state.signUp(user);
    }
  }, [isAutoLogin]);

  if (isAutoLogin) return <></>;

  return (
    <div
      style={{
        minHeight: "100%",
        display: "flex",
        flexDirection: "column",
        justifyContent: "center",
        width: "18rem",
        padding: "1rem 0",
        maxWidth: "calc(100vw - 2rem)",
        gap: "2rem",
        margin: "0 auto",
        ...(darkMode ? { background: "#000" } : {}),
      }}
    >
      {state.state === "loading" ? (
        <div>Loading...</div>
      ) : state.state === "ready" ? (
        <>
          <h1
            style={{
              color: darkMode ? "#fff" : "#000",
              textAlign: "center",
            }}
          >
            {appName}
          </h1>
          {state.errors.map((error) => (
            <div key={error} style={{ color: "red" }}>
              {error}
            </div>
          ))}
          <form
            style={{
              display: "flex",
              flexDirection: "column",
              gap: "0.5rem",
            }}
            onSubmit={(e) => {
              e.preventDefault();
              state.signUp(username);
            }}
          >
            <input
              placeholder="Display name"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              autoComplete="webauthn"
              style={{
                border: darkMode ? "2px solid #444" : "2px solid #ddd",
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
          </form>
          {state.existingUsers.length > 0 && (
            <div
              style={{
                display: "flex",
                flexDirection: "column",
                gap: "0.5rem",
              }}
            >
              <p
                style={{
                  color: darkMode ? "#e2e2e2" : "#000",
                  textAlign: "center",
                  paddingTop: "0.5rem",
                  borderTop: "1px solid",
                  borderColor: darkMode ? "#111" : "#e2e2e2",
                }}
              >
                Log in as
              </p>
              {state.existingUsers.map((user) => (
                <button
                  key={user}
                  onClick={() => state.logInAs(user)}
                  type="button"
                  aria-label={`Log in as ${user}`}
                  style={{
                    background: darkMode ? "#0d0d0d" : "#eee",
                    color: darkMode ? "#fff" : "#000",
                    padding: "0.5rem",
                    border: "none",
                    borderRadius: "6px",
                  }}
                >
                  {user}
                </button>
              ))}
            </div>
          )}
        </>
      ) : null}
    </div>
  );
};
