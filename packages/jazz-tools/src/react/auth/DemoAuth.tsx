import { useDemoAuth } from "jazz-tools/react-core";
import { useState } from "react";

export const DemoAuthBasicUI = (props: {
  appName: string;
  children?: React.ReactNode;
}) => {
  const auth = useDemoAuth();

  const [username, setUsername] = useState<string>("");

  const darkMode =
    typeof window !== "undefined"
      ? window.matchMedia("(prefers-color-scheme: dark)").matches
      : false;

  if (auth.state === "signedIn") return props.children ?? null;

  const { signUp, logIn, existingUsers } = auth;

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
        onSubmit={(e) => {
          e.preventDefault();
          signUp(username);
        }}
      >
        <input
          placeholder="Display name"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
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
      </form>
      {existingUsers.length > 0 && (
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
          {existingUsers.map((user) => (
            <button
              key={user}
              onClick={() => logIn(user)}
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
    </div>
  );
};
