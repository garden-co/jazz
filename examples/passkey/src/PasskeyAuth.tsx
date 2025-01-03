import { PasskeyAuthState } from "jazz-react";
import { useState } from "react";

export const PasskeyAuth = ({ state }: { state: PasskeyAuthState }) => {
  const [username, setUsername] = useState<string>("");

  if (state.state === "signedIn") {
    return null;
  }

  if (state.state !== "ready") {
    return <div>Loading...</div>;
  }

  const { logIn, signUp } = state;

  return (
    <div
      style={{
        width: "18rem",
        display: "flex",
        flexDirection: "column",
        gap: "2rem",
        margin: "auto",
      }}
    >
      {state.errors.length > 0 && (
        <div style={{ color: "red" }}>
          {state.errors.map((error, index) => (
            <div key={index}>{error}</div>
          ))}
        </div>
      )}
      <form
        style={{
          width: "18rem",
          display: "flex",
          flexDirection: "column",
          gap: "0.5rem",
        }}
        onSubmit={(e) => {
          e.preventDefault();
          signUp(username);
        }}
      >
        <label htmlFor="displayName">Display name</label>
        <input
          id="displayName"
          name="displayName"
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
          }}
        />
      </form>
      <button
        onClick={logIn}
        type="button"
        style={{
          background: "#000",
          color: "#fff",
          padding: "13px 5px",
          border: "none",
          borderRadius: "6px",
        }}
      >
        Log in with existing account
      </button>
    </div>
  );
};
