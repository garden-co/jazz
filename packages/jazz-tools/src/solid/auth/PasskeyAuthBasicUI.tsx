/**
 * @jsxImportSource solid-js
 */
import { type ParentProps, Show, createSignal } from "solid-js";
import { usePasskeyAuth } from "../hooks/usePasskeyAuth.js";

const formStyle = {
  display: "flex",
  "flex-direction": "column",
  gap: "0.5rem",
} as const;

const buttonStyle = {
  background: "#000",
  color: "#fff",
  padding: "6px 12px",
  border: "none",
  "border-radius": "6px",
  "min-height": "38px",
  cursor: "pointer",
} as const;

const inputStyle = {
  border: "2px solid #000",
  padding: "6px 12px",
  "border-radius": "6px",
  "min-height": "24px",
} as const;

interface PasskeyAuthBasicUIProps {
  appName: string;
}

export const PasskeyAuthBasicUI = (
  props: ParentProps<PasskeyAuthBasicUIProps>,
) => {
  const auth = usePasskeyAuth(() => ({ appName: props.appName }));
  const [error, setError] = createSignal<string | undefined>(undefined);

  const signUp = (e: Event) => {
    const formData = new FormData(e.currentTarget as HTMLFormElement);
    const name = formData.get("name") as string;

    if (!name) {
      setError("Name is required");
      return;
    }
    e.preventDefault();
    setError(undefined);
    auth
      .auth()
      .signUp(name)
      .catch((e) => {
        setError(e.message);
      });
  };

  const logIn = (e: Event) => {
    setError(undefined);
    e.preventDefault();
    e.stopPropagation();
    auth
      .auth()
      .logIn()
      .catch((e) => {
        setError(e.message);
      });
  };

  return (
    <Show when={auth.state() === "anonymous"} fallback={props.children}>
      <div
        style={{
          width: "100vw",
          height: "100vh",
          display: "flex",
          "align-items": "center",
          "justify-content": "center",
        }}
      >
        <div
          style={{
            "max-width": "18rem",
            display: "flex",
            "flex-direction": "column",
            gap: "2rem",
          }}
        >
          <Show when={error()}>
            <div style={{ color: "red" }}>{error()}</div>
          </Show>
          <form onSubmit={signUp} style={formStyle}>
            <input
              type="text"
              name="name"
              placeholder="Display name"
              autocomplete="name"
              style={inputStyle}
            />
            <input type="submit" value="Sign up" style={buttonStyle} />
          </form>
          <button onClick={logIn} style={buttonStyle}>
            Log in with existing account
          </button>
        </div>
      </div>
    </Show>
  );
};
