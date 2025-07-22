/** @jsxImportSource solid-js */
import { type JSX, Match, ParentProps, Switch, createSignal } from "solid-js";
import { usePassphraseAuth } from "../hooks/usePassphraseAuth.js";

const containerStyle: JSX.CSSProperties = {
  "min-height": "100vh",
  display: "flex",
  "align-items": "center",
  "justify-content": "center",
  "background-color": "#f3f4f6",
};

const cardStyle: JSX.CSSProperties = {
  "background-color": "white",
  padding: "2rem",
  "border-radius": "0.5rem",
  "box-shadow":
    "0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)",
  width: "24rem",
};

const buttonStyle: JSX.CSSProperties = {
  width: "100%",
  padding: "0.5rem 1rem",
  "border-radius": "0.25rem",
  "font-weight": "bold",
  cursor: "pointer",
  "margin-bottom": "1rem",
};

const primaryButtonStyle: JSX.CSSProperties = {
  ...buttonStyle,
  "background-color": "black",
  color: "white",
  border: "none",
};

const secondaryButtonStyle: JSX.CSSProperties = {
  ...buttonStyle,
  "background-color": "white",
  color: "black",
  border: "1px solid black",
};

const headingStyle: JSX.CSSProperties = {
  color: "black",
  "font-size": "1.5rem",
  "font-weight": "bold",
  "text-align": "center",
  "margin-bottom": "1rem",
};

const textareaStyle: JSX.CSSProperties = {
  width: "100%",
  padding: "0.5rem",
  border: "1px solid #d1d5db",
  "border-radius": "0.25rem",
  "margin-bottom": "1rem",
  "box-sizing": "border-box",
};

type PassphraseAuthBasicUIProps = {
  readonly appName: string;
  readonly wordlist: string[];
};

export function PassphraseAuthBasicUI(
  props: ParentProps<PassphraseAuthBasicUIProps>,
) {
  const auth = usePassphraseAuth(() => ({
    wordlist: props.wordlist,
  }));

  const [step, setStep] = createSignal<"initial" | "create" | "login">(
    "initial",
  );
  const [loginPassphrase, setLoginPassphrase] = createSignal("");
  const [isCopied, setIsCopied] = createSignal(false);

  const handleCreateAccount = async () => {
    setStep("create");
  };

  const handleLogin = () => {
    setStep("login");
  };

  const handleBack = () => {
    setStep("initial");
    setLoginPassphrase("");
  };

  const handleCopy = async () => {
    await navigator.clipboard.writeText(auth.passphrase());
    setIsCopied(true);
  };

  const handleLoginSubmit = async () => {
    await auth.logIn(loginPassphrase()); // Sets the state to signed in

    // Reset the state in case of logout
    setStep("initial");
    setLoginPassphrase("");
  };

  const handleNext = async () => {
    await auth.signUp(); // Sets the state to signed in

    // Reset the state in case of logout
    setStep("initial");
    setLoginPassphrase("");
  };

  return (
    <div style={containerStyle}>
      <div style={cardStyle}>
        <Switch>
          <Match when={auth.state() === "signedIn"}>{props.children}</Match>
          <Match when={step() === "initial"}>
            <div>
              <h1 style={headingStyle}>{props.appName}</h1>
              <button onClick={handleCreateAccount} style={primaryButtonStyle}>
                Create new account
              </button>
              <button onClick={handleLogin} style={secondaryButtonStyle}>
                Log in
              </button>
            </div>
          </Match>
          <Match when={step() === "create"}>
            <>
              <h1 style={headingStyle}>Your Passphrase</h1>
              <p
                style={{
                  "font-size": "0.875rem",
                  color: "#4b5563",
                  "text-align": "center",
                  "margin-bottom": "1rem",
                }}
              >
                Please copy and store this passphrase somewhere safe. You'll
                need it to log in.
              </p>
              <textarea
                readOnly
                value={auth.passphrase()}
                style={textareaStyle}
                rows={5}
              />
              <div
                style={{
                  display: "flex",
                  "justify-content": "space-between",
                  gap: "1rem",
                }}
              >
                <button onClick={handleBack} style={secondaryButtonStyle}>
                  Back
                </button>
                <button onClick={handleCopy} style={primaryButtonStyle}>
                  {isCopied() ? "Copied!" : "Copy Passphrase"}
                </button>
                <button onClick={handleNext} style={primaryButtonStyle}>
                  I have saved it!
                </button>
              </div>
            </>
          </Match>
          <Match when={step() === "login"}>
            <div>
              <h1 style={headingStyle}>Log In</h1>
              <textarea
                value={loginPassphrase()}
                onChange={(e) => setLoginPassphrase(e.target.value)}
                placeholder="Enter your passphrase"
                style={textareaStyle}
                rows={5}
              />
              <div
                style={{
                  display: "flex",
                  "justify-content": "space-between",
                  gap: "1rem",
                }}
              >
                <button onClick={handleBack} style={secondaryButtonStyle}>
                  Back
                </button>
                <button onClick={handleLoginSubmit} style={primaryButtonStyle}>
                  Log In
                </button>
              </div>
            </div>
          </Match>
        </Switch>
      </div>
    </div>
  );
}
