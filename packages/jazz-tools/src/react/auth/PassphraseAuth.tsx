import { usePassphraseAuth } from "jazz-tools/react-core";
import { useState } from "react";

export function PassphraseAuthBasicUI(props: {
  appName: string;
  wordlist: string[];
  children?: React.ReactNode;
}) {
  const auth = usePassphraseAuth({
    wordlist: props.wordlist,
  });

  const [step, setStep] = useState<"initial" | "create" | "login">("initial");
  const [loginPassphrase, setLoginPassphrase] = useState("");
  const [isCopied, setIsCopied] = useState(false);

  if (auth.state === "signedIn") {
    return props.children ?? null;
  }

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
    await navigator.clipboard.writeText(auth.passphrase);
    setIsCopied(true);
  };

  const handleLoginSubmit = async () => {
    await auth.logIn(loginPassphrase); // Sets the state to signed in

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
        {step === "initial" && (
          <div>
            <h1 style={headingStyle}>{props.appName}</h1>
            <button onClick={handleCreateAccount} style={primaryButtonStyle}>
              Create new account
            </button>
            <button onClick={handleLogin} style={secondaryButtonStyle}>
              Log in
            </button>
          </div>
        )}

        {step === "create" && (
          <>
            <h1 style={headingStyle}>Your Passphrase</h1>
            <p
              style={{
                fontSize: "0.875rem",
                color: "#4b5563",
                textAlign: "center",
                marginBottom: "1rem",
              }}
            >
              Please copy and store this passphrase somewhere safe. You'll need
              it to log in.
            </p>
            <textarea
              readOnly
              value={auth.passphrase}
              style={textareaStyle}
              rows={5}
            />
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
                gap: "1rem",
              }}
            >
              <button onClick={handleBack} style={secondaryButtonStyle}>
                Back
              </button>
              <button onClick={handleCopy} style={primaryButtonStyle}>
                {isCopied ? "Copied!" : "Copy Passphrase"}
              </button>
              <button onClick={handleNext} style={primaryButtonStyle}>
                I have saved it!
              </button>
            </div>
          </>
        )}

        {step === "login" && (
          <div>
            <h1 style={headingStyle}>Log In</h1>
            <textarea
              value={loginPassphrase}
              onChange={(e) => setLoginPassphrase(e.target.value)}
              placeholder="Enter your passphrase"
              style={textareaStyle}
              rows={5}
            />
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
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
        )}
      </div>
    </div>
  );
}

const containerStyle: React.CSSProperties = {
  minHeight: "100vh",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  backgroundColor: "#f3f4f6",
};

const cardStyle: React.CSSProperties = {
  backgroundColor: "white",
  padding: "2rem",
  borderRadius: "0.5rem",
  boxShadow:
    "0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)",
  width: "24rem",
};

const buttonStyle: React.CSSProperties = {
  width: "100%",
  padding: "0.5rem 1rem",
  borderRadius: "0.25rem",
  fontWeight: "bold",
  cursor: "pointer",
  marginBottom: "1rem",
};

const primaryButtonStyle: React.CSSProperties = {
  ...buttonStyle,
  backgroundColor: "black",
  color: "white",
  border: "none",
};

const secondaryButtonStyle: React.CSSProperties = {
  ...buttonStyle,
  backgroundColor: "white",
  color: "black",
  border: "1px solid black",
};

const headingStyle: React.CSSProperties = {
  color: "black",
  fontSize: "1.5rem",
  fontWeight: "bold",
  textAlign: "center",
  marginBottom: "1rem",
};

const textareaStyle: React.CSSProperties = {
  width: "100%",
  padding: "0.5rem",
  border: "1px solid #d1d5db",
  borderRadius: "0.25rem",
  marginBottom: "1rem",
  boxSizing: "border-box",
};
