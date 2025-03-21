import { JazzProvider, usePassphraseAuth } from "jazz-react";
import { StrictMode, useState } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";
import { wordlist } from "./wordlist.ts";

function PassphraseAuthBasicUI(props: {
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
  const [currentPassphrase, setCurrentPassphrase] = useState(() =>
    auth.generateRandomPassphrase(),
  );

  if (auth.state === "signedIn") {
    return props.children ?? null;
  }

  const handleCreateAccount = async () => {
    setStep("create");
  };

  const handleLogin = () => {
    setStep("login");
  };

  const handleReroll = () => {
    const newPassphrase = auth.generateRandomPassphrase();
    setCurrentPassphrase(newPassphrase);
    setIsCopied(false);
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
    await auth.logIn(loginPassphrase);
    setStep("initial");
    setLoginPassphrase("");
  };

  const handleNext = async () => {
    await auth.registerNewAccount(currentPassphrase, "My Account");
    setStep("initial");
    setLoginPassphrase("");
  };

  return (
    <div className="auth-container">
      <div className="auth-card">
        {step === "initial" && (
          <div>
            <h1 className="auth-heading">{props.appName}</h1>
            <button
              onClick={handleCreateAccount}
              className="auth-button-primary"
            >
              Create new account
            </button>
            <button onClick={handleLogin} className="auth-button-secondary">
              Log in
            </button>
          </div>
        )}

        {step === "create" && (
          <>
            <h1 className="auth-heading">Your Passphrase</h1>
            <p className="auth-description">
              Please copy and store this passphrase somewhere safe. You'll need
              it to log in.
            </p>
            <textarea
              readOnly
              value={currentPassphrase}
              className="auth-textarea"
              rows={5}
            />
            <button onClick={handleCopy} className="auth-button-primary">
              {isCopied ? "Copied!" : "Copy"}
            </button>
            <div className="auth-button-group">
              <button onClick={handleBack} className="auth-button-secondary">
                Back
              </button>
              <button onClick={handleReroll} className="auth-button-secondary">
                Generate New Passphrase
              </button>
              <button onClick={handleNext} className="auth-button-primary">
                Register
              </button>
            </div>
          </>
        )}

        {step === "login" && (
          <div>
            <h1 className="auth-heading">Log In</h1>
            <textarea
              value={loginPassphrase}
              onChange={(e) => setLoginPassphrase(e.target.value)}
              placeholder="Enter your passphrase"
              className="auth-textarea"
              rows={5}
            />
            <div className="auth-button-group">
              <button onClick={handleBack} className="auth-button-secondary">
                Back
              </button>
              <button
                onClick={handleLoginSubmit}
                className="auth-button-primary"
              >
                Log In
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function JazzAndAuth({ children }: { children: React.ReactNode }) {
  return (
    <JazzProvider
      sync={{
        peer: "wss://cloud.jazz.tools/?key=minimal-auth-passphrase-example@garden.co",
      }}
    >
      <PassphraseAuthBasicUI
        appName="Jazz Minimal Auth Passphrase Example"
        wordlist={wordlist}
      >
        {children}
      </PassphraseAuthBasicUI>
    </JazzProvider>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzAndAuth>
      <App />
    </JazzAndAuth>
  </StrictMode>,
);
