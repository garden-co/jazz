import { SignInButton } from "@clerk/clerk-react";
import { usePassphraseAuth } from "jazz-react";
import { JazzProviderProps, useIsAuthenticated } from "jazz-react";
import { useState } from "react";
import "../index.css";
import { useClerk } from "@clerk/clerk-react";
import { JazzProviderWithClerk } from "jazz-react-auth-clerk";
import { wordlist } from "../wordlist.ts";

export function OmniAuthContainer(props: {
  appName: string;
  wordlist: string[];
  children?: React.ReactNode;
}) {
  const isAuthenticated = useIsAuthenticated();

  const passphraseAuth = usePassphraseAuth({
    wordlist: props.wordlist,
  });

  const [step, setStep] = useState<
    "initial" | "create" | "loginWithPassphrase" | "loginWithClerk"
  >("initial");
  const [loginPassphrase, setLoginPassphrase] = useState("");
  const [isCopied, setIsCopied] = useState(false);
  const [currentPassphrase, setCurrentPassphrase] = useState(() =>
    passphraseAuth.generateRandomPassphrase(),
  );

  if (passphraseAuth.state === "signedIn" || isAuthenticated) {
    return props.children ?? null;
  }

  const handleCreateAccount = async () => {
    setStep("create");
  };

  const handleLoginWithPassphrase = () => {
    setStep("loginWithPassphrase");
  };

  const handleLoginWithClerk = () => {
    setStep("loginWithClerk");
  };

  const handleReroll = () => {
    const newPassphrase = passphraseAuth.generateRandomPassphrase();
    setCurrentPassphrase(newPassphrase);
    setIsCopied(false);
  };

  const handleBack = () => {
    setStep("initial");
    setLoginPassphrase("");
  };

  const handleCopy = async () => {
    await navigator.clipboard.writeText(passphraseAuth.passphrase);
    setIsCopied(true);
  };

  const handleLoginSubmit = async () => {
    await passphraseAuth.logIn(loginPassphrase);
    setStep("initial");
    setLoginPassphrase("");
  };

  const handleNext = async () => {
    await passphraseAuth.registerNewAccount(currentPassphrase, "My Account");
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
              Signup with passphrase
            </button>
            <button
              onClick={handleLoginWithPassphrase}
              className="auth-button-secondary"
            >
              Login with passphrase
            </button>
            <button
              onClick={handleLoginWithClerk}
              className="auth-button-secondary"
            >
              Login with Clerk
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

        {step === "loginWithPassphrase" && (
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

        {step === "loginWithClerk" && <SignInButton />}
      </div>
    </div>
  );
}

export function OmniAuth({
  children,
  AccountSchema,
  sync,
}: {
  children: React.ReactNode;
} & JazzProviderProps) {
  const clerk = useClerk();

  return (
    <JazzProviderWithClerk
      clerk={clerk}
      sync={sync}
      AccountSchema={AccountSchema}
    >
      <OmniAuthContainer
        appName="Jazz Multi-Authentication Example"
        wordlist={wordlist}
      >
        {children}
      </OmniAuthContainer>
    </JazzProviderWithClerk>
  );
}
