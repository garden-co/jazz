import { useState } from "react";
import { usePassphraseAuth } from "jazz-tools/react";
type AuthModalProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
};
// #region Basic
import { wordlist } from "./wordlist";

export function AuthModal({ open, onOpenChange }: AuthModalProps) {
  const [loginPassphrase, setLoginPassphrase] = useState("");

  const auth = usePassphraseAuth({
    // Must be inside the JazzProvider!
    wordlist: wordlist,
  });

  if (auth.state === "signedIn") {
    // You can also use `useIsAuthenticated()`
    return <div>You are already signed in</div>;
  }

  const handleSignUp = async () => {
    await auth.signUp();
    onOpenChange(false);
  };

  const handleLogIn = async () => {
    await auth.logIn(loginPassphrase);
    onOpenChange(false);
  };

  return (
    <div>
      <label>
        Your current passphrase
        <textarea readOnly value={auth.passphrase} rows={5} />
      </label>
      <button onClick={handleSignUp}>I have stored my passphrase</button>
      <label>
        Log in with your passphrase
        <textarea
          value={loginPassphrase}
          onChange={(e) => setLoginPassphrase(e.target.value)}
          placeholder="Enter your passphrase"
          rows={5}
          required
        />
      </label>
      <button onClick={handleLogIn}>Log in</button>
    </div>
  );
}
// #endregion
