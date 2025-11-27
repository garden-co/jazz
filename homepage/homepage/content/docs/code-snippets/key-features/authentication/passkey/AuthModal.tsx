import * as React from "react";
import { useState } from "react";
import { usePasskeyAuth } from "jazz-tools/react";
type AuthModalProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
};
// #region Basic
export function AuthModal({ open, onOpenChange }: AuthModalProps) {
  const [username, setUsername] = useState("");

  const auth = usePasskeyAuth({
    // Must be inside the JazzProvider!
    appName: "My super-cool web app",
  });

  if (auth.state === "signedIn") {
    // You can also use `useIsAuthenticated()`
    return <div>You are already signed in</div>;
  }

  const handleSignUp = async () => {
    await auth.signUp(username);
    onOpenChange(false);
  };

  const handleLogIn = async () => {
    await auth.logIn();
    onOpenChange(false);
  };

  return (
    <div>
      <button onClick={handleLogIn}>Log in</button>
      <input
        type="text"
        value={username}
        onChange={(e) => setUsername(e.target.value)}
      />
      <button onClick={handleSignUp}>Sign up</button>
    </div>
  );
}
// #endregion
