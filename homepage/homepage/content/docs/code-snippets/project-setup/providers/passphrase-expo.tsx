import React, { ReactNode } from "react";
function SignInScreen({ auth }: { auth: any }) {
  return null;
}
const apiKey = "you@example.com";
// ---cut-before---
// #region Passphrase
import { JazzExpoProvider, usePassphraseAuth } from "jazz-tools/expo";
// @ts-expect-error No wordlist
import { englishWordlist } from "./wordlist";

function JazzAuthentication({ children }: { children: ReactNode }) {
  const auth = usePassphraseAuth({
    wordlist: englishWordlist,
  });

  // If the user is already signed in, render the App
  if (auth.state === "signedIn") {
    return children;
  }

  // Otherwise, show a sign-in screen
  return <SignInScreen auth={auth} />;
}

function AuthenticatedProvider({ children }: { children: ReactNode }) {
  return (
    <JazzExpoProvider sync={{ peer: `wss://cloud.jazz.tools/?key=${apiKey}` }}>
      <JazzAuthentication>{children}</JazzAuthentication>
    </JazzExpoProvider>
  );
}
// #endregion
