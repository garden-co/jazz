import React, { ReactNode } from "react";
function SignInScreen({ auth }: { auth: any }) {
  return null;
}
const apiKey = "you@example.com";
// #region Passphrase
import {
  JazzReactNativeProvider,
  usePassphraseAuth,
} from "jazz-tools/react-native";
// @ts-expect-error not included
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
    <JazzReactNativeProvider
      sync={{ peer: `wss://cloud.jazz.tools/?key=${apiKey}` }}
    >
      <JazzAuthentication>{children}</JazzAuthentication>
    </JazzReactNativeProvider>
  );
}
// #endregion
