import { useState } from "react";
// [!code --:1]
// @ts-expect-error Duplicate imports
import { usePasskeyAuth } from "jazz-tools/react";
// [!code ++:2]
// @ts-expect-error Duplicate imports
import { usePasskeyAuth, usePassphraseAuth } from "jazz-tools/react";
import { wordlist } from "./wordlist"; // or the path to your wordlist

export function Auth({ children }: { children: React.ReactNode }) {
  const [name, setName] = useState("");

  const auth = usePasskeyAuth({
    // Must be inside the JazzProvider because the hook depends on an active Jazz context.
    appName: "JazzFest",
  });

  // [!code ++:1]
  const passphraseAuth = usePassphraseAuth({ wordlist }); // This should be inside the provider too

  return (
    <>
      <div>
        <button onClick={() => auth.logIn()}>Log in</button>
        <input
          type="text"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
        <button onClick={() => auth.signUp(name)}>Sign up</button>
      </div>
      {auth.state === "signedIn" && (
        <>
          {children}
          {/* [!code ++:5]*/}
          <textarea readOnly value={passphraseAuth.passphrase} rows={5} />
        </>
      )}
    </>
  );
}
