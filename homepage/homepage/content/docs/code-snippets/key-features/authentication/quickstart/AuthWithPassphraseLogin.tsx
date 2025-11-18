import { useState } from "react";
import { usePasskeyAuth, usePassphraseAuth } from "jazz-tools/react";
import { wordlist } from "./wordlist"; // or the path to your wordlist

export function Auth({ children }: { children: React.ReactNode }) {
  const [name, setName] = useState("");
  // [!code ++:1]
  const [passphraseInput, setPassphraseInput] = useState("");

  const auth = usePasskeyAuth({
    // Must be inside the JazzProvider because the hook depends on an active Jazz context.
    appName: "JazzFest",
  });

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
          <textarea readOnly value={passphraseAuth.passphrase} rows={5} />
        </>
      )}
      {/* [!code ++:8]*/}
      {auth.state !== "signedIn" && (
        <>
          <textarea
            onChange={(e) => setPassphraseInput(e.target.value)}
            rows={5}
          />
          <button onClick={() => passphraseAuth.logIn(passphraseInput)}>
            Sign In with Passphrase
          </button>
        </>
      )}
    </>
  );
}
