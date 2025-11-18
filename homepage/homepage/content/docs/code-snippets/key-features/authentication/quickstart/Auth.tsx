import { useState } from "react";
import { usePasskeyAuth } from "jazz-tools/react";

export function Auth({ children }: { children: React.ReactNode }) {
  const [name, setName] = useState("");

  const auth = usePasskeyAuth({
    // Must be inside the JazzProvider because the hook depends on an active Jazz context.
    appName: "JazzFest",
  });

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
      {auth.state === "signedIn" && children}
    </>
  );
}
