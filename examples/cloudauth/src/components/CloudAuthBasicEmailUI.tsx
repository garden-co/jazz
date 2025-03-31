import { useCloudAuth } from "jazz-react-auth-cloudauth";
import { useState } from "react";
import { CloudAuthBasicEmailSignInUI } from "./CloudAuthBasicEmailSignInUI";
import { CloudAuthBasicEmailSignUpUI } from "./CloudAuthBasicEmailSignUpUI";
import { Button } from "./atoms/Button";

export const CloudAuthBasicEmailUI = (props: {
  baseUrl: string;
  children?: React.ReactNode;
}) => {
  const auth = useCloudAuth(props.baseUrl);
  const [menuState, setMenuState] = useState<"initial" | "signUp" | "signIn">(
    "initial",
  );

  if (auth.state === "signedIn") return props.children ?? null;

  return (
    <main className="h-full max-w-xl mx-auto px-4 flex flex-col gap-4 justify-center min-h-screen">
      {menuState === "initial" && (
        <div className="w-full flex flex-col gap-6">
          <Button onClick={() => setMenuState("signUp")}>Sign up</Button>
          <Button onClick={() => setMenuState("signIn")}>Sign in</Button>
        </div>
      )}
      {menuState === "signUp" && (
        <CloudAuthBasicEmailSignUpUI auth={auth} setMenuState={setMenuState} />
      )}
      {menuState === "signIn" && (
        <CloudAuthBasicEmailSignInUI auth={auth} setMenuState={setMenuState} />
      )}
    </main>
  );
};
