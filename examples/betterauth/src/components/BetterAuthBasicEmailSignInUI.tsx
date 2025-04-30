import type { useBetterAuth } from "jazz-react-auth-betterauth";
import { useState } from "react";
import { Alert } from "./atoms/Alert";
import { Button } from "./atoms/Button";
import { Input } from "./atoms/Input";
import { JazzLogo } from "./atoms/JazzLogo";

export const BetterAuthBasicEmailSignInUI = (props: {
  auth: ReturnType<typeof useBetterAuth>;
  setMenuState: React.Dispatch<
    React.SetStateAction<"initial" | "signUp" | "signIn">
  >;
  children?: React.ReactNode;
}) => {
  const [email, setEmail] = useState<string>("");
  const [password, setPassword] = useState<string>("");
  const [error, setError] = useState<Error | undefined>(undefined);

  if (props.auth.state === "signedIn") return props.children ?? null;

  return (
    <div className="min-h-screen flex flex-col justify-center">
      <h1 className="sr-only">Sign in</h1>
      <div className="max-w-md flex flex-col gap-8 w-full px-6 py-12 mx-auto">
        <a onClick={() => props.setMenuState("initial")}>
          <span className="sr-only">Back to home</span>
          <JazzLogo className="w-24 h-auto" />
        </a>

        {error && <Alert variant="danger">{error.message}</Alert>}

        <form
          className="flex flex-col gap-6"
          onSubmit={async (e) => {
            e.preventDefault();
            await props.auth.authClient.signIn.email(
              {
                email,
                password,
              },
              {
                onSuccess: async () => {
                  await props.auth.logIn();
                  props.setMenuState("initial");
                },
                onError: (error) => {
                  setError(error.error);
                },
              },
            );
          }}
        >
          <Input
            label="Email address"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
          />
          <Input
            label="Password"
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
          <Button type="submit">Sign in</Button>
        </form>

        <div className="flex items-center gap-4">
          <hr className="flex-1" />
          <p className="text-center">or</p>
          <hr className="flex-1" />
        </div>

        {/* <div className="flex flex-col gap-4">
					<Button variant="secondary" className="relative">
						<SiGoogle size={16} className="absolute left-3" />
						Continue with Google
					</Button>
					<Button variant="secondary" className="relative">
						<SiGithub size={16} className="absolute left-3" />
						Continue with GitHub
					</Button>
				</div> */}

        <p className="text-sm">
          Don't have an account?{" "}
          <Button onClick={() => props.setMenuState("signUp")}>Sign up</Button>
        </p>
      </div>
    </div>
  );
};
