import { Button } from "@/basicComponents/ui/button";
import { useIsAuthenticated, usePasskeyAuth } from "jazz-react";
import { KeyRoundIcon, QrCodeIcon } from "lucide-react";
import { useState } from "react";
import { CreateAccountTransferLink } from "./CreateAccountTransferLink";

export const PasskeyAndCrossDeviceAccountTransferAuth = ({
  children,
}: {
  children: React.ReactNode;
}) => {
  const isAuthenticated = useIsAuthenticated();
  const [username, setUsername] = useState("");
  const [isSignUp, setIsSignUp] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [authMethod, setAuthMethod] = useState<
    "passkey" | "auth-transfer" | null
  >(null);

  const passkeyAuth = usePasskeyAuth({
    appName: "Jazz Todo App",
  });

  const handleViewChange = () => {
    setIsSignUp(!isSignUp);
    setError(null);
  };

  const handleSignUp = async () => {
    try {
      await passkeyAuth.signUp(username);
    } catch (error) {
      setError(error instanceof Error ? error.message : "Unknown error");
    }
  };

  const handleLogin = async () => {
    try {
      await passkeyAuth.logIn();
    } catch (error) {
      setError(error instanceof Error ? error.message : "Unknown error");
    }
  };

  if (isAuthenticated) return children;

  if (authMethod === "auth-transfer") {
    return (
      <div className="flex flex-col gap-4 items-center max-w-md mx-auto p-6 text-center">
        <h2 className="text-2xl font-bold">Sign in with mobile device</h2>
        <CreateAccountTransferLink onLoggedIn={() => setAuthMethod(null)} />
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center gap-4 p-6 text-center max-w-md mx-auto">
      <h2 className="text-2xl font-bold">
        {isSignUp ? "Create account" : "Welcome back"}
      </h2>
      <p>Sign {isSignUp ? "up" : "in"} to access your todos</p>

      {isSignUp && (
        <div className="w-full flex flex-col items-start">
          <label htmlFor="username" className="block text-sm font-medium">
            Username
          </label>
          <input
            id="username"
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            placeholder="Enter your username"
            className="w-full px-3 py-2 border rounded-md"
            required
          />
        </div>
      )}

      {error ? <div className="text-sm">{error}</div> : null}

      <div className="flex flex-col gap-3 w-full">
        <Button
          onClick={isSignUp ? handleSignUp : handleLogin}
          className="w-full"
        >
          <KeyRoundIcon className="w-4 h-4 mr-2" />
          {isSignUp ? "Sign up" : "Login"} with passkey
        </Button>

        <Button
          onClick={() => setAuthMethod("auth-transfer")}
          className="w-full"
          variant="outline"
        >
          <QrCodeIcon className="w-4 h-4 mr-2" />
          Sign in with mobile device
        </Button>

        <div className="text-sm">
          {isSignUp ? "Already have an account?" : "Don't have an account?"}{" "}
          <Button type="button" onClick={handleViewChange} variant="link">
            {isSignUp ? "Login" : "Sign up"}
          </Button>
        </div>
      </div>
    </div>
  );
};
