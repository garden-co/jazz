import {
  usePasskeyAuth,
  usePassphraseAuth,
  useSuspenseAccount,
} from "jazz-tools/react";
import { ProfileForm } from "./ProfileForm";
import { Button } from "./ui/button";
import { Textarea } from "./ui/textarea";
import { MusicaAccount } from "@/1_schema";
import { wordlist } from "@/wordlist";
import { useState } from "react";
import { ArrowLeft } from "lucide-react";

type LoginStep = "initial" | "passphrase-input";

export function WelcomeScreen() {
  const [loginStep, setLoginStep] = useState<LoginStep>("initial");
  const [loginPassphrase, setLoginPassphrase] = useState("");
  const [error, setError] = useState<string | null>(null);

  const passkeyAuth = usePasskeyAuth({
    appName: "Jazz Music Player",
  });

  const passphraseAuth = usePassphraseAuth({
    wordlist,
  });

  const { handleCompleteSetup } = useSuspenseAccount(MusicaAccount, {
    select: (me) => ({
      id: me.root.$jazz.id,
      handleCompleteSetup: () => {
        me.root.$jazz.set("accountSetupCompleted", true);
      },
    }),
    equalityFn: (a, b) => a.id === b.id, // Update only on account change
  });

  if (!handleCompleteSetup) return null;

  const handlePasskeyLogin = () => {
    passkeyAuth.logIn();
  };

  const handlePassphraseLogin = async () => {
    try {
      await passphraseAuth.logIn(loginPassphrase);
      setLoginStep("initial");
      setLoginPassphrase("");
      setError(null);
    } catch (error) {
      if (error instanceof Error) {
        setError(error.message);
      } else {
        setError("Unknown error");
      }
    }
  };

  const handleBack = () => {
    setLoginStep("initial");
    setLoginPassphrase("");
    setError(null);
  };

  return (
    <div className="w-full lg:w-auto min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 flex items-center justify-center p-4">
      <div className="flex flex-col lg:flex-row gap-8 lg:gap-16 items-center">
        {/* Form Panel */}
        <div className="w-full max-w-md bg-white rounded-lg shadow-xl p-8">
          <ProfileForm
            onSubmit={handleCompleteSetup}
            submitButtonText="Continue"
            showHeader={true}
            headerTitle="Welcome to Music Player! 🎵"
            headerDescription="Let's set up your profile to get started"
          />
        </div>

        {/* Mobile Login Section */}
        <div className="lg:hidden pt-4 flex flex-col items-center w-full gap-4">
          <div className="text-sm font-semibold text-gray-600">
            Already a user?
          </div>
          {loginStep === "initial" ? (
            <div className="flex gap-2 w-full max-w-md">
              <Button onClick={handlePasskeyLogin} size="sm" className="flex-1">
                Passkey
              </Button>
              <Button
                onClick={() => setLoginStep("passphrase-input")}
                size="sm"
                variant="outline"
                className="flex-1"
              >
                Passphrase
              </Button>
            </div>
          ) : (
            <div className="w-full max-w-md space-y-3">
              {error && <div className="text-sm text-red-500">{error}</div>}
              <Textarea
                value={loginPassphrase}
                onChange={(e) => setLoginPassphrase(e.target.value)}
                placeholder="Enter your passphrase..."
                className="font-mono text-sm"
                rows={3}
              />
              <div className="flex gap-2">
                <Button onClick={handleBack} size="sm" variant="ghost">
                  <ArrowLeft className="size-4 mr-1" />
                  Back
                </Button>
                <Button
                  onClick={handlePassphraseLogin}
                  size="sm"
                  className="flex-1"
                  disabled={!loginPassphrase.trim()}
                >
                  Login
                </Button>
              </div>
            </div>
          )}
        </div>

        {/* Title Section - Hidden on mobile, shown on right side for larger screens */}
        <div className="hidden lg:flex flex-col justify-center items-start max-w-md">
          <div className="space-y-6">
            <h1 className="text-4xl lg:text-5xl font-bold text-gray-900 leading-tight">
              Your Music at your fingertips.
            </h1>

            <div className="space-y-4">
              <p className="text-xl lg:text-2xl text-gray-700 font-medium">
                Offline, Collaborative, Fast
              </p>

              <div className="flex items-center space-x-2">
                <span className="text-sm text-gray-500 font-medium">
                  Powered by
                </span>
                <a
                  href="https://jazz.tools"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-lg font-bold text-blue-600 hover:underline"
                >
                  Jazz
                </a>
              </div>

              {/* Login Section */}
              <div className="pt-4">
                <p className="text-sm font-semibold text-gray-600 mb-3">
                  Already a user?
                </p>
                {loginStep === "initial" ? (
                  <div className="flex flex-col gap-2">
                    <Button
                      onClick={handlePasskeyLogin}
                      className="bg-blue-600 hover:bg-blue-700 text-white px-6 py-3 text-lg font-medium rounded-lg shadow-lg hover:shadow-xl transition-all duration-200"
                      size="lg"
                    >
                      Login with passkey
                    </Button>
                    <Button
                      onClick={() => setLoginStep("passphrase-input")}
                      variant="outline"
                      size="lg"
                      className="px-6 py-3 text-lg font-medium rounded-lg"
                    >
                      Login with passphrase
                    </Button>
                  </div>
                ) : (
                  <div className="space-y-3">
                    {error && (
                      <div className="text-sm text-red-500">{error}</div>
                    )}
                    <Textarea
                      data-testid="passphrase-input"
                      value={loginPassphrase}
                      onChange={(e) => setLoginPassphrase(e.target.value)}
                      placeholder="Enter your passphrase..."
                      className="font-mono text-sm bg-white"
                      rows={4}
                    />
                    <div className="flex gap-2">
                      <Button onClick={handleBack} variant="ghost">
                        <ArrowLeft className="size-4 mr-2" />
                        Back
                      </Button>
                      <Button
                        onClick={handlePassphraseLogin}
                        className="flex-1 bg-blue-600 hover:bg-blue-700"
                        disabled={!loginPassphrase.trim()}
                      >
                        Login
                      </Button>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
