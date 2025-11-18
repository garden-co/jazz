import { usePasskeyAuth } from "jazz-tools/react";
import { ProfileForm } from "./ProfileForm";
import { Button } from "./ui/button";
import { useAccountSelector } from "@/components/AccountProvider.tsx";

export function WelcomeScreen() {
  const auth = usePasskeyAuth({
    appName: "Jazz Music Player",
  });

  const { handleCompleteSetup } = useAccountSelector({
    select: (me) =>
      me.$isLoaded
        ? {
            id: me.root.$jazz.id,
            handleCompleteSetup: () => {
              me.root.$jazz.set("accountSetupCompleted", true);
            },
          }
        : { id: null, handleCompleteSetup: null },
    equalityFn: (a, b) => a.id === b.id,
  });

  const initialUsername = useAccountSelector({
    select: (me) => (me.$isLoaded ? me.profile.name : undefined),
  });

  if (!handleCompleteSetup) return null;

  const handleLogin = () => {
    auth.logIn();
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
            initialUsername={initialUsername}
          />
        </div>
        <div className="lg:hidden pt-4 flex justify-end items-center w-full gap-2">
          <div className="text-sm font-semibold text-gray-600">
            Already a user?
          </div>
          <Button onClick={handleLogin} size="sm">
            Login
          </Button>
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

              {/* Login Button */}
              <div className="pt-4">
                <p className="text-sm font-semibold text-gray-600 mb-2">
                  Already a user?
                </p>
                <Button
                  onClick={handleLogin}
                  className="bg-blue-600 hover:bg-blue-700 text-white px-6 py-3 text-lg font-medium rounded-lg shadow-lg hover:shadow-xl transition-all duration-200"
                  size="lg"
                >
                  Login
                </Button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
