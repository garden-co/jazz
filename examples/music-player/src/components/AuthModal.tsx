import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Textarea } from "@/components/ui/textarea";
import {
  usePasskeyAuth,
  usePassphraseAuth,
  useSuspenseAccount,
} from "jazz-tools/react";
import { useState } from "react";
import { MusicaAccount, PlaylistWithTracks } from "@/1_schema";
import { wordlist } from "@/wordlist";
import { Copy, Check, ArrowLeft } from "lucide-react";

interface AuthModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

type AuthStep =
  | "choose"
  | "passkey-signup"
  | "passkey-login"
  | "passphrase-signup"
  | "passphrase-login";

export function AuthModal({ open, onOpenChange }: AuthModalProps) {
  const [step, setStep] = useState<AuthStep>("choose");
  const [error, setError] = useState<string | null>(null);
  const [loginPassphrase, setLoginPassphrase] = useState("");
  const [isCopied, setIsCopied] = useState(false);

  const profileName = useSuspenseAccount(MusicaAccount, {
    select: (me) => me.profile.name,
  });

  const passkeyAuth = usePasskeyAuth({
    appName: "Jazz Music Player",
  });

  const passphraseAuth = usePassphraseAuth({
    wordlist,
  });

  const [generatedPassphrase, setGeneratedPassphrase] = useState(() =>
    passphraseAuth.generateRandomPassphrase(),
  );

  const handleBack = () => {
    setStep("choose");
    setError(null);
    setLoginPassphrase("");
    setIsCopied(false);
  };

  const handlePasskeySubmit = async (isSignUp: boolean) => {
    try {
      if (isSignUp) {
        if (profileName) {
          await passkeyAuth.signUp(profileName);
        }
      } else {
        await passkeyAuth.logIn();
      }
      onOpenChange(false);
    } catch (error) {
      if (error instanceof Error) {
        if (error.cause instanceof Error) {
          setError(error.cause.message);
        } else {
          setError(error.message);
        }
      } else {
        setError("Unknown error");
      }
    }
  };

  const handlePassphraseSignUp = async () => {
    try {
      if (profileName) {
        await passphraseAuth.registerNewAccount(
          generatedPassphrase,
          profileName,
        );
      }
      onOpenChange(false);
      setStep("choose");
      setLoginPassphrase("");
      setIsCopied(false);
    } catch (error) {
      if (error instanceof Error) {
        setError(error.message);
      } else {
        setError("Unknown error");
      }
    }
  };

  const handlePassphraseLogin = async () => {
    try {
      await passphraseAuth.logIn(loginPassphrase);
      onOpenChange(false);
      setStep("choose");
      setLoginPassphrase("");
    } catch (error) {
      if (error instanceof Error) {
        setError(error.message);
      } else {
        setError("Unknown error");
      }
    }
  };

  const handleCopy = async () => {
    await navigator.clipboard.writeText(generatedPassphrase);
    setIsCopied(true);
  };

  const handleReroll = () => {
    const newPassphrase = passphraseAuth.generateRandomPassphrase();
    setGeneratedPassphrase(newPassphrase);
    setIsCopied(false);
  };

  const shouldShowTransferRootPlaylist = useSuspenseAccount(MusicaAccount, {
    resolve: {
      root: {
        rootPlaylist: PlaylistWithTracks.resolveQuery,
      },
    },
    select: (me) =>
      (step === "passkey-login" || step === "passphrase-login") &&
      me.root.rootPlaylist.tracks.some((track) => !track.isExampleTrack),
  });

  const renderContent = () => {
    switch (step) {
      case "choose":
        return (
          <>
            <DialogHeader>
              <DialogTitle className="text-2xl font-bold">
                Welcome to Music Player
              </DialogTitle>
              <DialogDescription>
                Sign up to enable network sync and share your playlists with
                others
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-3 pt-4">
              <div className="space-y-2">
                <p className="text-sm font-medium text-gray-700">
                  Create account
                </p>
                <Button
                  onClick={() => setStep("passkey-signup")}
                  className="w-full bg-blue-600 hover:bg-blue-700"
                >
                  Sign up with passkey
                </Button>
                <Button
                  onClick={() => setStep("passphrase-signup")}
                  variant="outline"
                  className="w-full"
                >
                  Sign up with passphrase
                </Button>
              </div>
              <div className="relative py-2">
                <div className="absolute inset-0 flex items-center">
                  <span className="w-full border-t" />
                </div>
                <div className="relative flex justify-center text-xs uppercase">
                  <span className="bg-white px-2 text-gray-500">Or</span>
                </div>
              </div>
              <div className="space-y-2">
                <p className="text-sm font-medium text-gray-700">
                  Already have an account?
                </p>
                <Button
                  onClick={() => setStep("passkey-login")}
                  variant="outline"
                  className="w-full"
                >
                  Login with passkey
                </Button>
                <Button
                  onClick={() => setStep("passphrase-login")}
                  variant="outline"
                  className="w-full"
                >
                  Login with passphrase
                </Button>
              </div>
            </div>
          </>
        );

      case "passkey-signup":
        return (
          <>
            <DialogHeader>
              <DialogTitle className="text-2xl font-bold">
                Create account with passkey
              </DialogTitle>
              <DialogDescription>
                Use your device's biometric authentication to create a secure
                account
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-4 pt-4">
              {error && <div className="text-sm text-red-500">{error}</div>}
              <Button
                onClick={() => handlePasskeySubmit(true)}
                className="w-full bg-blue-600 hover:bg-blue-700"
              >
                Create passkey
              </Button>
              <Button onClick={handleBack} variant="ghost" className="w-full">
                <ArrowLeft className="size-4 mr-2" />
                Back
              </Button>
            </div>
          </>
        );

      case "passkey-login":
        return (
          <>
            <DialogHeader>
              <DialogTitle className="text-2xl font-bold">
                Login with passkey
              </DialogTitle>
              <DialogDescription>
                Use your saved passkey to log in
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-4 pt-4">
              {error && <div className="text-sm text-red-500">{error}</div>}
              {shouldShowTransferRootPlaylist && (
                <div className="text-sm text-red-500">
                  You have tracks in your root playlist that are not example
                  tracks. If you log in, your playlists will be transferred to
                  your logged account.
                </div>
              )}
              <Button
                onClick={() => handlePasskeySubmit(false)}
                className="w-full bg-blue-600 hover:bg-blue-700"
              >
                Login with passkey
              </Button>
              <Button onClick={handleBack} variant="ghost" className="w-full">
                <ArrowLeft className="size-4 mr-2" />
                Back
              </Button>
            </div>
          </>
        );

      case "passphrase-signup":
        return (
          <>
            <DialogHeader>
              <DialogTitle className="text-2xl font-bold">
                Your recovery passphrase
              </DialogTitle>
              <DialogDescription>
                Please copy and store this passphrase somewhere safe. You'll
                need it to log in on other devices.
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-4 pt-4">
              {error && <div className="text-sm text-red-500">{error}</div>}
              <Textarea
                readOnly
                value={generatedPassphrase}
                className="font-mono text-sm bg-gray-50"
                rows={4}
              />
              <div className="flex gap-2">
                <Button
                  onClick={handleCopy}
                  variant="outline"
                  className="flex-1"
                >
                  {isCopied ? (
                    <>
                      <Check className="size-4 mr-2" />
                      Copied!
                    </>
                  ) : (
                    <>
                      <Copy className="size-4 mr-2" />
                      Copy
                    </>
                  )}
                </Button>
                <Button
                  onClick={handleReroll}
                  variant="outline"
                  className="flex-1"
                >
                  Generate new
                </Button>
              </div>
              <Button
                onClick={handlePassphraseSignUp}
                className="w-full bg-blue-600 hover:bg-blue-700"
                disabled={!isCopied}
              >
                {isCopied ? "Create account" : "Copy passphrase first"}
              </Button>
              <Button onClick={handleBack} variant="ghost" className="w-full">
                <ArrowLeft className="size-4 mr-2" />
                Back
              </Button>
            </div>
          </>
        );

      case "passphrase-login":
        return (
          <>
            <DialogHeader>
              <DialogTitle className="text-2xl font-bold">
                Login with passphrase
              </DialogTitle>
              <DialogDescription>
                Enter your recovery passphrase to log in
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-4 pt-4">
              {error && <div className="text-sm text-red-500">{error}</div>}
              {shouldShowTransferRootPlaylist && (
                <div className="text-sm text-red-500">
                  You have tracks in your root playlist that are not example
                  tracks. If you log in, your playlists will be transferred to
                  your logged account.
                </div>
              )}
              <Textarea
                value={loginPassphrase}
                onChange={(e) => setLoginPassphrase(e.target.value)}
                placeholder="Enter your passphrase..."
                className="font-mono text-sm"
                rows={4}
              />
              <Button
                onClick={handlePassphraseLogin}
                className="w-full bg-blue-600 hover:bg-blue-700"
                disabled={!loginPassphrase.trim()}
              >
                Login
              </Button>
              <Button onClick={handleBack} variant="ghost" className="w-full">
                <ArrowLeft className="size-4 mr-2" />
                Back
              </Button>
            </div>
          </>
        );
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[425px]">
        {renderContent()}
      </DialogContent>
    </Dialog>
  );
}
