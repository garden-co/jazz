import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useAccount, usePasskeyAuth } from "jazz-react";
import { Loader2 } from "lucide-react";
import { useState } from "react";

interface AuthModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function AuthModal({ open, onOpenChange }: AuthModalProps) {
  const [username, setUsername] = useState("");
  const [isSignUp, setIsSignUp] = useState(true);

  const { me } = useAccount();

  const [, authState] = usePasskeyAuth({
    appName: "Jazz Music Player",
    onAnonymousUserUpgrade: ({ username, isSignUp }) => {
      if (isSignUp) {
        me.profile!.name = username;
      }

      onOpenChange(false);
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (authState.state === "ready") {
      if (isSignUp) {
        authState.signUp(username);
      } else {
        authState.logIn();
      }
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[425px]">
        <DialogHeader>
          <DialogTitle className="text-2xl font-bold">
            {isSignUp ? "Create account" : "Welcome back"}
          </DialogTitle>
          {authState.state === "ready" && (
            <DialogDescription>
              {isSignUp
                ? "Sign up to enable network sync and share your playlists with others"
                : "Changes done before logging in will be lost"}
            </DialogDescription>
          )}
        </DialogHeader>
        <form onSubmit={handleSubmit} className="space-y-4">
          {isSignUp && (
            <div className="space-y-2">
              <Label htmlFor="username">Username</Label>
              <Input
                id="username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="Enter your username"
                required
              />
            </div>
          )}
          {authState.errors.length > 0 && (
            <div className="text-sm text-red-500">
              {authState.errors.map((error, i) => (
                <p key={i}>{error}</p>
              ))}
            </div>
          )}
          <div className="space-y-4">
            <Button
              type="submit"
              className="w-full bg-blue-600 hover:bg-blue-700"
              disabled={authState.state === "loading"}
            >
              {authState.state === "loading" ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : isSignUp ? (
                "Sign up with passkey"
              ) : (
                "Login with passkey"
              )}
            </Button>
            <div className="text-center text-sm">
              {isSignUp ? "Already have an account?" : "Don't have an account?"}{" "}
              <button
                type="button"
                onClick={() => setIsSignUp(!isSignUp)}
                className="text-blue-600 hover:underline"
              >
                {isSignUp ? "Login" : "Sign up"}
              </button>
            </div>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}
