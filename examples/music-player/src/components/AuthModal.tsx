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
import { useState } from "react";

interface AuthModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function AuthModal({ open, onOpenChange }: AuthModalProps) {
  const [username, setUsername] = useState("");
  const [isSignUp, setIsSignUp] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const { me } = useAccount({
    resolve: {
      root: {
        rootPlaylist: {
          tracks: {
            $each: true,
          },
        },
      },
    },
  });

  const auth = usePasskeyAuth({
    appName: "Jazz Music Player",
  });

  const handleViewChange = () => {
    setIsSignUp(!isSignUp);
    setError(null);
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    try {
      if (isSignUp) {
        await auth.signUp(username);
      } else {
        await auth.logIn();
      }
      onOpenChange(false);
    } catch (error) {
      setError(error instanceof Error ? error.message : "Unknown error");
    }
  };

  const shouldShowTransferRootPlaylist =
    !isSignUp &&
    me?.root.rootPlaylist.tracks.some((track) => !track.isExampleTrack);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[425px]">
        <DialogHeader>
          <DialogTitle className="text-2xl font-bold">
            {isSignUp ? "Create account" : "Welcome back"}
          </DialogTitle>
          <DialogDescription>
            {isSignUp
              ? "Sign up to enable network sync and share your playlists with others"
              : "Changes done before logging in will be lost"}
          </DialogDescription>
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
          {error && <div className="text-sm text-red-500">{error}</div>}
          {shouldShowTransferRootPlaylist && (
            <div className="text-sm text-red-500">
              You have tracks in your root playlist that are not example tracks.
              If you log in with a passkey, your playlists will be transferred
              to your logged account.
            </div>
          )}
          <div className="space-y-4">
            <Button
              type="submit"
              className="w-full bg-blue-600 hover:bg-blue-700"
            >
              {isSignUp ? "Sign up with passkey" : "Login with passkey"}
            </Button>
            <div className="text-center text-sm">
              {isSignUp ? "Already have an account?" : "Don't have an account?"}{" "}
              <button
                type="button"
                onClick={handleViewChange}
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
