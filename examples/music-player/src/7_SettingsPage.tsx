import type { MediaPlayer } from "@/5_useMediaPlayer";
import { PlayerControls } from "@/components/PlayerControls";
import { DeleteAccountDialog } from "@/components/DeleteAccountDialog";
import { ProfileForm } from "@/components/ProfileForm";
import { SidePanel } from "@/components/SidePanel";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { Textarea } from "@/components/ui/textarea";
import { SidebarInset, SidebarTrigger } from "@/components/ui/sidebar";
import { toast } from "@/hooks/use-toast";
import { wordlist } from "@/wordlist";
import { useLogOut, usePassphraseAuth } from "jazz-tools/react";
import { Copy, Check, ShieldAlert } from "lucide-react";
import { useState } from "react";
import { deleteMyMusicPlayerAccount } from "./4_actions";

export function SettingsPage({ mediaPlayer }: { mediaPlayer: MediaPlayer }) {
  const [isCopied, setIsCopied] = useState(false);
  const [isDeleteOpen, setIsDeleteOpen] = useState(false);
  const logOut = useLogOut();

  const passphraseAuth = usePassphraseAuth({
    wordlist,
  });

  const handleCopyPassphrase = async () => {
    if (passphraseAuth.passphrase) {
      await navigator.clipboard.writeText(passphraseAuth.passphrase);
      setIsCopied(true);
      toast({
        title: "Copied",
        description: "Passphrase copied to clipboard.",
      });
      setTimeout(() => setIsCopied(false), 2000);
    }
  };

  return (
    <SidebarInset className="flex flex-col h-screen text-gray-800">
      <div className="flex flex-1 overflow-hidden">
        <SidePanel />
        <main className="flex-1 px-2 py-4 md:px-6 overflow-y-auto overflow-x-hidden relative sm:h-[calc(100vh-80px)] bg-white h-[calc(100vh-165px)]">
          <SidebarTrigger className="md:hidden" />

          <div className="pl-1 md:pl-10 pr-2 md:pr-0 mt-2 md:mt-0 w-full">
            <h1 className="text-2xl font-bold text-blue-800">
              Profile settings
            </h1>
            <p className="text-gray-600 mt-2">
              Update your profile information and manage your account.
            </p>

            <Separator className="my-6" />

            <div className="max-w-2xl space-y-8">
              <section className="space-y-3">
                <h2 className="text-lg font-semibold text-gray-900">Profile</h2>
                <p className="text-sm text-gray-600">
                  Update your profile name and avatar.
                </p>

                <ProfileForm
                  className="max-w-md"
                  submitButtonText="Save"
                  onSubmit={() => {
                    toast({
                      title: "Saved",
                      description: "Your profile has been updated.",
                    });
                  }}
                />
              </section>

              <Separator />

              <section className="space-y-3">
                <h2 className="text-lg font-semibold text-gray-900">
                  Recovery passphrase
                </h2>
                <p className="text-sm text-gray-600">
                  Use this passphrase to log in on other devices or recover your
                  account.
                </p>

                <div className="bg-amber-50 border border-amber-200 rounded-lg p-4 flex items-start gap-3">
                  <ShieldAlert className="size-5 text-amber-600 mt-0.5 shrink-0" />
                  <div className="text-sm text-amber-800">
                    <p className="font-medium">Keep this passphrase secret</p>
                    <p className="mt-1">
                      Anyone with this passphrase can access your account. Store
                      it somewhere safe and never share it.
                    </p>
                  </div>
                </div>

                <div className="space-y-3 max-w-md">
                  <Textarea
                    readOnly
                    value={passphraseAuth.passphrase || "Loading..."}
                    className="font-mono text-sm bg-gray-50"
                    rows={4}
                  />
                  <Button
                    onClick={handleCopyPassphrase}
                    variant="outline"
                    disabled={!passphraseAuth.passphrase}
                  >
                    {isCopied ? (
                      <>
                        <Check className="size-4 mr-2" />
                        Copied!
                      </>
                    ) : (
                      <>
                        <Copy className="size-4 mr-2" />
                        Copy passphrase
                      </>
                    )}
                  </Button>
                </div>
              </section>
              <section className="space-y-3">
                <h2 className="text-lg font-semibold text-gray-900">
                  Danger zone
                </h2>
                <p className="text-sm text-gray-600">
                  Deleting your account will remove your music-player data.
                </p>
                <Button
                  variant="destructive"
                  onClick={() => setIsDeleteOpen(true)}
                >
                  Delete account
                </Button>
              </section>
            </div>
          </div>
        </main>

        <PlayerControls mediaPlayer={mediaPlayer} />
      </div>
      {isDeleteOpen && (
        <DeleteAccountDialog
          isOpen={isDeleteOpen}
          onOpenChange={setIsDeleteOpen}
          onConfirm={async () => {
            await deleteMyMusicPlayerAccount();
            logOut();
            window.location.href = "/";
          }}
        />
      )}
    </SidebarInset>
  );
}
