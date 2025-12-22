import type { MediaPlayer } from "@/5_useMediaPlayer";
import { MusicaAccount } from "@/1_schema";
import { PlayerControls } from "@/components/PlayerControls";
import { DeleteAccountDialog } from "@/components/DeleteAccountDialog";
import { ProfileForm } from "@/components/ProfileForm";
import { SidePanel } from "@/components/SidePanel";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { SidebarInset, SidebarTrigger } from "@/components/ui/sidebar";
import { toast } from "@/hooks/use-toast";
import { deleteMyMusicPlayerAccount } from "@/4_actions";
import { useLogOut } from "jazz-tools/react";
import { useState } from "react";
export function SettingsPage({ mediaPlayer }: { mediaPlayer: MediaPlayer }) {
  const [isDeleteOpen, setIsDeleteOpen] = useState(false);
  const logOut = useLogOut();

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
