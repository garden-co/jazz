import { Suspense, useState } from "react";
import { useDb } from "jazz-tools/react";
import { LogOutIcon } from "lucide-react";
import { Avatar } from "@/components/Avatar";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet";
import { useMyProfile } from "@/hooks/useMyProfile";
import { logOut } from "@/lib/utils";
import { app } from "../../../schema.js";

interface ProfileProps {
  onClose: () => void;
  children: React.ReactNode;
}

export function Profile({ onClose, children }: ProfileProps) {
  const [open, setOpen] = useState(false);

  return (
    <Sheet
      onOpenChange={(v) => {
        setOpen(v);
        if (!v) onClose?.();
      }}
      open={open}
    >
      <SheetTrigger asChild>{children}</SheetTrigger>
      <SheetContent className="overflow-y-auto">
        <Suspense
          fallback={
            <div className="p-8 text-center text-muted-foreground italic">Loading account...</div>
          }
        >
          <ProfileContent setOpen={setOpen} />
        </Suspense>
      </SheetContent>
    </Sheet>
  );
}

function ProfileContent({ setOpen }: { setOpen: (v: boolean) => void }) {
  const db = useDb();

  const myProfile = useMyProfile();

  const handleNameChange = (newName: string) => {
    if (myProfile) {
      db.update(app.profiles, myProfile.id, { name: newName });
    }
  };

  const handleAvatarChange = async (evt: React.ChangeEvent<HTMLInputElement>) => {
    if (!myProfile || !evt.target.files?.[0]) return;
    const file = evt.target.files[0];
    const reader = new FileReader();
    reader.onload = () => {
      const dataUrl = reader.result as string;
      db.update(app.profiles, myProfile.id, { avatar: dataUrl });
    };
    reader.readAsDataURL(file);
  };

  const handleAvatarRemove = () => {
    if (!myProfile) return;
    // TODO remove cast once https://github.com/garden-co/jazz2/pull/349 is merged
    db.update(app.profiles, myProfile.id, { avatar: null as unknown as string });
  };

  return (
    <>
      <SheetHeader>
        <SheetTitle>Your Account</SheetTitle>
      </SheetHeader>

      <SheetDescription className="sr-only">
        Manage your account settings and preferences.
      </SheetDescription>
      <div className="px-4 space-y-4">
        <div className="space-y-2">
          <Label htmlFor="avatar">Avatar</Label>
          <p className="text-xs text-muted-foreground">Upload a profile picture.</p>
          <div className="flex items-center gap-3">
            <label className="cursor-pointer transition-opacity hover:opacity-80">
              <Avatar profileId={myProfile?.id ?? ""} avatarData={myProfile?.avatar} size={64} />
              <input
                type="file"
                className="hidden"
                id="avatar"
                accept="image/*"
                // Expose handler on DOM for browser tests (Radix portal blocks synthetic events)
                ref={(el) => {
                  if (el)
                    (el as unknown as Record<string, unknown>).__handleAvatarChange =
                      handleAvatarChange;
                }}
                onChange={handleAvatarChange}
              />
            </label>
            {myProfile?.avatar && (
              <Button variant="outline" onClick={handleAvatarRemove}>
                Remove
              </Button>
            )}
          </div>
        </div>

        <div className="space-y-2">
          <Label htmlFor="name">Name</Label>
          <p className="text-xs text-muted-foreground">The name you would like to be known by.</p>
          <Input
            type="text"
            id="name"
            value={myProfile?.name ?? ""}
            onChange={(evt) => handleNameChange(evt.currentTarget.value)}
          />
        </div>
      </div>
      <SheetFooter>
        <div className="space-y-2">
          <Label htmlFor="logout">Log Out</Label>
          <p className="text-xs text-muted-foreground">
            If you log out, you will be automatically provisioned with a new local-first identity.
          </p>
          <Button
            id="logout"
            variant="destructive"
            onClick={() => {
              logOut();
              setOpen(false);
            }}
          >
            <LogOutIcon /> Log Out
          </Button>
        </div>
      </SheetFooter>
    </>
  );
}
