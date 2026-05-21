import { Suspense, useState } from "react";
import { reatomComponent } from "@reatom/react";
import { wrap } from "@reatom/core";
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
import { jazz } from "@/jazz";
import { myProfile } from "@/model/my-profile";
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

const ProfileContent = reatomComponent(({ setOpen }: { setOpen: (v: boolean) => void }) => {
  const { db } = jazz();
  const profile = myProfile();

  const handleNameChange = (newName: string) => {
    if (profile) {
      db.update(app.profiles, profile.id, { name: newName });
    }
  };

  const handleAvatarChange = async (evt: React.ChangeEvent<HTMLInputElement>) => {
    if (!profile || !evt.target.files?.[0]) return;
    const file = evt.target.files[0];
    const reader = new FileReader();
    reader.onload = wrap(() => {
      const dataUrl = reader.result as string;
      db.update(app.profiles, profile.id, { avatar: dataUrl });
    });
    reader.readAsDataURL(file);
  };

  const handleAvatarRemove = () => {
    if (!profile) return;
    // TODO remove cast once https://github.com/garden-co/jazz2/pull/349 is merged
    db.update(app.profiles, profile.id, { avatar: null as unknown as string });
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
              <Avatar
                profileId={profile?.id ?? ""}
                avatarData={profile?.avatar ?? undefined}
                size={64}
              />
              <input
                type="file"
                className="hidden"
                id="avatar"
                accept="image/*"
                ref={(el) => {
                  if (el)
                    (el as unknown as Record<string, unknown>).__handleAvatarChange =
                      handleAvatarChange;
                }}
                onChange={handleAvatarChange}
              />
            </label>
            {profile?.avatar && (
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
            value={profile?.name ?? ""}
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
}, "ProfileContent");
