import { Suspense, useState } from "react";
import { useDb, useAll, useSession } from "jazz-tools/react";
import { LogOutIcon, Share2Icon } from "lucide-react";
import { Avatar } from "@/components/Avatar";
import { ShareModal } from "@/components/composer/ShareModal";
import { Button } from "@/components/ui/button";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { navigate } from "@/hooks/useRouter";
import { app } from "../../../schema/app.js";

interface ChatSettingsProps {
  chatId: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function ChatSettings({ chatId, open, onOpenChange }: ChatSettingsProps) {
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="overflow-y-auto">
        <Suspense
          fallback={
            <div className="p-8 text-center text-muted-foreground italic">Loading settings...</div>
          }
        >
          <ChatSettingsContent chatId={chatId} onOpenChange={onOpenChange} />
        </Suspense>
      </SheetContent>
    </Sheet>
  );
}

function ChatSettingsContent({
  chatId,
  onOpenChange,
}: {
  chatId: string;
  onOpenChange: (open: boolean) => void;
}) {
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id ?? null;
  const [willShare, setWillShare] = useState(false);

  const chatRows = useAll(app.chats.where({ id: chatId })) ?? [];
  const chat = chatRows[0];

  const members = useAll(app.chatMembers.where({ chatId })) ?? [];
  const allProfiles = useAll(app.profiles) ?? [];

  const memberUserIds = new Set(members.map((m) => m.userId));
  const memberProfiles = allProfiles.filter((p) => memberUserIds.has(p.userId));

  const myMembership = members.find((m) => m.userId === userId);

  const handleNameChange = (newName: string) => {
    if (!chat) return;
    db.update(app.chats, chatId, {
      name: newName || (null as unknown as string),
    });
  };

  const handleLeave = () => {
    if (!myMembership) return;
    db.delete(app.chatMembers, myMembership.id);
    onOpenChange(false);
    navigate("/#/chats");
  };

  return (
    <>
      <SheetHeader>
        <SheetTitle>Chat settings</SheetTitle>
      </SheetHeader>

      <SheetDescription className="sr-only">
        Manage chat name, view members, and leave the chat.
      </SheetDescription>

      <div className="px-4 space-y-4">
        <div className="space-y-2">
          <Label htmlFor="chat-name">Chat name</Label>
          <p className="text-xs text-muted-foreground">
            Leave blank to show participant names instead.
          </p>
          <Input
            id="chat-name"
            value={chat?.name ?? ""}
            onChange={(evt) => handleNameChange(evt.currentTarget.value)}
          />
        </div>

        <Separator />

        <div className="space-y-2">
          <Label>Members ({memberProfiles.length})</Label>
          <div className="space-y-2">
            {memberProfiles.map((profile) => (
              <div key={profile.id} className="flex items-center gap-2">
                <Avatar profileId={profile.id} avatarData={profile.avatar} size={32} />
                <span>{profile.name}</span>
                {profile.userId === userId && (
                  <span className="text-xs text-muted-foreground">(you)</span>
                )}
              </div>
            ))}
          </div>
        </div>

        {chat && !chat.isPublic && chat.joinCode && (
          <>
            <Separator />
            <Button variant="outline" className="w-full" onClick={() => setWillShare(true)}>
              <Share2Icon /> Invite to chat
            </Button>
            <ShareModal
              chatId={chatId}
              joinCode={chat.joinCode}
              open={willShare}
              onOpenChange={setWillShare}
            />
          </>
        )}

        <Separator />

        <div className="space-y-2">
          <Label>Leave chat</Label>
          <p className="text-xs text-muted-foreground">
            You will no longer see this chat, but other members will still have access.
          </p>
          <AlertDialog>
            <AlertDialogTrigger asChild>
              <Button variant="destructive">
                <LogOutIcon /> Leave chat
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>Leave this chat?</AlertDialogTitle>
                <AlertDialogDescription>
                  You will be removed from the member list. You can rejoin later if you receive a
                  new invite.
                </AlertDialogDescription>
              </AlertDialogHeader>
              <AlertDialogFooter>
                <AlertDialogCancel>Cancel</AlertDialogCancel>
                <AlertDialogAction variant="destructive" onClick={handleLeave}>
                  Leave
                </AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
        </div>
      </div>
    </>
  );
}
