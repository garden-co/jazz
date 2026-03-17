import { useState } from "react";
import { useDb, useSession, useAll } from "jazz-tools/react";
import { BrushIcon, CloudUploadIcon, ImageIcon, PlusIcon, Share2Icon } from "lucide-react";
import { ShareModal } from "@/components/composer/ShareModal";
import { UploadModal, type AttachmentData } from "@/components/composer/UploadModal";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { useMyProfile } from "@/hooks/useMyProfile";
import { app } from "../../../schema/app.js";

interface ActionMenuProps {
  chatId: string;
  onAttachment?: (attachment: AttachmentData) => Promise<void>;
}

export function ActionMenu({ chatId, onAttachment }: ActionMenuProps) {
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id;
  const [willShare, setWillShare] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);
  const [uploadMode, setUploadMode] = useState<"image" | "file" | null>(null);

  const chats = useAll(app.chats.where({ id: chatId })) ?? [];
  const chat = chats[0];

  const myMemberships =
    useAll(app.chatMembers.where({ chatId, userId: userId ?? "__none__" })) ?? [];
  const myJoinCode = myMemberships[0]?.joinCode ?? undefined;

  const myProfile = useMyProfile();

  const handleCreateCanvas = () => {
    if (!userId || !myProfile) return;
    const canvas = db.insert(app.canvases, {
      chatId,
      createdAt: new Date(),
    });
    db.insert(app.messages, {
      chatId,
      text: `[Canvas: ${canvas.id}]`,
      senderId: myProfile.id,
      createdAt: new Date(),
    });
  };

  return (
    <>
      <DropdownMenu open={menuOpen} onOpenChange={setMenuOpen}>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" size="icon-lg" className="rounded-full">
            <PlusIcon />
          </Button>
        </DropdownMenuTrigger>

        <DropdownMenuContent>
          <DropdownMenuItem onSelect={() => setUploadMode("image")}>
            <ImageIcon /> Image
          </DropdownMenuItem>

          <DropdownMenuItem onSelect={() => setUploadMode("file")}>
            <CloudUploadIcon /> File
          </DropdownMenuItem>

          <DropdownMenuItem onSelect={handleCreateCanvas}>
            <BrushIcon /> Canvas
          </DropdownMenuItem>

          {chat && chat.createdBy === userId && (
            <DropdownMenuItem onSelect={() => setWillShare(true)}>
              <Share2Icon /> Invite to chat
            </DropdownMenuItem>
          )}
        </DropdownMenuContent>
      </DropdownMenu>

      <UploadModal
        open={!!uploadMode}
        onOpenChange={(isOpen) => !isOpen && setUploadMode(null)}
        title={uploadMode === "image" ? "Upload image" : "Upload file"}
        accept={uploadMode === "image" ? "image/*" : undefined}
        onUpload={async (attachment) => {
          await onAttachment?.(attachment);
          setUploadMode(null);
        }}
      />

      {chat && (
        <ShareModal
          chatId={chatId}
          joinCode={myJoinCode}
          open={willShare}
          onOpenChange={setWillShare}
        />
      )}
    </>
  );
}
