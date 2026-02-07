import type { co } from "jazz-tools";
import { useState } from "react";
import {
  createInviteLink,
  useSuspenseAccount,
  useSuspenseCoState,
} from "jazz-tools/react";
import {
  BrushIcon,
  CloudUploadIcon,
  ImageIcon,
  PlusIcon,
  Share2Icon,
} from "lucide-react";
import { ShareModal } from "@/components/composer/ShareModal";
import { UploadModal } from "@/components/composer/UploadModal";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  type Attachment,
  Canvas,
  CanvasAttachment,
  Chat,
  ChatAccount,
} from "@/schema";

type UploadIntent = "image" | "file" | null;

interface ActionMenuProps {
  onAddAttachment: (attachment: co.loaded<typeof Attachment>) => void;
  chatId: string;
}

export function ActionMenu({ onAddAttachment, chatId }: ActionMenuProps) {
  const chat = useSuspenseCoState(Chat, chatId);
  const me = useSuspenseAccount(ChatAccount);

  const [intent, setIntent] = useState<UploadIntent>(null);
  const [willShare, setWillShare] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);
  const [inviteLink, setInviteLink] = useState("");

  // Check if the user can share the chat (either as an admin, or because the chat is public)
  const canShare = me.canAdmin(chat) || chat.$jazz.owner.getRoleOf("everyone");

  return (
    <>
      <DropdownMenu open={menuOpen} onOpenChange={setMenuOpen}>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" size="icon-lg" className="rounded-full">
            <PlusIcon />
          </Button>
        </DropdownMenuTrigger>

        <DropdownMenuContent>
          <DropdownMenuItem onSelect={() => setIntent("image")}>
            <ImageIcon /> Image
          </DropdownMenuItem>

          <DropdownMenuItem onSelect={() => setIntent("file")}>
            <CloudUploadIcon /> File
          </DropdownMenuItem>

          <DropdownMenuItem
            onSelect={() => {
              const canvas = Canvas.create({}, { owner: chat.$jazz.owner });
              const attachment = CanvasAttachment.create(
                {
                  type: "canvas",
                  name: "New Canvas",
                  canvas: canvas,
                },
                { owner: chat.$jazz.owner },
              );
              onAddAttachment(attachment);
            }}
          >
            <BrushIcon /> Canvas
          </DropdownMenuItem>

          {canShare && (
            <DropdownMenuItem
              onSelect={() => {
                if (!inviteLink) {
                  if (me.canAdmin(chat)) {
                    setInviteLink(
                      createInviteLink(chat, "writer", {
                        baseURL: `${window.location.origin}/`,
                      }),
                    );
                  } else {
                    // Because this is a public chat, we can use the link directly
                    setInviteLink(window.location.href);
                  }
                }
                setWillShare(true);
              }}
            >
              <Share2Icon /> Invite to chat
            </DropdownMenuItem>
          )}
        </DropdownMenuContent>
      </DropdownMenu>

      <UploadModal
        owner={chat.$jazz.owner}
        open={!!intent}
        onOpenChange={(isOpen) => !isOpen && setIntent(null)}
        title={intent === "image" ? "Upload image" : "Upload file"}
        accept={intent === "image" ? "image/*" : undefined}
        onUpload={(file) => {
          onAddAttachment(file);
          setIntent(null);
        }}
      />

      {canShare && (
        <ShareModal
          chatId={chat.$jazz.id}
          open={willShare}
          inviteLink={inviteLink}
          onOpenChange={setWillShare}
        />
      )}
    </>
  );
}
