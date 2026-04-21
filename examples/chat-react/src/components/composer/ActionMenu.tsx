import { useState } from "react";
import { useDb, useSession } from "jazz-tools/react";
import { BrushIcon, CloudUploadIcon, ImageIcon, PlusIcon } from "lucide-react";
import { UploadModal, type AttachmentData } from "@/components/composer/UploadModal";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { useMyProfile } from "@/hooks/useMyProfile";
import { app } from "../../../schema.js";
import { DurabilityTier } from "jazz-tools";

interface ActionMenuProps {
  chatId: string;
  onAttachment?: (attachment: AttachmentData) => Promise<void>;
  disabled?: boolean;
}

export function ActionMenu({ chatId, onAttachment, disabled = false }: ActionMenuProps) {
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id;
  const sharedWriteOptions: { tier: DurabilityTier } = {
    tier: db.getConfig().serverUrl ? "edge" : "local",
  };
  const [menuOpen, setMenuOpen] = useState(false);
  const [uploadMode, setUploadMode] = useState<"image" | "file" | null>(null);

  const myProfile = useMyProfile();

  const handleCreateCanvas = () => {
    if (!userId || !myProfile) return;
    void (async () => {
      const canvas = await db
        .insert(app.canvases, {
          chatId,
          createdAt: new Date(),
        })
        .wait(sharedWriteOptions);
      await db
        .insert(app.messages, {
          chatId,
          text: `[Canvas: ${canvas.id}]`,
          senderId: myProfile.id,
          createdAt: new Date(),
        })
        .wait(sharedWriteOptions);
    })().catch((error) => {
      console.error("failed to create canvas", error);
    });
  };

  return (
    <>
      <DropdownMenu open={menuOpen} onOpenChange={setMenuOpen}>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" size="icon-lg" className="rounded-full" disabled={disabled}>
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
    </>
  );
}
