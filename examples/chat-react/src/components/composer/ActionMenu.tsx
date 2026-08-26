import { useDb, useSession } from "jazz-tools/react";
import { BrushIcon, PlusIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { useMyProfile } from "@/hooks/useMyProfile";
import { waitForWrite } from "@/lib/db-write";
import { app } from "../../../schema.js";
import { DurabilityTier } from "jazz-tools";

interface ActionMenuProps {
  chatId: string;
  disabled?: boolean;
}

export function ActionMenu({ chatId, disabled = false }: ActionMenuProps) {
  const db = useDb();
  const session = useSession();
  const userId = session?.user;
  const sharedWriteOptions: { tier: DurabilityTier } = {
    tier: db.getConfig().serverUrl ? "edge" : "local",
  };
  const myProfile = useMyProfile();

  const handleCreateCanvas = () => {
    if (!userId || !myProfile) return;
    void (async () => {
      const canvas = await waitForWrite(
        db.insert(app.canvases, {
          chatId,
        }),
        sharedWriteOptions,
      );
      await waitForWrite(
        db.insert(app.messages, {
          chatId,
          text: `[Canvas: ${canvas.id}]`,
          senderId: myProfile.id,
        }),
        sharedWriteOptions,
      );
    })().catch((error) => {
      console.error("failed to create canvas", error);
    });
  };

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" size="icon-lg" className="rounded-full" disabled={disabled}>
            <PlusIcon />
          </Button>
        </DropdownMenuTrigger>

        <DropdownMenuContent>
          <DropdownMenuItem onSelect={handleCreateCanvas}>
            <BrushIcon /> Canvas
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </>
  );
}
