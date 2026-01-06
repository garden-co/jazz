import { createNewPlaylist } from "@/4_actions";
import { useState } from "react";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "./ui/dialog";
import { MusicaAccountWithPlaylists } from "@/1_schema";
import { useSuspenseAccount } from "jazz-tools/react";

interface CreatePlaylistModalProps {
  isOpen: boolean;
  onClose: () => void;
  onPlaylistCreated: (playlistId: string) => void;
}

export function CreatePlaylistModal({
  isOpen,
  onClose,
  onPlaylistCreated,
}: CreatePlaylistModalProps) {
  const [playlistTitle, setPlaylistTitle] = useState("");
  const [isCreating, setIsCreating] = useState(false);

  function handleTitleChange(evt: React.ChangeEvent<HTMLInputElement>) {
    setPlaylistTitle(evt.target.value);
  }

  const me = useSuspenseAccount(MusicaAccountWithPlaylists, {
    equalityFn: (a, b) => a.$jazz.id === b.$jazz.id,
  });

  async function handleCreate() {
    if (!playlistTitle.trim()) return;

    setIsCreating(true);
    try {
      const playlist = await createNewPlaylist(me, playlistTitle.trim());
      setPlaylistTitle("");
      onPlaylistCreated(playlist.$jazz.id);
      onClose();
    } catch (error) {
      console.error("Failed to create playlist:", error);
    } finally {
      setIsCreating(false);
    }
  }

  function handleCancel() {
    setPlaylistTitle("");
    onClose();
  }

  function handleKeyDown(evt: React.KeyboardEvent) {
    if (evt.key === "Enter") {
      handleCreate();
    } else if (evt.key === "Escape") {
      handleCancel();
    }
  }

  function handleOpenChange(open: boolean) {
    if (!open) {
      handleCancel();
    }
  }

  return (
    <Dialog open={isOpen} onOpenChange={handleOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Create New Playlist</DialogTitle>
          <DialogDescription>Give your new playlist a name</DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <div>
            <Label
              htmlFor="playlist-title"
              className="text-sm font-medium text-gray-700"
            >
              Playlist Title
            </Label>
            <Input
              id="playlist-title"
              value={playlistTitle}
              onChange={handleTitleChange}
              onKeyDown={handleKeyDown}
              placeholder="Enter playlist title"
              className="mt-1"
              autoFocus
            />
          </div>
        </div>

        <DialogFooter>
          <Button
            variant="outline"
            onClick={handleCancel}
            disabled={isCreating}
          >
            Cancel
          </Button>
          <Button
            onClick={handleCreate}
            disabled={!playlistTitle.trim() || isCreating}
            className="bg-blue-600 hover:bg-blue-700 disabled:opacity-50"
          >
            {isCreating ? "Creating..." : "Create Playlist"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
