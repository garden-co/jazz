import { createNewPlaylist } from "@/4_actions";
import { useState } from "react";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";

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

  async function handleCreate() {
    if (!playlistTitle.trim()) return;

    setIsCreating(true);
    try {
      const playlist = await createNewPlaylist(playlistTitle.trim());
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

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-lg shadow-xl p-6 w-full max-w-md">
        <div className="mb-6">
          <h2 className="text-xl font-semibold text-gray-900 mb-2">
            Create New Playlist
          </h2>
          <p className="text-sm text-gray-600">Give your new playlist a name</p>
        </div>

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

          <div className="flex justify-end space-x-3 pt-4">
            <Button
              variant="outline"
              onClick={handleCancel}
              className="px-4 py-2"
              disabled={isCreating}
            >
              Cancel
            </Button>
            <Button
              onClick={handleCreate}
              disabled={!playlistTitle.trim() || isCreating}
              className="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:opacity-50"
            >
              {isCreating ? "Creating..." : "Create Playlist"}
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
