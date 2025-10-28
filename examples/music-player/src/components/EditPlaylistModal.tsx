import { Playlist } from "@/1_schema";
import { updatePlaylistTitle } from "@/4_actions";
import { useCoState } from "jazz-tools/react";
import { ChangeEvent, useState, useEffect } from "react";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Label } from "./ui/label";

interface EditPlaylistModalProps {
  playlistId: string | undefined;
  isOpen: boolean;
  onClose: () => void;
}

export function EditPlaylistModal({
  playlistId,
  isOpen,
  onClose,
}: EditPlaylistModalProps) {
  const playlist = useCoState(Playlist, playlistId);
  const [localPlaylistTitle, setLocalPlaylistTitle] = useState("");

  // Reset local title when modal opens or playlist changes
  useEffect(() => {
    if (isOpen && playlist) {
      setLocalPlaylistTitle(playlist.title ?? "");
    }
  }, [isOpen, playlist]);

  function handleTitleChange(evt: ChangeEvent<HTMLInputElement>) {
    setLocalPlaylistTitle(evt.target.value);
  }

  function handleSave() {
    if (playlist && localPlaylistTitle.trim()) {
      updatePlaylistTitle(playlist, localPlaylistTitle.trim());
      onClose();
    }
  }

  function handleCancel() {
    setLocalPlaylistTitle(playlist?.title ?? "");
    onClose();
  }

  function handleKeyDown(evt: React.KeyboardEvent) {
    if (evt.key === "Enter") {
      handleSave();
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
            Edit Playlist
          </h2>
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
              value={localPlaylistTitle}
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
            >
              Cancel
            </Button>
            <Button
              onClick={handleSave}
              disabled={!localPlaylistTitle.trim()}
              className="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:opacity-50"
            >
              Save Changes
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
