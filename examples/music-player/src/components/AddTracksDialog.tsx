import { MusicaAccount, PlaylistWithTracks } from "@/1_schema";
import { addTrackToPlaylist } from "@/4_actions";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { useSuspenseAccount, useSuspenseCoState } from "jazz-tools/react-core";
import { useState, useMemo } from "react";

interface AddTracksDialogProps {
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  playlist: PlaylistWithTracks;
}

export function AddTracksDialog({
  isOpen,
  onOpenChange,
  playlist,
}: AddTracksDialogProps) {
  const rootPlaylistId = useSuspenseAccount(MusicaAccount, {
    select: (me) => me.root.$jazz.refs.rootPlaylist.id,
  });

  const rootPlaylistTracks = useSuspenseCoState(
    PlaylistWithTracks,
    rootPlaylistId,
    {
      select: (rootPlaylist) => rootPlaylist.tracks,
    },
  );

  // Filter tracks that are not already in the current playlist
  const availableTracks = useMemo(() => {
    const currentPlaylistTrackIds = new Set(
      playlist.tracks.map((track) => track.$jazz.id),
    );

    return rootPlaylistTracks.filter(
      (track) => !currentPlaylistTrackIds.has(track.$jazz.id),
    );
  }, [rootPlaylistTracks, playlist.tracks]);

  const [selectedTrackIds, setSelectedTrackIds] = useState(new Set<string>());
  const [isAdding, setIsAdding] = useState(false);

  function handleTrackToggle(trackId: string) {
    setSelectedTrackIds((prev) => {
      const next = new Set(prev);
      if (next.has(trackId)) {
        next.delete(trackId);
      } else {
        next.add(trackId);
      }
      return next;
    });
  }

  function handleSelectAll() {
    if (selectedTrackIds.size === availableTracks.length) {
      setSelectedTrackIds(new Set());
    } else {
      setSelectedTrackIds(
        new Set(availableTracks.map((track) => track.$jazz.id)),
      );
    }
  }

  async function handleAddTracks() {
    if (selectedTrackIds.size === 0) return;

    setIsAdding(true);
    try {
      for (const trackId of selectedTrackIds) {
        const track = availableTracks.find((t) => t.$jazz.id === trackId);
        if (track) {
          await addTrackToPlaylist(playlist, track);
        }
      }
      setSelectedTrackIds(new Set());
      onOpenChange(false);
    } catch (error) {
      console.error("Failed to add tracks:", error);
    } finally {
      setIsAdding(false);
    }
  }

  function handleCancel() {
    setSelectedTrackIds(new Set());
    onOpenChange(false);
  }

  function handleOpenChange(open: boolean) {
    setSelectedTrackIds(new Set());
    onOpenChange(open);
  }

  return (
    <Dialog open={isOpen} onOpenChange={handleOpenChange}>
      <DialogContent className="max-w-2xl max-h-[80vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>Add Tracks from Library</DialogTitle>
          <DialogDescription>
            Select tracks from your library to add to this playlist.
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto min-h-0">
          {availableTracks.length === 0 ? (
            <div className="text-center py-8 text-gray-500">
              All tracks are already in this playlist.
            </div>
          ) : (
            <>
              <div className="mb-4 flex items-center justify-between">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={handleSelectAll}
                  className="text-sm"
                >
                  {selectedTrackIds.size === availableTracks.length
                    ? "Deselect All"
                    : "Select All"}
                </Button>
                <span className="text-sm text-gray-600">
                  {selectedTrackIds.size} of {availableTracks.length} selected
                </span>
              </div>
              <ul className="space-y-1">
                {availableTracks.map((track) => (
                  <li
                    key={track.$jazz.id}
                    className="flex items-center gap-3 p-2 rounded hover:bg-gray-100 cursor-pointer"
                    onClick={() => handleTrackToggle(track.$jazz.id)}
                  >
                    <input
                      type="checkbox"
                      checked={selectedTrackIds.has(track.$jazz.id)}
                      onChange={() => handleTrackToggle(track.$jazz.id)}
                      onClick={(e) => e.stopPropagation()}
                      className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                    />
                    <span className="flex-1 text-sm">{track.title}</span>
                  </li>
                ))}
              </ul>
            </>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={handleCancel} disabled={isAdding}>
            Cancel
          </Button>
          <Button
            onClick={handleAddTracks}
            disabled={selectedTrackIds.size === 0 || isAdding}
            className="bg-blue-600 hover:bg-blue-700"
          >
            {isAdding
              ? "Adding..."
              : `Add ${selectedTrackIds.size > 0 ? `${selectedTrackIds.size} ` : ""}Track${selectedTrackIds.size !== 1 ? "s" : ""}`}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
