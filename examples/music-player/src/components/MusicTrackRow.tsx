import { Loaded } from "jazz-tools";
import { useAccount, useCoState } from "jazz-tools/react";
import { MoreHorizontal } from "lucide-react";
import { Fragment, useCallback, useState } from "react";
import { MusicaAccount, MusicTrack, Playlist } from "@/1_schema";
import { addTrackToPlaylist, removeTrackFromPlaylist } from "@/4_actions";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { cn } from "@/lib/utils";
import { EditTrackDialog } from "./RenameTrackDialog";
import { Button } from "./ui/button";

function isPartOfThePlaylist(
  trackId: string,
  playlist: Loaded<typeof Playlist, { tracks: true }>,
) {
  return Array.from(playlist.tracks._refs).some((t) => t.id === trackId);
}

export function MusicTrackRow({
  trackId,
  isLoading,
  isPlaying,
  onClick,
}: {
  trackId: string;
  isLoading: boolean;
  isPlaying: boolean;
  onClick: (track: Loaded<typeof MusicTrack>) => void;
}) {
  const track = useCoState(MusicTrack, trackId);
  const [isEditDialogOpen, setIsEditDialogOpen] = useState(false);
  const [isDropdownOpen, setIsDropdownOpen] = useState(false);

  const { me } = useAccount(MusicaAccount, {
    resolve: { root: { playlists: { $each: { tracks: true } } } },
  });

  const playlists = me?.root.playlists ?? [];

  function handleTrackClick() {
    if (!track) return;
    onClick(track);
  }

  function handleAddToPlaylist(playlist: Loaded<typeof Playlist>) {
    if (!track) return;
    addTrackToPlaylist(playlist, track);
  }

  function handleRemoveFromPlaylist(playlist: Loaded<typeof Playlist>) {
    if (!track) return;
    removeTrackFromPlaylist(playlist, track);
  }

  function deleteTrack() {
    if (!me || !track) return;
    const tracks = me.root.rootPlaylist?.tracks;
    if (!tracks) return;
    const index = tracks.findIndex((t) => t?.id === trackId);
    if (index !== -1) {
      tracks.splice(index, 1);
    }
  }

  function handleEdit() {
    setIsEditDialogOpen(true);
  }

  const handleContextMenu = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setIsDropdownOpen(true);
  }, []);

  return (
    <li
      className={"flex gap-1 hover:bg-slate-200 group py-2 px-2 cursor-pointer"}
    >
      <button
        className={cn(
          "flex items-center justify-center bg-transparent w-8 h-8 ",
          !isPlaying && "group-hover:bg-slate-300 rounded-full",
        )}
        onClick={handleTrackClick}
        aria-label={`${isPlaying ? "Pause" : "Play"} ${track?.title}`}
      >
        {isLoading ? (
          <div className="animate-spin">߷</div>
        ) : isPlaying ? (
          "⏸️"
        ) : (
          "▶️"
        )}
      </button>
      <button
        onContextMenu={handleContextMenu}
        onClick={handleTrackClick}
        className="w-full flex items-center overflow-hidden text-ellipsis whitespace-nowrap"
      >
        {track?.title}
      </button>
      <div onClick={(evt) => evt.stopPropagation()}>
        <DropdownMenu open={isDropdownOpen} onOpenChange={setIsDropdownOpen}>
          <DropdownMenuTrigger asChild>
            <Button
              variant="ghost"
              className="h-8 w-8 p-0"
              aria-label={`Open ${track?.title} menu`}
            >
              <span className="sr-only">Open menu</span>
              <MoreHorizontal className="h-4 w-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem onSelect={handleEdit}>Edit</DropdownMenuItem>
            {playlists.map((playlist, index) => (
              <Fragment key={index}>
                {isPartOfThePlaylist(trackId, playlist) ? (
                  <DropdownMenuItem
                    key={`remove-${index}`}
                    onSelect={() => handleRemoveFromPlaylist(playlist)}
                  >
                    Remove from {playlist.title}
                  </DropdownMenuItem>
                ) : (
                  <DropdownMenuItem
                    key={`add-${index}`}
                    onSelect={() => handleAddToPlaylist(playlist)}
                  >
                    Add to {playlist.title}
                  </DropdownMenuItem>
                )}
              </Fragment>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
      {track && isEditDialogOpen && (
        <EditTrackDialog
          track={track}
          isOpen={isEditDialogOpen}
          onOpenChange={setIsEditDialogOpen}
          onDelete={deleteTrack}
        />
      )}
    </li>
  );
}
