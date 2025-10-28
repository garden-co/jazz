import { MusicTrack, MusicaAccount, Playlist } from "@/1_schema";
import {
  addTrackToPlaylist,
  removeTrackFromAllPlaylists,
  removeTrackFromPlaylist,
} from "@/4_actions";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { cn } from "@/lib/utils";
import { Loaded } from "jazz-tools";
import { useAccountWithSelector, useCoState } from "jazz-tools/react";
import { MoreHorizontal, Pause, Play } from "lucide-react";
import { Fragment, useCallback, useState } from "react";
import { EditTrackDialog } from "./RenameTrackDialog";
import { Waveform } from "./Waveform";
import { Button } from "./ui/button";
import { useAccountSelector } from "@/components/AccountProvider.tsx";

function isPartOfThePlaylist(
  trackId: string,
  playlist: Loaded<typeof Playlist, { tracks: true }>,
) {
  return Array.from(playlist.tracks.$jazz.refs).some((t) => t.id === trackId);
}

export function MusicTrackRow({
  trackId,
  isPlaying,
  onClick,
  index,
}: {
  trackId: string;
  isPlaying: boolean;
  onClick: (track: MusicTrack) => void;
  index: number;
}) {
  const track = useCoState(MusicTrack, trackId);
  const [isEditDialogOpen, setIsEditDialogOpen] = useState(false);
  const [isDropdownOpen, setIsDropdownOpen] = useState(false);
  const [isHovered, setIsHovered] = useState(false);

  const playlists = useAccountWithSelector(MusicaAccount, {
    resolve: {
      root: { playlists: { $onError: null, $each: { tracks: true } } },
    },
    select: (account) => account?.root.playlists,
  });

  const isActiveTrack = useAccountSelector({
    select: (me) => me.root.activeTrack?.$jazz.id === trackId,
  });

  const canEditTrack = useAccountSelector({
    select: (me) => Boolean(track && me.canWrite(track)),
  });

  function handleTrackClick() {
    if (!track) return;
    onClick(track);
  }

  function handleAddToPlaylist(playlist: Playlist) {
    if (!track) return;
    addTrackToPlaylist(playlist, track);
  }

  function handleRemoveFromPlaylist(playlist: Playlist) {
    if (!track) return;
    removeTrackFromPlaylist(playlist, track);
  }

  function deleteTrack() {
    if (!track) return;

    removeTrackFromAllPlaylists(track);
  }

  function handleEdit() {
    setIsEditDialogOpen(true);
  }

  const handleContextMenu = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setIsDropdownOpen(true);
  }, []);

  const showWaveform = isHovered || isActiveTrack;

  return (
    <li
      className={cn(
        "flex gap-1 hover:bg-slate-200 group py-2 cursor-pointer rounded-lg",
        isActiveTrack && "bg-slate-200",
      )}
      onMouseEnter={() => {
        setIsHovered(true);
      }}
      onMouseLeave={() => {
        setIsHovered(false);
      }}
    >
      <button
        className={cn(
          "flex items-center justify-center bg-transparent w-8 h-8 transition-opacity cursor-pointer",
          // Show play button on hover or when active, hide otherwise
          "md:opacity-0 opacity-50 group-hover:opacity-100",
          isActiveTrack && "md:opacity-100 opacity-100",
        )}
        onClick={handleTrackClick}
        aria-label={`${isPlaying ? "Pause" : "Play"} ${track?.title}`}
      >
        {isPlaying ? (
          <Pause height={16} width={16} fill="currentColor" />
        ) : (
          <Play height={16} width={16} fill="currentColor" />
        )}
      </button>
      {/* Show track index when play button is hidden - hidden on mobile */}
      <div
        className={cn(
          "hidden md:flex items-center justify-center w-8 h-8 text-sm text-gray-500 font-mono transition-opacity",
        )}
      >
        {index + 1}
      </div>
      <button
        onContextMenu={handleContextMenu}
        onClick={handleTrackClick}
        className="flex items-center overflow-hidden text-ellipsis whitespace-nowrap cursor-pointer flex-1 min-w-0"
      >
        {track?.title}
      </button>

      {/* Waveform that appears on hover */}
      {track && showWaveform && (
        <div className="flex-1 min-w-0 px-2 items-center hidden md:flex">
          <Waveform
            track={track}
            height={20}
            className="opacity-70 w-full"
            showProgress={isActiveTrack}
          />
        </div>
      )}

      {canEditTrack && (
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
              {playlists?.filter(Boolean).map((playlist, playlistIndex) => (
                <Fragment key={playlistIndex}>
                  {isPartOfThePlaylist(trackId, playlist) ? (
                    <DropdownMenuItem
                      key={`remove-${playlistIndex}`}
                      onSelect={() => handleRemoveFromPlaylist(playlist)}
                    >
                      Remove from {playlist.title}
                    </DropdownMenuItem>
                  ) : (
                    <DropdownMenuItem
                      key={`add-${playlistIndex}`}
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
      )}
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
