import { MusicTrack } from "@/1_schema";
import { MediaPlayer } from "@/5_useMediaPlayer";
import { useMediaEndListener } from "@/lib/audio/useMediaEndListener";
import { usePlayState } from "@/lib/audio/usePlayState";
import { useKeyboardListener } from "@/lib/useKeyboardListener";
import { useCoState } from "jazz-tools/react";
import { Pause, Play, SkipBack, SkipForward } from "lucide-react";
import WaveformCanvas from "./WaveformCanvas";
import { Button } from "./ui/button";
import { useAccountSelector } from "@/components/AccountProvider.tsx";

export function PlayerControls({ mediaPlayer }: { mediaPlayer: MediaPlayer }) {
  const playState = usePlayState();
  const isPlaying = playState.value === "play";

  const activePlaylistTitle = useAccountSelector({
    select: (me) =>
      me.$isLoaded && me.root.activePlaylist?.$isLoaded
        ? (me.root.activePlaylist.title ?? "All tracks")
        : "All tracks",
  });

  const activeTrack = useCoState(MusicTrack, mediaPlayer.activeTrackId);

  if (!activeTrack.$isLoaded) return null;

  const activeTrackTitle = activeTrack.title;

  return (
    <footer className="flex flex-wrap sm:flex-nowrap items-center justify-between pt-4 p-2 sm:p-4 gap-4 sm:gap-4 bg-white border-t border-gray-200 absolute bottom-0 left-0 right-0 w-full z-50">
      {/* Player Controls - Always on top */}
      <div className="flex justify-center items-center space-x-1 sm:space-x-2 flex-shrink-0 w-full sm:w-auto order-1 sm:order-none">
        <div className="flex items-center space-x-2">
          <Button
            variant="ghost"
            size="icon"
            onClick={mediaPlayer.playPrevTrack}
            aria-label="Previous track"
          >
            <SkipBack className="h-5 w-5" fill="currentColor" />
          </Button>
          <Button
            size="icon"
            onClick={playState.toggle}
            className="bg-blue-600 text-white hover:bg-blue-700"
            aria-label={isPlaying ? "Pause active track" : "Play active track"}
          >
            {isPlaying ? (
              <Pause className="h-5 w-5" fill="currentColor" />
            ) : (
              <Play className="h-5 w-5" fill="currentColor" />
            )}
          </Button>
          <Button
            variant="ghost"
            size="icon"
            onClick={mediaPlayer.playNextTrack}
            aria-label="Next track"
          >
            <SkipForward className="h-5 w-5" fill="currentColor" />
          </Button>
        </div>
      </div>

      {/* Waveform - Below controls on mobile, between controls and info on desktop */}
      <WaveformCanvas
        className="order-1 sm:order-none"
        track={activeTrack}
        height={50}
      />

      {/* Track Info - Below waveform on mobile, on the right on desktop */}
      <div className="flex flex-col gap-1 min-w-fit sm:flex-shrink-0 text-center w-full sm:text-right items-center sm:items-end sm:w-auto order-0 sm:order-none">
        <h4 className="font-medium text-blue-800 text-base sm:text-base truncate max-w-80 sm:max-w-80">
          {activeTrackTitle}
        </h4>
        <p className="hidden sm:block text-xs sm:text-sm text-gray-600 truncate sm:max-w-80">
          {activePlaylistTitle || "All tracks"}
        </p>
      </div>
    </footer>
  );
}

export function KeyboardListener({
  mediaPlayer,
}: {
  mediaPlayer: MediaPlayer;
}) {
  const playState = usePlayState();

  useMediaEndListener(mediaPlayer.playNextTrack);
  useKeyboardListener("Space", () => {
    if (document.activeElement !== document.body) return;

    playState.toggle();
  });

  return null;
}
