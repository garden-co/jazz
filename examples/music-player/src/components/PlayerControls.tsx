import { MusicaAccount, MusicTrack } from "@/1_schema";
import { MediaPlayer } from "@/5_useMediaPlayer";
import { useMediaEndListener } from "@/lib/audio/useMediaEndListener";
import { usePlayState } from "@/lib/audio/usePlayState";
import { useKeyboardListener } from "@/lib/useKeyboardListener";
import { useCoState, useSuspenseAccount } from "jazz-tools/react";
import { Loader2, Pause, Play, SkipBack, SkipForward } from "lucide-react";
import WaveformCanvas from "./WaveformCanvas";
import { Button } from "./ui/button";
import { useSyncExternalStore } from "react";

const noopSubscribe = () => () => {};

export function PlayerControls({ mediaPlayer }: { mediaPlayer: MediaPlayer }) {
  const playState = usePlayState();
  const isPlaying = playState.value === "play";
  const readyToPlay = useSyncExternalStore(
    mediaPlayer.source?.subscribeToStreamingState ?? noopSubscribe,
    () => mediaPlayer.source?.getStreamingState().readyToPlay,
  );

  const activePlaylistTitle = useSuspenseAccount(MusicaAccount, {
    select: (me) =>
      me.root.activePlaylist?.$isLoaded
        ? (me.root.activePlaylist.title ?? "All tracks")
        : "All tracks",
  });

  const activeTrack = useCoState(MusicTrack, mediaPlayer.activeTrackId);

  if (!activeTrack.$isLoaded) return null;

  const activeTrackTitle = activeTrack.title;

  return (
    <footer className="flex flex-nowrap items-center justify-between p-4 gap-4 bg-white border-t border-gray-200 absolute bottom-0 left-0 right-0 w-full z-50">
      <div className="flex justify-center items-center space-x-1 sm:space-x-2 flex-shrink-0 w-auto order-none">
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
            disabled={!readyToPlay}
          >
            {!readyToPlay ? (
              <Loader2 className="h-5 w-5 animate-spin" />
            ) : isPlaying ? (
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

      <WaveformCanvas
        className="order-1 sm:order-none hidden sm:block"
        source={mediaPlayer.source}
        track={activeTrack}
        height={50}
      />

      <div className="flex flex-col gap-1 min-w-fit sm:flex-shrink-0 text-center w-full sm:text-right items-end sm:w-auto order-0 sm:order-none">
        <h4 className="font-medium text-blue-800 text-base sm:text-base truncate max-w-80 sm:max-w-80">
          {activeTrackTitle}
        </h4>
        <div className="flex items-center gap-2">
          <p className="text-xs sm:text-sm text-gray-600 truncate sm:max-w-80">
            {activePlaylistTitle || "All tracks"}
          </p>
        </div>
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
