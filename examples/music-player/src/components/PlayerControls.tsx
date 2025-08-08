import { useAccount, useCoState } from "jazz-tools/react";
import { Pause, Play, SkipBack, SkipForward } from "lucide-react";
import { MusicaAccount, MusicTrack } from "@/1_schema";
import { MediaPlayer } from "@/5_useMediaPlayer";
import { useMediaEndListener } from "@/lib/audio/useMediaEndListener";
import { usePlayState } from "@/lib/audio/usePlayState";
import { useKeyboardListener } from "@/lib/useKeyboardListener";
import { Waveform } from "./Waveform";

export function PlayerControls({ mediaPlayer }: { mediaPlayer: MediaPlayer }) {
  const playState = usePlayState();
  const isPlaying = playState.value === "play";

  const activePlaylist = useAccount(MusicaAccount, {
    resolve: { root: { activePlaylist: true } },
  }).me?.root.activePlaylist;

  const activeTrack = useCoState(MusicTrack, mediaPlayer.activeTrackId, {
    resolve: { waveform: true },
  });

  if (!activeTrack) return null;

  const activeTrackTitle = activeTrack.title;

  return (
    <footer className="flex items-center justify-between p-2 sm:p-4 gap-2 sm:gap-4 bg-white border-t border-gray-200 absolute bottom-0 left-0 right-0 w-full z-50">
      <div className="flex justify-center items-center space-x-1 sm:space-x-2 flex-shrink-0">
        <div className="flex items-center space-x-2 sm:space-x-4">
          <button
            onClick={mediaPlayer.playPrevTrack}
            className="text-blue-600 hover:text-blue-800"
            aria-label="Previous track"
          >
            <SkipBack size={16} className="sm:w-5 sm:h-5" />
          </button>
          <button
            onClick={playState.toggle}
            className="w-8 h-8 sm:w-[42px] sm:h-[42px] flex items-center justify-center bg-blue-600 rounded-full text-white hover:bg-blue-700"
            aria-label={isPlaying ? "Pause active track" : "Play active track"}
          >
            {isPlaying ? (
              <Pause size={16} className="sm:w-6 sm:h-6" fill="currentColor" />
            ) : (
              <Play size={16} className="sm:w-6 sm:h-6" fill="currentColor" />
            )}
          </button>
          <button
            onClick={mediaPlayer.playNextTrack}
            className="text-blue-600 hover:text-blue-800"
            aria-label="Next track"
          >
            <SkipForward size={16} className="sm:w-5 sm:h-5" />
          </button>
        </div>
      </div>
      <div className="md:hidden sm:hidden  lg:flex flex-1 justify-center items-center min-w-0 px-2">
        <Waveform
          track={activeTrack}
          height={30}
          className="h-5 sm:h-6 md:h-8 lg:h-10"
        />
      </div>
      <div className="flex flex-col items-end gap-1 text-right min-w-fit flex-shrink-0">
        <h4 className="font-medium text-blue-800 text-sm sm:text-base truncate max-w-32 sm:max-w-80">
          {activeTrackTitle}
        </h4>
        <p className="text-xs sm:text-sm text-gray-600 truncate max-w-32 sm:max-w-80">
          {activePlaylist?.title || "All tracks"}
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
