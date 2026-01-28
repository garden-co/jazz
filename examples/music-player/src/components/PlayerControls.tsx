import { MusicaAccount, MusicTrack } from "@/1_schema";
import { MediaPlayer } from "@/5_useMediaPlayer";
import { useAudioManager } from "@/lib/audio/AudioManager";
import { useCoState, useSuspenseAccount } from "jazz-tools/react";
import {
  ChevronUp,
  Loader2,
  Pause,
  Play,
  SkipBack,
  SkipForward,
} from "lucide-react";
import { useState, useSyncExternalStore } from "react";
import WaveformCanvas from "./WaveformCanvas";
import { Button } from "./ui/button";
import {
  Drawer,
  DrawerContent,
  DrawerDescription,
  DrawerTitle,
  DrawerTrigger,
} from "./ui/drawer";

export function PlayerControls({ mediaPlayer }: { mediaPlayer: MediaPlayer }) {
  const audioManager = useAudioManager();
  const isPlaying = useSyncExternalStore(
    (callback) => audioManager.on("statusChange", callback),
    () => audioManager.isPlaying,
  );

  const activePlaylistTitle = useSuspenseAccount(MusicaAccount, {
    select: (me) =>
      me.root.activePlaylist?.$isLoaded
        ? (me.root.activePlaylist.title ?? "All tracks")
        : "All tracks",
  });
  const activeTrack = useCoState(MusicTrack, mediaPlayer.activeTrackId);

  const [drawerOpen, setDrawerOpen] = useState(false);

  const isLoading = mediaPlayer.loading !== null;

  if (!activeTrack.$isLoaded) return null;

  const activeTrackTitle = activeTrack.title;

  return (
    <Drawer open={drawerOpen} onOpenChange={setDrawerOpen}>
      <footer className="flex flex-nowrap items-center justify-between p-4 pb-[max(1rem,env(safe-area-inset-bottom))] gap-4 bg-white border-t border-gray-200 fixed bottom-0 left-0 right-0 w-full z-50">
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
              onClick={audioManager.togglePlayPause}
              className="bg-blue-600 text-white hover:bg-blue-700"
              aria-label={
                isPlaying ? "Pause active track" : "Play active track"
              }
              disabled={isLoading}
            >
              {isLoading ? (
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
          track={activeTrack}
          height={50}
        />

        {/* Desktop: Static track info */}
        <div className="hidden sm:flex flex-col gap-1 min-w-fit flex-shrink-0 text-right items-end w-auto">
          <h4 className="font-medium text-blue-800 text-base truncate max-w-80">
            {activeTrackTitle}
          </h4>
          <div className="flex items-center gap-2">
            <p className="text-sm text-gray-600 truncate max-w-80">
              {activePlaylistTitle || "All tracks"}
            </p>
          </div>
        </div>

        {/* Mobile: Tappable track info that opens drawer */}
        <DrawerTrigger asChild>
          <button
            type="button"
            className="flex flex-row gap-1 sm:hidden text-center items-center cursor-pointer hover:bg-gray-50 rounded-lg p-2 -m-2 transition-colors"
            aria-label="Open player controls"
          >
            <h4 className="font-medium text-blue-800 text-base">
              {activeTrackTitle}
            </h4>
            <ChevronUp className="h-4 w-4 text-gray-400 grow" />
          </button>
        </DrawerTrigger>
      </footer>

      {/* Mobile drawer with waveform */}
      <DrawerContent className="pb-8">
        <DrawerTitle className="sr-only">Now Playing</DrawerTitle>
        <DrawerDescription className="sr-only">
          Player controls and waveform visualization
        </DrawerDescription>
        <div className="flex flex-col gap-6 p-6">
          {/* Track info */}
          <div className="text-center">
            <h3 className="font-semibold text-xl text-blue-800">
              {activeTrackTitle}
            </h3>
            <p className="text-sm text-gray-600 mt-1">
              {activePlaylistTitle || "All tracks"}
            </p>
          </div>

          {/* Waveform */}
          <div className="w-full">
            <WaveformCanvas track={activeTrack} height={80} />
          </div>

          {/* Large controls */}
          <div className="flex justify-center items-center gap-4">
            <Button
              variant="ghost"
              size="icon"
              onClick={mediaPlayer.playPrevTrack}
              aria-label="Previous track"
              className="h-14 w-14"
            >
              <SkipBack className="h-7 w-7" fill="currentColor" />
            </Button>
            <Button
              size="icon"
              onClick={audioManager.togglePlayPause}
              className="bg-blue-600 text-white hover:bg-blue-700 h-16 w-16"
              aria-label={
                isPlaying ? "Pause active track" : "Play active track"
              }
              disabled={isLoading}
            >
              {isLoading ? (
                <Loader2 className="h-8 w-8 animate-spin" />
              ) : isPlaying ? (
                <Pause className="h-8 w-8" fill="currentColor" />
              ) : (
                <Play className="h-8 w-8" fill="currentColor" />
              )}
            </Button>
            <Button
              variant="ghost"
              size="icon"
              onClick={mediaPlayer.playNextTrack}
              aria-label="Next track"
              className="h-14 w-14"
            >
              <SkipForward className="h-7 w-7" fill="currentColor" />
            </Button>
          </div>
        </div>
      </DrawerContent>
    </Drawer>
  );
}
