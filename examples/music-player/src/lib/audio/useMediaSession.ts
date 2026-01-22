import { useEffect } from "react";
import { useAudioManager } from "./AudioManager";
import { usePlayState } from "./usePlayState";

interface UseMediaSessionOptions {
  trackTitle?: string;
  playlistTitle?: string;
  onPrevTrack?: () => void;
  onNextTrack?: () => void;
}

export function useMediaSession(options: UseMediaSessionOptions) {
  const audioManager = useAudioManager();
  const playState = usePlayState();

  // Update metadata when track changes
  useEffect(() => {
    if (!options.trackTitle) return;

    navigator.mediaSession.metadata = new MediaMetadata({
      title: options.trackTitle,
      artist: options.playlistTitle || "All tracks",
    });
  }, [options.trackTitle, options.playlistTitle]);

  // Set up action handlers
  useEffect(() => {
    navigator.mediaSession.setActionHandler("play", () => audioManager.play());
    navigator.mediaSession.setActionHandler("pause", () =>
      audioManager.pause(),
    );
    navigator.mediaSession.setActionHandler(
      "previoustrack",
      options.onPrevTrack ?? null,
    );
    navigator.mediaSession.setActionHandler(
      "nexttrack",
      options.onNextTrack ?? null,
    );

    return () => {
      navigator.mediaSession.setActionHandler("play", null);
      navigator.mediaSession.setActionHandler("pause", null);
      navigator.mediaSession.setActionHandler("previoustrack", null);
      navigator.mediaSession.setActionHandler("nexttrack", null);
    };
  }, [audioManager, options.onPrevTrack, options.onNextTrack]);

  // Sync playback state
  useEffect(() => {
    navigator.mediaSession.playbackState =
      playState.value === "play" ? "playing" : "paused";
  }, [playState.value]);

  // Sync position state (time progression)
  useEffect(() => {
    const updatePositionState = () => {
      const audioDuration = audioManager.mediaElement.duration;
      const audioCurrentTime = audioManager.mediaElement.currentTime;

      if (audioDuration > 0) {
        navigator.mediaSession.setPositionState({
          duration: audioDuration,
          playbackRate: 1,
          position: audioCurrentTime,
        });
      }
    };

    // Update position on timeupdate events
    audioManager.mediaElement.addEventListener(
      "timeupdate",
      updatePositionState,
    );

    return () => {
      audioManager.mediaElement.removeEventListener(
        "timeupdate",
        updatePositionState,
      );
    };
  }, [audioManager]);
}
