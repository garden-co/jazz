import { useLayoutEffect, useState } from "react";
import { AudioManager, useAudioManager } from "./AudioManager";

export function usePlayerCurrentTime() {
  const audioManager = useAudioManager();
  const [value, setValue] = useState<number>(0);

  useLayoutEffect(() => {
    setValue(getPlayerCurrentTime(audioManager));

    return subscribeToPlayerCurrentTime(audioManager, setValue);
  }, [audioManager]);

  function setCurrentTime(time: number) {
    // Seek to the new time (and start playing if paused)
    if (!audioManager.isPlaying()) {
      audioManager.play();
    }
    audioManager.seek(time);
  }

  return {
    value,
    setValue: setCurrentTime,
  };
}

export function setPlayerCurrentTime(audioManager: AudioManager, time: number) {
  audioManager.seek(time);
}

export function getPlayerCurrentTime(audioManager: AudioManager): number {
  return audioManager.getCurrentTime();
}

export function subscribeToPlayerCurrentTime(
  audioManager: AudioManager,
  callback: (time: number) => void,
): () => void {
  return audioManager.on("timeupdate", (e) => callback(e.detail));
}
