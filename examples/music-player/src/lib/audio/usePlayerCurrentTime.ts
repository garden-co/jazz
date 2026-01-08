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
    if (audioManager.mediaElement.paused) audioManager.play();

    // eslint-disable-next-line react-compiler/react-compiler
    audioManager.mediaElement.currentTime = time;
  }

  return {
    value,
    setValue: setCurrentTime,
  };
}

export function setPlayerCurrentTime(audioManager: AudioManager, time: number) {
  audioManager.mediaElement.currentTime = time;
}

export function getPlayerCurrentTime(audioManager: AudioManager): number {
  return audioManager.mediaElement.currentTime;
}

export function subscribeToPlayerCurrentTime(
  audioManager: AudioManager,
  callback: (time: number) => void,
) {
  const onTimeUpdate = () => {
    callback(audioManager.mediaElement.currentTime);
  };

  audioManager.mediaElement.addEventListener("timeupdate", onTimeUpdate);

  return () => {
    audioManager.mediaElement.removeEventListener("timeupdate", onTimeUpdate);
  };
}
