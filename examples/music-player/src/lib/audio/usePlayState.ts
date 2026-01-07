import { useLayoutEffect, useState } from "react";
import { useAudioManager } from "./AudioManager";

export type PlayState = "pause" | "play";

export function usePlayState() {
  const audioManager = useAudioManager();
  const [value, setValue] = useState<PlayState>("pause");

  useLayoutEffect(() => {
    // Initialize state from AudioManager
    setValue(audioManager.isPlaying() ? "play" : "pause");

    const onPlay = () => setValue("play");
    const onPause = () => setValue("pause");
    const onEnded = () => setValue("pause");

    const unsubPlay = audioManager.on("play", onPlay);
    const unsubPause = audioManager.on("pause", onPause);
    const unsubEnded = audioManager.on("ended", onEnded);

    return () => {
      unsubPlay();
      unsubPause();
      unsubEnded();
    };
  }, [audioManager]);

  function togglePlayState() {
    if (value === "pause") {
      audioManager.play();
    } else {
      audioManager.pause();
    }
  }

  return { value, toggle: togglePlayState };
}
