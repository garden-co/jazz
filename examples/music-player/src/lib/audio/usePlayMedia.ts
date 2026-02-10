import { useRef } from "react";
import { useAudioManager } from "./AudioManager";

export function usePlayMedia() {
  const audioManager = useAudioManager();

  const previousMediaLoad = useRef<Promise<unknown> | undefined>(undefined);

  async function playMedia(file: Blob, autoPlay = true) {
    // Wait for the previous load to finish
    // to avoid to incur into concurrency issues
    await previousMediaLoad.current;

    const promise = audioManager.load(file);

    previousMediaLoad.current = promise;

    await promise;

    if (autoPlay) {
      audioManager.play();
    }
  }

  return playMedia;
}
