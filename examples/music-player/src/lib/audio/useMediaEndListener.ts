import { useEffect } from "react";
import { useAudioManager } from "./AudioManager";

export function useMediaEndListener(callback: () => void) {
  const audioManager = useAudioManager();

  useEffect(() => {
    return audioManager.on("ended", callback);
  }, [audioManager, callback]);
}
