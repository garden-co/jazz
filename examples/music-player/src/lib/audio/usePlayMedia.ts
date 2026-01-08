import { useState, useRef } from "react";
import { useAudioManager } from "./AudioManager";
import { MusicTrack } from "@/1_schema";
import { FileStreamSource } from "./FileStreamSource";

export function usePlayMedia() {
  const audioManager = useAudioManager();
  const [source, setSource] = useState<FileStreamSource | null>(null);
  const currentSourceRef = useRef<FileStreamSource | null>(null);

  async function playMedia(track: MusicTrack, autoPlay: boolean) {
    const newSource = new FileStreamSource(track.$jazz.refs.file.id);

    // Dispose previous source and track current
    currentSourceRef.current?._dispose();
    currentSourceRef.current = newSource;
    setSource(newSource);

    await newSource.waitForReady();

    // Bail if a newer source was requested
    if (currentSourceRef.current !== newSource) {
      newSource._dispose();
      return;
    }

    await audioManager.loadAudio(newSource);

    if (autoPlay && currentSourceRef.current === newSource) {
      audioManager.play();
    }
  }

  return { playMedia, source };
}
