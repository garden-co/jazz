import { MusicaAccount, MusicTrack, Playlist } from "@/1_schema";
import { usePlayState } from "@/lib/audio/usePlayState";
import { usePlayMedia } from "@/lib/audio/usePlayMedia";
import { useState } from "react";
import { updateActivePlaylist, updateActiveTrack } from "./4_actions";
import { useAudioManager } from "./lib/audio/AudioManager";
import { getNextTrack, getPrevTrack } from "./lib/getters";
import { useSuspenseAccount } from "jazz-tools/react-core";

export function useMediaPlayer() {
  const audioManager = useAudioManager();
  const playState = usePlayState();
  const { source, playMedia } = usePlayMedia();

  const [loading, setLoading] = useState<string | null>(null);

  const activeTrackId = useSuspenseAccount(MusicaAccount, {
    select: (me) => me.root.activeTrack?.$jazz.id,
  });

  async function loadTrack(track: MusicTrack, autoPlay = true) {
    audioManager.unloadCurrentAudio();

    setLoading(track.$jazz.id);
    updateActiveTrack(track);

    try {
      await playMedia(track, autoPlay);
    } catch (error) {
      console.error("Failed to load track:", error);
    }

    setLoading(null);
  }

  async function playNextTrack() {
    const track = await getNextTrack();

    if (track.$isLoaded) {
      updateActiveTrack(track);
      await loadTrack(track);
    }
  }

  async function playPrevTrack() {
    const track = await getPrevTrack();

    if (track.$isLoaded) {
      await loadTrack(track);
    }
  }

  async function setActiveTrack(track: MusicTrack, playlist?: Playlist) {
    if (activeTrackId === track.$jazz.id) {
      playState.toggle();
      return;
    }

    updateActivePlaylist(playlist);

    await loadTrack(track);

    if (playState.value === "pause") {
      playState.toggle();
    }
  }

  return {
    activeTrackId,
    setActiveTrack,
    playNextTrack,
    playPrevTrack,
    loading,
    loadTrack,
    source,
  };
}

export type MediaPlayer = ReturnType<typeof useMediaPlayer>;
