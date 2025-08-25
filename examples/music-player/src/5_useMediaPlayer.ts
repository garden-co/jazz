import { MusicTrack, MusicaAccount, Playlist } from "@/1_schema";
import { usePlayMedia } from "@/lib/audio/usePlayMedia";
import { usePlayState } from "@/lib/audio/usePlayState";
import { useAccount } from "jazz-tools/react";
import { useRef, useState } from "react";
import { updateActivePlaylist, updateActiveTrack } from "./4_actions";
import { useAudioManager } from "./lib/audio/AudioManager";
import { getNextTrack, getPrevTrack } from "./lib/getters";

export function useMediaPlayer() {
  const { me } = useAccount(MusicaAccount, {
    resolve: { root: true },
  });

  const audioManager = useAudioManager();
  const playState = usePlayState();
  const playMedia = usePlayMedia();

  const [loading, setLoading] = useState<string | null>(null);

  const activeTrackId = me?.root.$jazz.refs.activeTrack?.id;

  // Reference used to avoid out-of-order track loads
  const lastLoadedTrackId = useRef<string | null>(null);

  async function loadTrack(track: MusicTrack, autoPlay = true) {
    lastLoadedTrackId.current = track.$jazz.id;
    audioManager.unloadCurrentAudio();

    setLoading(track.$jazz.id);
    updateActiveTrack(track);

    const file = await MusicTrack.shape.file.loadAsBlob(
      track.$jazz.refs.file!.id,
    ); // TODO: see if we can avoid !

    if (!file) {
      setLoading(null);
      return;
    }

    // Check if another track has been loaded during
    // the file download
    if (lastLoadedTrackId.current !== track.$jazz.id) {
      return;
    }

    await playMedia(file, autoPlay);

    setLoading(null);
  }

  async function playNextTrack() {
    const track = await getNextTrack();

    if (track) {
      updateActiveTrack(track);
      await loadTrack(track);
    }
  }

  async function playPrevTrack() {
    const track = await getPrevTrack();

    if (track) {
      await loadTrack(track);
    }
  }

  async function setActiveTrack(track: MusicTrack, playlist?: Playlist) {
    if (
      activeTrackId === track.$jazz.id &&
      lastLoadedTrackId.current !== null
    ) {
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
  };
}

export type MediaPlayer = ReturnType<typeof useMediaPlayer>;
