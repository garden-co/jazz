import { MusicTrack, Playlist } from "@/1_schema";
import { usePlayMedia } from "@/lib/audio/usePlayMedia";
import { usePlayState } from "@/lib/audio/usePlayState";
import { useAccount } from "jazz-react";
import { FileStream, ID } from "jazz-tools";
import { useRef, useState } from "react";
import { updateActivePlaylist, updateActiveTrack } from "./4_actions";
import { getNextTrack, getPrevTrack } from "./lib/getters";

export function useMediaPlayer() {
  const { me } = useAccount({
    root: {},
  });

  const playState = usePlayState();
  const playMedia = usePlayMedia();

  const [loading, setLoading] = useState<ID<MusicTrack> | null>(null);

  const activeTrackId = me?.root._refs.activeTrack?.id;

  // Reference used to avoid out-of-order track loads
  const lastLoadedTrackId = useRef<ID<MusicTrack> | null>(null);

  async function loadTrack(track: MusicTrack) {
    lastLoadedTrackId.current = track.id;

    setLoading(track.id);

    const file = await FileStream.loadAsBlob(track._refs.file.id);

    if (!file) {
      setLoading(null);
      return;
    }

    // Check if another track has been loaded during
    // the file download
    if (lastLoadedTrackId.current !== track.id) {
      return;
    }

    updateActiveTrack(track);

    await playMedia(file);

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
    if (activeTrackId === track.id && lastLoadedTrackId.current !== null) {
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
  };
}

export type MediaPlayer = ReturnType<typeof useMediaPlayer>;
