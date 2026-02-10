import { MusicaAccount, MusicTrack, Playlist } from "@/1_schema";
import { usePlayMedia } from "@/lib/audio/usePlayMedia";
import { useEffect, useRef, useState } from "react";
import { updateActivePlaylist, updateActiveTrack } from "./4_actions";
import { useAudioManager } from "./lib/audio/AudioManager";
import {
  getNextTrack,
  getPrevTrack,
  getActivePlaylistTitle,
} from "./lib/getters";
import { useSuspenseAccount } from "jazz-tools/react-core";

// Cache for prefetched audio files
const prefetchCache = new Map<string, Blob>();
const prefetchingInProgress = new Set<string>();

async function prefetchTrackAudio(track: MusicTrack) {
  const trackId = track.$jazz.id;

  // Skip if already cached or prefetching
  if (prefetchCache.has(trackId) || prefetchingInProgress.has(trackId)) {
    return;
  }

  prefetchingInProgress.add(trackId);

  try {
    const file = await MusicTrack.shape.file.loadAsBlob(
      track.$jazz.refs.file.id,
    );
    if (file) {
      prefetchCache.set(trackId, file);
    }
  } finally {
    prefetchingInProgress.delete(trackId);
  }
}

export function useMediaPlayer() {
  const audioManager = useAudioManager();
  const playMedia = usePlayMedia();

  const [loading, setLoading] = useState<string | null>(null);

  const activeTrackId = useSuspenseAccount(MusicaAccount, {
    select: (me) => me.root.activeTrack?.$jazz.id,
  });
  // Reference used to avoid out-of-order track loads
  const lastLoadedTrackId = useRef<string | null>(null);

  // Store refs for the handlers so they can access current state
  const playNextTrackRef = useRef<() => Promise<void>>(() => Promise.resolve());
  const playPrevTrackRef = useRef<() => Promise<void>>(() => Promise.resolve());

  async function loadTrack(track: MusicTrack, autoPlay = true) {
    const trackId = track.$jazz.id;
    lastLoadedTrackId.current = trackId;
    audioManager.unload();

    setLoading(trackId);
    updateActiveTrack(track);

    // Check prefetch cache first
    let file = prefetchCache.get(trackId);
    if (file) {
      prefetchCache.delete(trackId); // Use once, then remove from cache
    } else {
      file = await MusicTrack.shape.file.loadAsBlob(track.$jazz.refs.file.id);
    }

    if (!file) {
      setLoading(null);
      return;
    }

    // Check if another track has been loaded during
    // the file download
    if (lastLoadedTrackId.current !== trackId) {
      return;
    }

    await playMedia(file, autoPlay);

    // Set metadata for MediaSession API (browser media controls)
    const playlistTitle = await getActivePlaylistTitle();
    audioManager.setMetadata({
      title: track.title,
      artist: playlistTitle,
      duration: track.duration,
    });

    setLoading(null);

    // Prefetch the next track in the background
    getNextTrack().then((nextTrack) => {
      if (nextTrack?.$isLoaded) {
        prefetchTrackAudio(nextTrack);
      }
    });
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

  // Keep refs updated
  playNextTrackRef.current = playNextTrack;
  playPrevTrackRef.current = playPrevTrack;

  // Register handlers with AudioManager and enable keyboard shortcuts
  useEffect(() => {
    audioManager.setNextTrackHandler(() => playNextTrackRef.current?.());
    audioManager.setPreviousTrackHandler(() => playPrevTrackRef.current?.());
    audioManager.enableKeyboardShortcuts();

    return () => {
      audioManager.setNextTrackHandler(null);
      audioManager.setPreviousTrackHandler(null);
      audioManager.disableKeyboardShortcuts();
    };
  }, [audioManager]);

  async function setActiveTrack(track: MusicTrack, playlist?: Playlist) {
    if (
      activeTrackId === track.$jazz.id &&
      lastLoadedTrackId.current !== null
    ) {
      audioManager.togglePlayPause();
      return;
    }

    updateActivePlaylist(playlist);

    await loadTrack(track);

    if (audioManager.isPaused) {
      audioManager.play();
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
