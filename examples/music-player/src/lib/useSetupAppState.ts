import { MusicaAccount } from "@/1_schema";
import { MediaPlayer } from "@/5_useMediaPlayer";
import { useAgent } from "jazz-tools/react";
import { useEffect, useState } from "react";
import { createMusicTrackFromFile, updateActiveTrack } from "../4_actions";

export function useSetupAppState(mediaPlayer: MediaPlayer) {
  const [isReady, setIsReady] = useState(false);

  // We want this effect to run every time the account changes
  const agent = useAgent();

  useEffect(() => {
    setupAppState(mediaPlayer).then(() => {
      setIsReady(true);
    });
  }, [agent]);

  return isReady;
}

async function setupAppState(mediaPlayer: MediaPlayer) {
  const { root } = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
      root: {
        activeTrack: { $onError: "catch" },
      },
    },
  });

  if (root.activeTrack?.$isLoaded) {
    // Load the active track in the AudioManager
    mediaPlayer.loadTrack(root.activeTrack, false);
    return;
  }

  const { rootPlaylist } = await root.$jazz.ensureLoaded({
    resolve: {
      rootPlaylist: {
        tracks: true,
      },
    },
  });

  if (root.exampleDataLoaded) {
    return;
  }

  // We first set the exampleDataLoaded to true to avoid race conditions
  root.$jazz.set("exampleDataLoaded", true);

  try {
    const trackFile = await (await fetch("/example.mp3")).blob();

    const track = await createMusicTrackFromFile(
      new File([trackFile], "Example song"),
      true,
    );
    rootPlaylist.tracks.$jazz.push(track);

    updateActiveTrack(track);
    mediaPlayer.loadTrack(track, false);
  } catch (error) {
    // If the track fails to load, we set the exampleDataLoaded to false to retry on the next load
    root.$jazz.set("exampleDataLoaded", false);
    throw error;
  }
}
