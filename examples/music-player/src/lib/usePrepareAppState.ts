import { MusicaAccount, MusicaAccountRoot } from "@/1_schema";
import { MediaPlayer } from "@/5_useMediaPlayer";
import { co } from "jazz-tools";
import { useAccount } from "jazz-tools/react";
import { useEffect, useState } from "react";
import { uploadMusicTracks } from "../4_actions";

export function usePrepareAppState(mediaPlayer: MediaPlayer) {
  const [isReady, setIsReady] = useState(false);

  const { agent } = useAccount();

  useEffect(() => {
    loadInitialData(mediaPlayer).then(() => {
      setIsReady(true);
    });
  }, [agent]);

  return isReady;
}

async function loadInitialData(mediaPlayer: MediaPlayer) {
  const me = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
      root: {
        rootPlaylist: { tracks: { $each: true } },
        activeTrack: { $onError: null },
        activePlaylist: { $onError: null },
      },
    },
  });

  uploadOnboardingData(me.root);

  // Load the active track in the AudioManager
  if (me.root.activeTrack) {
    mediaPlayer.loadTrack(me.root.activeTrack, false);
  }
}

async function uploadOnboardingData(root: co.loaded<typeof MusicaAccountRoot>) {
  if (root.exampleDataLoaded) return;

  root.$jazz.set("exampleDataLoaded", true);

  try {
    const trackFile = await (await fetch("/example.mp3")).blob();

    await uploadMusicTracks([new File([trackFile], "Example song")], true);
  } catch (error) {
    root.$jazz.set("exampleDataLoaded", false);
    throw error;
  }
}
