import { useAccount } from "jazz-tools/react";
import { useEffect } from "react";
import { MusicaAccount } from "@/1_schema";
import { uploadMusicTracks } from "../4_actions";

export function useUploadExampleData() {
  const { agent } = useAccount();

  useEffect(() => {
    uploadOnboardingData();
  }, [agent]);
}

async function uploadOnboardingData() {
  const me = await MusicaAccount.getMe().ensureLoaded({
    resolve: { root: true },
  });

  if (me.root.exampleDataLoaded) return;

  me.root.exampleDataLoaded = true;

  try {
    const trackFile = await (await fetch("/example.mp3")).blob();

    await uploadMusicTracks([new File([trackFile], "Example song")], true);
  } catch (error) {
    me.root.exampleDataLoaded = false;
    throw error;
  }
}
