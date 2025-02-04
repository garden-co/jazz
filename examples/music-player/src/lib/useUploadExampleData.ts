import { MusicaAccount } from "@/1_schema";
import { useEffect } from "react";
import { uploadMusicTracks } from "../4_actions";

export function useUploadExampleData() {
  useEffect(() => {
    uploadOnboardingData();
  }, []);
}

async function uploadOnboardingData() {
  const me = await MusicaAccount.getMe().ensureLoaded({
    resolve: { root: true },
  });

  if (!me) throw new Error("Me not resolved");

  if (me.root.exampleDataLoaded) return;

  me.root.exampleDataLoaded = true;

  try {
    const trackFile = await (await fetch("/example.mp3")).blob();

    await uploadMusicTracks([new File([trackFile], "Example song")]);
  } catch (error) {
    me.root.exampleDataLoaded = false;
    throw error;
  }
}
