import { MusicaAccount } from "@/1_schema.ts";
import { createAccountSubscriptionContext } from "jazz-tools/react-core";

export const { Provider: AccountProvider, useSelector: useAccountSelector } =
  createAccountSubscriptionContext(MusicaAccount, {
    root: {
      rootPlaylist: {
        tracks: {
          $each: true,
        },
      },
      playlists: true,
      activeTrack: true,
      activePlaylist: true,
    },
    profile: true,
  });
