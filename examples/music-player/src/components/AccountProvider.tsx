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
      playlists: {
        $each: {
          $onError: null,
        },
      },
      activeTrack: { $onError: null },
      activePlaylist: { $onError: null },
    },
    profile: true,
  });
