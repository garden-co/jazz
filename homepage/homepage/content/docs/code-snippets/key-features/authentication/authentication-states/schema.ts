import { co, z, Group } from "jazz-tools";

const MusicTrack = co.map({
  title: z.string(),
  duration: z.number(),
  isExampleTrack: z.boolean().optional(),
});
const Playlist = co.map({
  title: z.string(),
  tracks: co.list(MusicTrack),
});
const MusicaAccountRoot = co.map({
  rootPlaylist: Playlist,
});

export const MusicaAccount = co.account({
  root: MusicaAccountRoot,
  profile: co.profile(),
});
type MusicaAccount = co.loaded<typeof MusicaAccount>;

// #region OnAnonymousAccountDiscarded
export async function onAnonymousAccountDiscarded(
  anonymousAccount: MusicaAccount,
) {
  const { root: anonymousAccountRoot } =
    await anonymousAccount.$jazz.ensureLoaded({
      resolve: {
        root: {
          rootPlaylist: {
            tracks: {
              $each: true,
            },
          },
        },
      },
    });

  const me = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
      root: {
        rootPlaylist: {
          tracks: true,
        },
      },
    },
  });

  // @ts-expect-error https://github.com/microsoft/TypeScript/pull/62661 - this can be removed once the issue is fixed at the TS level.
  for (const track of anonymousAccountRoot.rootPlaylist.tracks) {
    if (track.isExampleTrack) continue;

    const trackGroup = track.$jazz.owner;
    trackGroup.addMember(me, "admin");

    me.root.rootPlaylist.tracks.$jazz.push(track);
  }
}
// #endregion
