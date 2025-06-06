import { Group, co, z } from "jazz-tools";
import { Camera, Cursor } from "./types";

export const CursorFeed = co.feed(Cursor);

export const CursorProfile = co.profile({
  name: z.string(),
});

export const CursorRoot = co.map({
  camera: Camera,
  cursors: CursorFeed,
});

export const CursorContainer = co.map({
  cursorFeed: CursorFeed,
});

export const CursorAccount = co
  .account({
    profile: CursorProfile,
    root: CursorRoot,
  })
  .withMigration((account) => {
    if (account.root === undefined) {
      account.root = CursorRoot.create({
        camera: {
          position: {
            x: 0,
            y: 0,
          },
        },
        cursors: CursorFeed.create([]),
      });
    }

    if (account.profile === undefined) {
      const group = Group.create();
      group.makePublic(); // The profile info is visible to everyone

      account.profile = CursorProfile.create(
        {
          name: "Anonymous user",
        },
        group,
      );
    }
  });
