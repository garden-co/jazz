import { co, z, setDefaultValidationMode } from "jazz-tools";
import { Camera, Cursor } from "./types";

setDefaultValidationMode("strict");

export const CursorFeed = co.feed(Cursor);
export type CursorFeed = co.loaded<typeof CursorFeed>;

export const CursorProfile = co
  .profile({
    name: z.string(),
  })
  .withPermissions({
    // The profile info is visible to everyone
    onCreate: (newGroup) => newGroup.makePublic(),
  });

export const CursorRoot = co.map({
  camera: Camera,
  cursors: CursorFeed,
});

export const CursorAccount = co
  .account({
    profile: CursorProfile,
    root: CursorRoot,
  })
  .withMigration((account) => {
    if (!account.$jazz.has("root")) {
      account.$jazz.set("root", {
        camera: {
          position: {
            x: 0,
            y: 0,
          },
        },
        cursors: [],
      });
    }

    if (!account.$jazz.has("profile")) {
      account.$jazz.set("profile", {
        name: "Anonymous user",
      });
    }
  });
