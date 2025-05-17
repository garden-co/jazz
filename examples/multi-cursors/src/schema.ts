import { Account, CoFeed, CoMap, Group, Profile, coField } from "jazz-tools";
import type { Camera, Cursor } from "./types";

export class CursorFeed extends CoFeed.Of(coField.json<Cursor>()) {}

export class CursorProfile extends Profile {
  name = coField.string;
}

export class CursorRoot extends CoMap {
  camera = coField.json<Camera>();
  cursors = coField.ref(CursorFeed);
}

export class CursorContainer extends CoMap {
  cursorFeed = coField.ref(CursorFeed);
}

export class CursorAccount extends Account {
  profile = coField.ref(CursorProfile);
  root = coField.ref(CursorRoot);

  /** The account migration is run on account creation and on every log-in.
   *  You can use it to set up the account root and any other initial CoValues you need.
   */
  migrate(this: CursorAccount) {
    if (this.root === undefined) {
      this.root = CursorRoot.create({
        camera: {
          position: {
            x: 0,
            y: 0,
          },
        },
        cursors: CursorFeed.create([]),
      });
    }

    if (this.profile === undefined) {
      const group = Group.create();
      group.addMember("everyone", "reader"); // The profile info is visible to everyone

      this.profile = CursorProfile.create(
        {
          name: "Anonymous user",
        },
        group,
      );
    }
  }
}
