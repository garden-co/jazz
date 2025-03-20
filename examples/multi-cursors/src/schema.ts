import { Account, CoFeed, CoMap, Group, Profile, co } from "jazz-tools";

export class Vec2 extends CoMap {
  x = co.number;
  y = co.number;
}

export class CursorProfile extends Profile {
  name = co.string;
  position = co.ref(Vec2);
}

export class Camera extends CoMap {
  position = co.ref(Vec2);
}

export class CursorRoot extends CoMap {
  camera = co.ref(Camera);
}

export class CursorFeed extends CoFeed.Of(co.ref(CursorProfile)) {}

export class CursorAccount extends Account {
  profile = co.ref(CursorProfile);
  root = co.ref(CursorRoot);

  /** The account migration is run on account creation and on every log-in.
   *  You can use it to set up the account root and any other initial CoValues you need.
   */
  migrate(this: CursorAccount) {
    console.log("migrate", this);
    if (this.root === undefined) {
      this.root = CursorRoot.create({
        camera: Camera.create({
          position: Vec2.create({
            x: 0,
            y: 0,
          }),
        }),
      });
    }

    if (this.profile === undefined) {
      const group = Group.create();
      group.addMember("everyone", "reader"); // The profile info is visible to everyone

      this.profile = CursorProfile.create(
        {
          name: "Anonymous user",
          position: Vec2.create(
            {
              x: 0,
              y: 0,
            },
            { owner: group },
          ),
        },
        group,
      );
    }
  }
}
