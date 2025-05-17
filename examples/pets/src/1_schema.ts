import {
  Account,
  CoFeed,
  CoList,
  CoMap,
  ImageDefinition,
  Profile,
  coField,
  zodSchemaToCoSchema,
} from "jazz-tools";

/** Walkthrough: Defining the data model with CoJSON
 *
 *  Here, we define our main data model of TODO
 *
 *  TODO
 **/

export const ReactionTypes = [
  "aww",
  "love",
  "haha",
  "wow",
  "tiny",
  "chonkers",
] as const;

export type ReactionType = (typeof ReactionTypes)[number];

export class PetReactions extends CoFeed.Of(coField.json<ReactionType>()) {}

export class PetPost extends CoMap {
  name = coField.string;
  image = coField.ref(zodSchemaToCoSchema(ImageDefinition));
  reactions = coField.ref(PetReactions);
}

export class ListOfPosts extends CoList.Of(coField.ref(PetPost)) {}

export class PetAccountRoot extends CoMap {
  posts = coField.ref(ListOfPosts);
}

export class PetAccount extends Account {
  profile = coField.ref(Profile);
  root = coField.ref(PetAccountRoot);

  migrate() {
    if (!this._refs.root) {
      this.root = PetAccountRoot.create(
        {
          posts: ListOfPosts.create([], { owner: this }),
        },
        { owner: this },
      );
    }
  }
}

/** Walkthrough: Continue with ./2_App.tsx */
