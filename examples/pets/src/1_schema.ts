import { co, z } from "jazz-tools";

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

export const PetReactions = co.feed(z.literal([...ReactionTypes]));

export const PetPost = co.map({
  name: z.string(),
  image: co.image(),
  reactions: PetReactions,
});

export const PetAccountRoot = co.map({
  posts: co.list(PetPost),
});

export const PetAccount = co
  .account({
    profile: co.profile(),
    root: PetAccountRoot,
  })
  .withMigration(async (account) => {
    if (!account.root) {
      account.root = PetAccountRoot.create(
        {
          posts: co.list(PetPost).create([], { owner: account }),
        },
        { owner: account },
      );
    }
  });

/** Walkthrough: Continue with ./2_App.tsx */
