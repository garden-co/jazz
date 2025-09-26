import { Group, co, z } from "jazz-tools";

export const JazzProfile = co.profile();

export const Embedding = co.vector(384); // <- 384-dim vector schema

export const JournalEntry = co.map({
  text: z.string(),
  feelings: z.array(z.string()),
  topics: z.array(z.string()),
  embedding: Embedding,
});
export type JournalEntry = co.loaded<typeof JournalEntry>;

export const JournalEntryList = co.list(JournalEntry);
export type JournalEntryList = co.loaded<typeof JournalEntryList>;

export const AccountRoot = co.map({
  journalEntries: JournalEntryList,
});
export type AccountRoot = co.loaded<typeof AccountRoot>;

export const JazzAccount = co
  .account({
    profile: JazzProfile,
    root: AccountRoot,
  })
  .withMigration(async (account) => {
    if (!account.$jazz.has("root")) {
      account.$jazz.set("root", {
        journalEntries: JournalEntryList.create([]),
      });
    }

    if (!account.$jazz.has("profile")) {
      const profileGroup = Group.create();
      profileGroup.makePublic();

      account.$jazz.set(
        "profile",
        JazzProfile.create(
          {
            name: "Anonymous",
          },
          profileGroup,
        ),
      );
    }
  });
export type JazzAccount = co.loaded<typeof JazzAccount>;
