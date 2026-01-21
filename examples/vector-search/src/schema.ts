import { co, z, setDefaultValidationMode } from "jazz-tools";

setDefaultValidationMode("strict");

export const JazzProfile = co.profile();

// 384-dim vector schema
export const Embedding = co.vector(384).withPermissions({
  onInlineCreate: "sameAsContainer",
});

export const JournalEntry = co
  .map({
    text: z.string(),
    feelings: z.array(z.string()),
    topics: z.array(z.string()),
    embedding: Embedding,
  })
  .withPermissions({
    onInlineCreate: "sameAsContainer",
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
        journalEntries: [],
      });
    }
  });
export type JazzAccount = co.loaded<typeof JazzAccount>;
