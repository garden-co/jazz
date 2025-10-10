import { useCallback, useState } from "react";
import { Embedding, JournalEntry, JournalEntryList } from "../schema";

export function useCreateEntry({
  createEmbedding,
  journalEntries,
}: {
  createEmbedding: (text: string) => Promise<number[]>;
  journalEntries?: JournalEntryList;
}) {
  const [isCreating, setIsCreating] = useState(false);

  const promptNewEntry = useCallback(async () => {
    const text = prompt("What's on your mind?");

    if (!text) {
      return;
    }

    if (!journalEntries) return;

    try {
      setIsCreating(true);
      const embedding = await createEmbedding(text);

      const journalEntry = JournalEntry.create(
        {
          text,
          feelings: [],
          topics: [],
          embedding: Embedding.create(embedding, {
            owner: journalEntries?.$jazz.owner,
          }),
        },
        { owner: journalEntries?.$jazz.owner },
      );

      if (journalEntries) {
        journalEntries.$jazz.unshift(journalEntry);
      }
    } catch (error) {
      console.error(error);
    } finally {
      setIsCreating(false);
    }
  }, [createEmbedding, journalEntries]);

  return { isCreatingEntry: isCreating, promptNewEntry };
}
