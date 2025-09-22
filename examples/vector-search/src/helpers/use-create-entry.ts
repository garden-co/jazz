import { useCallback, useState } from "react";
import { Embedding, JazzAccount, JournalEntry } from "../schema";

export function useCreateEntry({
  createEmbedding,
  owner,
}: {
  createEmbedding: (text: string) => Promise<number[]>;
  owner: JazzAccount;
}) {
  const [isCreating, setIsCreating] = useState(false);

  const promptNewEntry = useCallback(async () => {
    const text = prompt("What's on your mind?");

    if (!text) {
      return;
    }

    try {
      setIsCreating(true);
      const embedding = await createEmbedding(text);

      const journalEntry = JournalEntry.create({
        text,
        feelings: [],
        topics: [],
        embedding: await Embedding.createFrom(embedding),
      });

      if (owner?.root?.journalEntries) {
        owner.root.journalEntries.$jazz.unshift(journalEntry);
      }
    } catch (error) {
      console.error(error);
    } finally {
      setIsCreating(false);
    }
  }, [createEmbedding, owner]);

  return { isCreatingEntry: isCreating, promptNewEntry };
}
