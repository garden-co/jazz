import { useCallback, useState } from "react";
import { Embedding, type JazzAccount, JournalEntry } from "../schema";

const fetchJournalEntries = async () => {
  const response = await fetch("/datasets/journal/data-hou8Ux.json");
  const data = await response.json();

  return data as Array<{
    c: string;
    f: string[];
    t: string[];
  }>;
};

/**
 * Creates journal entries from the dataset.
 */
export const useJournalSeed = ({
  createEmbedding,
  owner,
}: {
  createEmbedding: (text: string) => Promise<number[]>;
  owner: JazzAccount;
}) => {
  const [isSeeding, setIsSeeding] = useState(false);

  const seedJournal = useCallback(async () => {
    setIsSeeding(true);
    try {
      const journalEntries = await fetchJournalEntries();

      for (const entry of journalEntries.slice(0, 10000)) {
        const embedding = await createEmbedding(entry.c);

        const journalEntry = JournalEntry.create({
          text: entry.c,
          feelings: entry.f,
          topics: entry.t,
          embedding: Embedding.create(embedding),
        });

        if (owner?.root?.journalEntries) {
          owner.root.journalEntries.$jazz.push(journalEntry);
        }
      }
    } catch (error) {
      console.error(error);
    } finally {
      setIsSeeding(false);
    }
  }, [createEmbedding, owner]);

  return { isSeeding, seedJournal };
};
