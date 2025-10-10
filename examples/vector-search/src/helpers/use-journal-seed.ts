import { useCallback, useState } from "react";
import { Embedding, JournalEntry, JournalEntryList } from "../schema";

const fetchJournalEntries = async () => {
  const response = await fetch("/datasets/journal/data-hou8Ux.json");
  const data = await response.json();

  return data as Array<{
    c: string;
    f: string[];
    t: string[];
  }>;
};

type SeedProgress = {
  targetCount: number;
  seededCount: number;
};
const SEED_PROGRESS_START: SeedProgress = { targetCount: 0, seededCount: 0 };

/**
 * Creates journal entries from the dataset.
 */
export const useJournalSeed = ({
  createEmbedding,
  journalEntries,
  batchSize = 10,
}: {
  createEmbedding: (text: string) => Promise<number[]>;
  journalEntries?: JournalEntryList;
  batchSize?: number;
}) => {
  const [isSeeding, setIsSeeding] = useState(false);
  const [progress, setProgress] = useState<SeedProgress>(SEED_PROGRESS_START);

  const seedJournal = useCallback(async () => {
    if (!journalEntries) return;

    setIsSeeding(true);
    setProgress(SEED_PROGRESS_START);
    try {
      const journalEntriesData = await fetchJournalEntries();
      setProgress({ targetCount: journalEntriesData.length, seededCount: 0 });

      for (let i = 0; i < journalEntriesData.length; i += batchSize) {
        const entriesDataBatch = journalEntriesData.slice(i, i + batchSize);

        const newEntriesBatch = [];

        for (const entry of entriesDataBatch) {
          const embedding = await createEmbedding(entry.c);

          const journalEntry = JournalEntry.create(
            {
              text: entry.c,
              feelings: entry.f,
              topics: entry.t,
              embedding: Embedding.create(embedding, {
                owner: journalEntries?.$jazz.owner,
              }),
            },
            { owner: journalEntries?.$jazz.owner },
          );

          newEntriesBatch.push(journalEntry);
        }

        setProgress((progress) => ({
          targetCount: progress.targetCount,
          seededCount: progress.seededCount + newEntriesBatch.length,
        }));

        if (journalEntries) {
          journalEntries.$jazz.push(...newEntriesBatch);
        }
        await new Promise((resolve) => setTimeout(resolve, 0));
      }
    } catch (error) {
      console.error(error);
    } finally {
      setIsSeeding(false);
    }
  }, [createEmbedding, journalEntries]);

  return { isSeeding, progress, seedJournal };
};
