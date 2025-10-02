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
  owner,
}: {
  createEmbedding: (text: string) => Promise<number[]>;
  owner: JazzAccount;
}) => {
  const [isSeeding, setIsSeeding] = useState(false);
  const [progress, setProgress] = useState<SeedProgress>(SEED_PROGRESS_START);

  const seedJournal = useCallback(async () => {
    setIsSeeding(true);
    setProgress(SEED_PROGRESS_START);
    try {
      const journalEntries = await fetchJournalEntries();
      setProgress({ targetCount: journalEntries.length, seededCount: 0 });

      for (const entry of journalEntries) {
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

        setProgress((progress) => ({
          targetCount: progress.targetCount,
          seededCount: progress.seededCount + 1,
        }));

        await new Promise((resolve) => setTimeout(resolve, 0));
      }
    } catch (error) {
      console.error(error);
    } finally {
      setIsSeeding(false);
    }
  }, [createEmbedding, owner]);

  return { isSeeding, progress, seedJournal };
};
