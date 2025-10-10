import { useCallback } from "react";
import { JournalEntryList } from "../schema";

export function useDeleteEntries({
  journalEntries,
}: {
  journalEntries?: JournalEntryList;
}) {
  const deleteEntries = useCallback(async () => {
    const confirmed = confirm("Are you sure you want to delete all entries?");

    if (!confirmed) {
      return;
    }

    try {
      if (journalEntries) {
        journalEntries.$jazz.applyDiff([]);
      }
    } catch (error) {
      console.error(error);
    }
  }, [journalEntries]);

  return { deleteEntries };
}
