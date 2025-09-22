import { useCallback } from "react";
import { JazzAccount, JournalEntryList } from "../schema";

export function useDeleteEntries({ owner }: { owner: JazzAccount }) {
  const deleteEntries = useCallback(async () => {
    const confirmed = confirm("Are you sure you want to delete all entries?");

    if (!confirmed) {
      return;
    }

    try {
      if (owner?.root?.journalEntries) {
        owner.root.$jazz.set("journalEntries", JournalEntryList.create([]));
        window.location.reload();
      }
    } catch (error) {
      console.error(error);
    }
  }, [owner]);

  return { deleteEntries };
}
