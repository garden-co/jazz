export function Footer({
  isCreatingEntry,
  isLoading,
  deleteEntries,
  promptNewEntry,
  showCreateNew,
}: {
  isCreatingEntry: boolean;
  isLoading: boolean;
  deleteEntries: () => void;
  promptNewEntry: () => void;
  showCreateNew: boolean;
}) {
  return (
    <div className="flex flex-col flex-col-reverse gap-2 md:flex-row md:gap-4 justify-between border-t border-zinc-200 py-4">
      <div className="flex gap-2">
        &copy;&nbsp;{new Date().getFullYear()}
        <a
          href="https://jazz.tools"
          target="_blank"
          rel="noopener noreferrer"
          className="underline hover:no-underline"
        >
          Jazz
        </a>
      </div>

      <div className="flex gap-2">
        {showCreateNew && (
          <button
            className="text-zinc-500 px-2 rounded cursor-pointer hover:bg-zinc-300 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            onClick={promptNewEntry}
            disabled={isCreatingEntry || isLoading}
          >
            {isCreatingEntry ? "Creating..." : "+ New entry"}
          </button>
        )}
        <button
          className="bg-zinc-200 px-2 rounded cursor-pointer hover:bg-zinc-300 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          onClick={deleteEntries}
        >
          Remove all entries
        </button>
      </div>
    </div>
  );
}
