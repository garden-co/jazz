import { useAccount } from "jazz-react";

export function DraftIndicator() {
  const { me } = useAccount({
    resolve: { root: { draft: true } },
  });

  if (me?.root.draft?.hasChanges) {
    return (
      <div className="absolute -top-1 -right-1 bg-blue-500 border-2 border-white w-3 h-3 rounded-full dark:border-stone-925">
        <span className="sr-only">You have a draft</span>
      </div>
    );
  }
}
