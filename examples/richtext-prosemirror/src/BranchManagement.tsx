import { useAccount } from "jazz-tools/react";
import { JazzAccount } from "./schema";

interface BranchManagementProps {
  currentBranch: string | undefined;
  onBranchChange: (branch: string | undefined) => void;
  onBranchMerge: () => void;
}

export function BranchManagement({
  currentBranch,
  onBranchChange,
  onBranchMerge,
}: BranchManagementProps) {
  const { me } = useAccount(JazzAccount, {
    resolve: { profile: { branches: true } },
  });

  const branches = me.$isLoaded ? me.profile.branches : undefined;

  function handleCreateBranch(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();

    const data = new FormData(e.currentTarget);
    const branch = data.get("branch");

    if (!branches || !branch || typeof branch !== "string") return;

    if (!branches.includes(branch)) {
      branches.$jazz.push(branch);
    }

    onBranchChange(branch);
  }

  function handleMergeBranch() {
    if (!currentBranch) return;
    onBranchMerge();
    onBranchChange(undefined);
  }

  function handleSelectBranch(e: React.ChangeEvent<HTMLSelectElement>) {
    const selectedBranch = e.target.value;
    if (selectedBranch === "") {
      onBranchChange(undefined);
    } else {
      onBranchChange(selectedBranch);
    }
  }

  return (
    <div className="flex flex-row gap-4">
      {/* Branch Selection */}
      <div className="flex flex-col gap-2">
        <label
          className="text-sm font-medium text-stone-600"
          htmlFor="branch-select"
        >
          Current Branch
        </label>
        <div className="flex flex-row gap-2">
          <select
            id="branch-select"
            value={currentBranch || ""}
            onChange={handleSelectBranch}
            className="border border-stone-200 rounded shadow-sm py-2 px-3 font-mono text-sm bg-stone-50 text-stone-900"
          >
            <option value="">Main branch</option>
            {branches?.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>
          {currentBranch && (
            <button
              className="bg-green-500 hover:bg-green-700 text-white font-bold px-4 rounded"
              type="button"
              onClick={handleMergeBranch}
            >
              Merge Branch
            </button>
          )}
        </div>
      </div>

      {/* Create New Branch */}
      {!currentBranch && (
        <form className="flex flex-col gap-2" onSubmit={handleCreateBranch}>
          <label
            className="text-sm font-medium text-stone-600"
            htmlFor="branch-input"
          >
            Create New Branch
          </label>
          <div className="flex flex-row gap-2">
            <input
              type="text"
              id="branch-input"
              name="branch"
              required
              placeholder="Enter branch name..."
              className="border border-stone-200 rounded shadow-sm py-2 px-3 font-mono text-sm bg-stone-50 text-stone-900"
            />
            <button
              type="submit"
              className="bg-blue-500 hover:bg-blue-700 text-white font-bold px-4 rounded"
            >
              Create Branch
            </button>
          </div>
        </form>
      )}
    </div>
  );
}
