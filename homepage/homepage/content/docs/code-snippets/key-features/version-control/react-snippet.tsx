import { ID } from "jazz-tools";
import { MyAccount, Project } from "./schema";
const projectId = "";
import { useAccount, useCoState } from "jazz-tools/react";

// #region UseCoState
const branch = useCoState(Project, projectId, {
  unstable_branch: { name: "feature-branch" },
});
// #endregion

// #region EditOnBranch
function EditProject({
  projectId,
  currentBranchName,
}: {
  projectId: ID<typeof Project>;
  currentBranchName: string;
}) {
  const project = useCoState(Project, projectId, {
    resolve: {
      tasks: { $each: true },
    },
    unstable_branch: {
      name: currentBranchName,
    },
  });

  const handleTitleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    // Won't be visible on main until merged
    project.$isLoaded && project.$jazz.set("title", e.target.value);
  };

  const handleTaskTitleChange = (
    index: number,
    e: React.ChangeEvent<HTMLInputElement>,
  ) => {
    const task = project.$isLoaded && project.tasks[index];

    // The task is also part of the branch because we used the `resolve` option
    // with `tasks: { $each: true }`
    // so the changes won't be visible on main until merged
    task && task.$jazz.set("title", e.target.value);
  };

  // @ts-expect-error handleSave not implemented
  return <form onSubmit={handleSave}>{/* Edit form fields */}</form>;
}
// #endregion

// #region AccountModifications
const me = useAccount(MyAccount, {
  resolve: { root: true },
  unstable_branch: { name: "feature-branch" },
});

me.$isLoaded && me.$jazz.set("root", { value: "Feature Branch" }); // Will also modify the main account
me.$isLoaded && me.root.$jazz.set("value", "Feature Branch"); // This only modifies the branch
// #endregion
