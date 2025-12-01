import { Group } from "jazz-tools";
import { Project } from "./schema";
import { createJazzTestAccount } from "jazz-tools/testing";
const projectId = "";
const member = await createJazzTestAccount();

// #region Branch
const branch = await Project.load(projectId, {
  unstable_branch: { name: "feature-branch" },
});
// #endregion

// #region Permissions
const featureBranch = await Project.load(projectId, {
  unstable_branch: { name: "feature-branch" },
});
featureBranch.$isLoaded &&
  featureBranch.$jazz.owner.addMember(member, "writer"); // Will also add the member to the main Group
// #endregion

// #region Merge
async function handleSave() {
  // Merge all currently loaded values in the branch
  branch.$isLoaded && branch.$jazz.unstable_merge();
}
// #endregion

// #region MergeWithResolve
async function handleSaveWithResolve() {
  // Merge the branch changes back to main
  await Project.unstable_merge(projectId, {
    resolve: {
      tasks: { $each: true },
    },
    branch: { name: "feature-branch" },
  });
}
// #endregion
//
const project = Project.create({ title: "Test", tasks: [], priority: "low" });

const originalProject = Project.create({
  title: "Test",
  tasks: [],
  priority: "low",
});

// #region CascadingMerge
async function handleTaskSave(index: number) {
  const task = project.tasks[index];
  // Only the changes to the task will be merged
  task.$jazz.unstable_merge();
}
// #endregion

// #region ConflictResolution
// Branch modifies priority to "high"
branch.$isLoaded && branch.$jazz.applyDiff({ priority: "high" });

// Meanwhile, main modifies priority to "urgent"
originalProject.$isLoaded &&
  originalProject.$jazz.applyDiff({ priority: "urgent" });

// Merge the branch
branch.$isLoaded && branch.$jazz.unstable_merge();

// Main's value ("urgent") wins because it was written later
console.log(originalProject.priority); // "urgent"
// #endregion

// #region PrivateBranches
// Create a private group for the branch
const privateGroup = Group.create();

const privateBranch = Project.load(projectId, {
  unstable_branch: {
    name: "private-edit",
    owner: privateGroup,
  },
});

// Only members of privateGroup can see the branch content
// The sync server cannot read the branch content
// #endregion

// #region BranchIdentification
const myBranch = await Project.load(projectId, {
  unstable_branch: { name: "feature-branch" },
});

console.log(myBranch.$jazz.id); // Branch ID is the same as source
console.log(myBranch.$isLoaded && myBranch.$jazz.branchName); // "feature-branch"
console.log(myBranch.$isLoaded && myBranch.$jazz.isBranched); // true
// #endregion
