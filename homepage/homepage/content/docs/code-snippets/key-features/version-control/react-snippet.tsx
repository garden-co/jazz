import { ID } from "jazz-tools";
import { MyAccount, Project } from "./schema";
const projectId = "";
import { useAccount, useCoState, useSuspenseCoState } from "jazz-tools/react";
import { Text, TextInput, View } from "react-native";
import { Suspense } from "react";

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
  const project = useSuspenseCoState(Project, projectId, {
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

  // [!code hide]
  const handleSave = () => null;
  return (
    <Suspense fallback={<p>Loading...</p>}>
      <form onSubmit={handleSave}>
        <label>Project Title
          <input
            value={project.title}
            onChange={(evt) => project.$jazz.set('title', evt.currentTarget.value)} />
        </label>
        {project.tasks.map((task, i) => {
          return <input
            key={task.$jazz.id}
            value={task.title}
            onChange={(evt) => task.$jazz.set('title', evt.currentTarget.value)} />
        })}
      </form>;
    </Suspense>
  )
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

// #region EditOnBranchRN
function EditProjectComponent({
  projectId,
  currentBranchName,
}: {
  projectId: ID<typeof Project>;
  currentBranchName: string;
}) {
  const project = useCoState(Project, projectId, {
    resolve: {
      // When we use a 'resolve' query with a branch, all of the 'resolved' CoValues are also part of the new branch
      tasks: { $each: true },
    },
    unstable_branch: {
      name: currentBranchName,
    },
  });

  return (
    <View>
      {project.$isLoaded &&
        <View>
          <Text>Project</Text>
          <TextInput value={project.title} onChangeText={v => project.$jazz.set("title", v)} />
          {project.tasks.map(task => {
            return (
              <TextInput key={task.$jazz.id} value={task.title} onChangeText={v => { task.$jazz.set('title', v) }} />
            )
          })}
        </View>
      }
    </View>
  );
}
// #endregion