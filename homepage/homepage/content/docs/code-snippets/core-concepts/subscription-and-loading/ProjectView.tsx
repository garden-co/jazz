import { useMemo } from "react";
import { Project, MyAppAccount } from "./schema";
// #region Basic
import { useCoState } from "jazz-tools/react";

function ProjectView({ projectId }: { projectId: string }) {
  // Subscribe to a project and resolve its tasks
  const project = useCoState(Project, projectId, {
    resolve: { tasks: { $each: true } }, // Tell Jazz to load each task in the list
  });

  if (!project.$isLoaded) {
    switch (project.$jazz.loadingState) {
      case "unauthorized":
        return "Project not accessible";
      case "unavailable":
        return "Project not found";
      case "loading":
        return "Loading project...";
    }
  }

  return (
    <div>
      <h1>{project.name}</h1>
      <ul>
        {project.tasks.map((task) => (
          <li key={task.$jazz.id}>{task.title}</li>
        ))}
      </ul>
    </div>
  );
}
// #endregion

// #region ShallowLoad
const projectId = "";
const projectWithTasksShallow = useCoState(Project, projectId, {
  resolve: {
    tasks: true,
  },
});
// #endregion

// #region Selector
function ProjectViewWithSelector({ projectId }: { projectId: string }) {
  // Subscribe to a project
  const project = useCoState(Project, projectId, {
    resolve: {
      tasks: true,
    },
    select: (project) => {
      if (!project.$isLoaded) return {
        loadingState: project.$jazz.loadingState
      };
      return {
        name: project.name,
        taskCount: project.tasks.length,
        loadingState: project.$jazz.loadingState
      };
    },
    // Only re-render if the name or the number of tasks change
    equalityFn: (a, b) => {
      if (a.loadingState !== 'loaded' || b.loadingState !== 'loaded') return false;

      return a?.name === b?.name && a?.taskCount === b?.taskCount;
    },
  });

  switch (project.loadingState) {
    case "unauthorized":
      return "Project not accessible";
    case "unavailable":
      return "Project not found";
    case "loading":
      return "Loading...";
  }

  return (
    <div>
      <h1>{project.name}</h1>
      <small>{project.taskCount} task(s)</small>
    </div>
  );
}
// #endregion

const someExpensiveSortFunction = () => 1;
const someExpensiveReduceFunction = () => ({});
const GroupedTaskDisplay = ({ tasks }: { tasks: any }) => { tasks; return null };

// #region ExpensiveSelector
function ProjectViewWithExpensiveOperations({ projectId }: { projectId: string }) {
  const project = useCoState(Project, projectId, {
    resolve: {
      tasks: {
        $each: true,
      }
    },
    select: (project) => {
      if (!project.$isLoaded) return {
        loadingState: project.$jazz.loadingState,
        tasks: [],
        taskIds: []
      };
      return {
        name: project.name,
        tasks: project.tasks,
        loadingState: project.$jazz.loadingState
      };
    },
    equalityFn: (a, b) => {
      if (a.loadingState !== 'loaded' || b.loadingState !== 'loaded') return false;
      if (a.name !== b.name) return false;
      if (a.tasks.length !== b.tasks.length) return false;
      const aTaskIds = new Set(a.tasks.map(t => t.$jazz.id));
      return b.tasks.every(t => aTaskIds.has(t.$jazz.id));
    },
  });

  const tasksAfterExpensiveComputations = useMemo(() => {
    const sortedTasks = project.tasks.slice(0).sort(someExpensiveSortFunction);
    const groupedTasks = sortedTasks.reduce(someExpensiveReduceFunction, {});
    return groupedTasks;
  }, [project.tasks]);


  switch (project.loadingState) {
    case "unauthorized":
      return "Project not accessible";
    case "unavailable":
      return "Project not found";
    case "loading":
      return "Loading...";
  }


  return (
    <div>
      <h1>{project.name}</h1>
      <GroupedTaskDisplay tasks={tasksAfterExpensiveComputations} />
    </div>
  );
}
// #endregion

// #region UseAccountWithSelector
import { useAccount } from "jazz-tools/react";

function ProfileName() {
  // Only re-renders when the profile name changes
  const profileName = useAccount(MyAppAccount, {
    resolve: {
      profile: true,
    },
    select: (account) =>
      account.$isLoaded ? account.profile.name : "Loading...",
  });

  return <div>{profileName}</div>;
}
// #endregion
