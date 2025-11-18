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
      if (!project.$isLoaded) return project.$jazz.loadingState;
      return {
        name: project.name,
        taskCount: project.tasks.length,
      };
    },
    // Only re-render if the name or the number of tasks change
    equalityFn: (a, b) => {
      if (typeof a === "string" || typeof b === "string") {
        // If either value is a string, it is not loaded, so we cannot check for equality.
        return false;
      }
      return a?.name === b?.name && a?.taskCount === b?.taskCount;
    },
  });

  if (typeof project === "string") {
    switch (project) {
      case "unauthorized":
        return "Project not accessible";
      case "unavailable":
        return "Project not found";
      case "loading":
        return "Loading...";
    }
  }

  return (
    <div>
      <h1>{project.name}</h1>
      <small>{project.taskCount} task(s)</small>
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
