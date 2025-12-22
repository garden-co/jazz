import { co, z } from "jazz-tools";
import { useSuspenseCoState } from "jazz-tools/react";
import React, { useState } from "react";

export const Milestone = co.map({
  name: z.string(),
});
export type Milestone = co.loaded<typeof Milestone>;

export const Projects = co
  .map({
    name: z.string(),
    milestones: co.list(Milestone).optional(),
  })
  .withMigration((project) => {
    if (!project.$jazz.has("milestones") && shouldRunMigration()) {
      console.log("Running migration", getMilestoneName());
      project.$jazz.set(
        "milestones",
        co.list(Milestone).create([{ name: getMilestoneName() }], {
          owner: project.$jazz.owner,
          unique: { type: "milestone", projectId: project.$jazz.id },
        }),
      );
    }
  })
  .withPermissions({
    onCreate(group) {
      group.makePublic("writer");
    },
  });
export type Projects = co.loaded<typeof Projects>;

// Using this to control when the migration should run.
function shouldRunMigration() {
  const url = new URL(window.location.href);
  return url.searchParams.get("runMigration") === "true";
}

function getMilestoneName() {
  const url = new URL(window.location.href);
  return url.searchParams.get("milestoneName") ?? "Milestone";
}

function getProjectId() {
  const url = new URL(window.location.href);

  const projectId = url.searchParams.get("projectId");

  if (projectId) {
    return projectId;
  }

  const project = Projects.create({ name: "Project 1" });

  project.$jazz.waitForSync().then(() => {
    history.pushState({}, "", `${url.pathname}?projectId=${project.$jazz.id}`);
  });

  return project.$jazz.id;
}

function ShowProject(props: { projectId: string }) {
  const project = useSuspenseCoState(Projects, props.projectId, {
    resolve: {
      milestones: { $each: true },
    },
  });

  return (
    <div>
      <h1>Concurrent Migration</h1>
      <div>
        {project.milestones?.map((milestone) => (
          <div key={milestone.$jazz.id}>
            <div>{milestone.name}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

export function ConcurrentMigration() {
  const id = useState(getProjectId)[0];
  const [runMigration, setRunMigration] = useState(shouldRunMigration);

  function handleRunMigration() {
    const url = new URL(window.location.href);
    url.searchParams.set("runMigration", "true");
    history.pushState({}, "", url.toString());
    setRunMigration(true);
  }

  if (!runMigration) {
    return (
      <div>
        <h1>Concurrent Migration</h1>
        <div>Migration not run yet. {runMigration ? "true" : "false"}</div>
        <button onClick={handleRunMigration}>Run Migration</button>
      </div>
    );
  }

  return (
    <div>
      <h1>Result</h1>
      <ErrorBoundary title="Error in Concurrent Migration">
        <ShowProject projectId={id} />
      </ErrorBoundary>
    </div>
  );
}

class ErrorBoundary extends React.Component<
  { children: React.ReactNode; title: string },
  { hasError: boolean }
> {
  constructor(props: { children: React.ReactNode; title: string }) {
    super(props);
    this.state = { hasError: false };
  }

  componentDidCatch(error: Error): void {
    console.error(error);
  }

  render() {
    if (this.state.hasError) {
      return <div>{this.props.title}</div>;
    }
    return this.props.children;
  }
}
