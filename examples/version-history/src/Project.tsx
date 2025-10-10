import { createInviteLink, useAccount } from "jazz-tools/react";
import { useCoState } from "jazz-tools/react";
import { IssueComponent } from "./Issue.tsx";
import { Issue, Project } from "./schema.ts";

export function ProjectComponent({ projectID }: { projectID: string }) {
  const { me } = useAccount();
  const project = useCoState(Project, projectID, {
    resolve: { issues: { $each: true } },
  });

  if (!me.$isLoaded || !project.$isLoaded) {
    return <div>Loading project...</div>;
  }

  const invite = (role: "reader" | "writer") => {
    const link = createInviteLink(project, role, { valueHint: "project" });
    navigator.clipboard.writeText(link);
  };
  const createAndAddIssue = () => {
    project.issues.$jazz.push(
      Issue.create(
        {
          title: "",
          description: "",
          estimate: 0,
          status: "backlog",
        },
        project.$jazz.owner,
      ),
    );
  };
  return (
    <div>
      <h1>{project.name}</h1>
      {me.canAdmin(project) && (
        <>
          <button onClick={() => invite("reader")}>Invite Guest</button>
          <button onClick={() => invite("writer")}>Invite Member</button>
        </>
      )}
      <div className="border-r border-b">
        {project.issues.map((issue) => (
          <IssueComponent key={issue.$jazz.id} issue={issue} />
        ))}
        <button onClick={createAndAddIssue}>Create Issue</button>
      </div>
    </div>
  );
}
