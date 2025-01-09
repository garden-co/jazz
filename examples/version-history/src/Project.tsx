import { createInviteLink } from "jazz-react";
import { useCoState } from "jazz-react";
import { ID } from "jazz-tools";
import { IssueComponent } from "./Issue.tsx";
import { Issue, Project } from "./schema.ts";
export function ProjectComponent({ projectID }: { projectID: ID<Project> }) {
  const project = useCoState(Project, projectID, { issues: [{}] });

  if (!project) return;

  const invite = (role: "reader" | "writer") => {
    const link = createInviteLink(project, role, { valueHint: "project" });
    navigator.clipboard.writeText(link);
  };
  const createAndAddIssue = () => {
    project?.issues.push(
      Issue.create(
        {
          title: "",
          description: "",
          estimate: 0,
          status: "backlog",
        },
        { owner: project._owner },
      ),
    );
  };
  return project ? (
    <div>
      <h1>{project.name}</h1>
      {project._owner?.myRole() === "admin" && (
        <>
          <button onClick={() => invite("reader")}>Invite Guest</button>
          <button onClick={() => invite("writer")}>Invite Member</button>
        </>
      )}
      <div className="border-r border-b">
        {project.issues.map((issue) => (
          <IssueComponent key={issue.id} issue={issue} />
        ))}
        <button onClick={createAndAddIssue}>Create Issue</button>
      </div>
    </div>
  ) : (
    <div>Loading project...</div>
  );
}
