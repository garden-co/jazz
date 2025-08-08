import { co, Group } from "jazz-tools";
import { useAccount } from "jazz-tools/react";
import { useCallback } from "react";
import { useNavigate } from "react-router";
import { Task, TodoAccount, TodoProject } from "./1_schema";
import { SubmittableInput } from "./basicComponents";

export function NewProjectForm() {
  // `me` represents the current user account, which will determine
  // access rights to CoValues. We get it from the top-level provider `<WithJazz/>`.
  const { me } = useAccount(TodoAccount, {
    resolve: { root: { projects: { $each: { $onError: null } } } },
  });
  const navigate = useNavigate();

  const createProject = useCallback(
    (title: string) => {
      if (!me || !title) return;

      // To create a new todo project, we first create a `Group`,
      // which is a scope for defining access rights (reader/writer/admin)
      // of its members, which will apply to all CoValues owned by that group.
      const projectGroup = Group.create();

      // Then we create an empty todo project within that group
      const project = TodoProject.create(
        {
          title,
          tasks: co.list(Task).create([], { owner: projectGroup }),
        },
        { owner: projectGroup },
      );

      me.root?.projects?.push(project);

      navigate("/project/" + project.id);
    },
    [me, navigate],
  );

  return (
    <SubmittableInput
      onSubmit={createProject}
      label="Create New Project"
      placeholder="New project title"
    />
  );
}

/** Walkthrough: continue with ./4_ProjectTodoTable.tsx */
