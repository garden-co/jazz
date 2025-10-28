import { useState } from "react";
import { Project } from "../schema.ts";
import { useOrganizationSelector } from "./OrganizationProvider.ts";

export function CreateProject() {
  const organizationOwner = useOrganizationSelector({
    select: (organization) => organization.$jazz.owner,
  });

  const projects = useOrganizationSelector({
    select: (organization) => organization.projects,
  });

  const [name, setName] = useState<string>("");

  const onSave = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();

    if (name.length > 0) {
      const project = Project.create({ name }, { owner: organizationOwner });

      projects.$jazz.push(project);
      setName("");
    }
  };

  return (
    <form onSubmit={onSave} className="flex gap-3 items-center">
      <label className="flex-1">
        <span className="sr-only">Project name</span>
        <input
          type="text"
          name="name"
          id="name"
          placeholder="Enter project name..."
          value={name}
          className="rounded-md shadow-sm dark:bg-transparent w-full"
          onChange={(e) => setName(e.target.value)}
          required
        />
      </label>
      <button
        type="submit"
        className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded"
      >
        Add project
      </button>
    </form>
  );
}
