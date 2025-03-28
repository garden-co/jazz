import { useAccount, useCoState } from "jazz-react";
import { Group, ID } from "jazz-tools";
import { useState } from "react";
import { useNavigate } from "react-router";
import { DraftOrganization, ListOfProjects, Organization } from "../schema.ts";
import { Errors } from "./Errors.tsx";
import { OrganizationForm } from "./OrganizationForm.tsx";

export function CreateOrganization() {
  const { me } = useAccount({
    resolve: { root: { draftOrganization: true, organizations: true } },
  });
  const [errors, setErrors] = useState<string[]>([]);
  const navigate = useNavigate();

  if (!me?.root?.organizations) return;

  const onSave = (draft: DraftOrganization) => {
    // validate if the draft is a valid organization
    const validation = draft.validate();
    setErrors(validation.errors);
    if (validation.errors.length > 0) {
      return;
    }

    const group = Group.create();

    me.root.organizations.push(draft as Organization);

    me.root.draftOrganization = DraftOrganization.create(
      {
        projects: ListOfProjects.create([], { owner: group }),
      },
      { owner: group },
    );

    navigate(`/organizations/${draft.id}`);
  };

  return (
    <>
      {errors && <Errors errors={errors} />}
      <CreateOrganizationForm
        id={me?.root?.draftOrganization?.id}
        onSave={onSave}
      />
    </>
  );
}

function CreateOrganizationForm({
  id,
  onSave,
}: {
  id: ID<DraftOrganization>;
  onSave: (draft: DraftOrganization) => void;
}) {
  const draft = useCoState(DraftOrganization, id);

  if (!draft) return;

  const addOrganization = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    onSave(draft);
  };

  return <OrganizationForm organization={draft} onSave={addOrganization} />;
}
