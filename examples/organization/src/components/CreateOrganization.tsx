import { co, Group, Loaded } from "jazz-tools";
import { useAccount, useCoState } from "jazz-tools/react";
import { useState } from "react";
import { useNavigate } from "react-router";
import {
  DraftOrganization,
  JazzAccount,
  Organization,
  Project,
  validateDraftOrganization,
} from "../schema.ts";
import { Errors } from "./Errors.tsx";
import { OrganizationForm } from "./OrganizationForm.tsx";

export function CreateOrganization() {
  const { me } = useAccount(JazzAccount, {
    resolve: { root: { draftOrganization: true, organizations: true } },
  });
  const [errors, setErrors] = useState<string[]>([]);
  const navigate = useNavigate();

  if (!me?.root?.organizations) return;

  const onSave = (draft: Loaded<typeof DraftOrganization>) => {
    const validation = validateDraftOrganization(draft);
    setErrors(validation.errors);
    if (validation.errors.length > 0) {
      return;
    }

    const group = Group.create();

    me.root.organizations.push(draft as Loaded<typeof Organization>);

    me.root.draftOrganization = DraftOrganization.create(
      {
        projects: co.list(Project).create([], { owner: group }),
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
  id: string;
  onSave: (draft: Loaded<typeof DraftOrganization>) => void;
}) {
  const draft = useCoState(DraftOrganization, id);

  if (!draft) return;

  const addOrganization = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    onSave(draft);
  };

  return <OrganizationForm organization={draft} onSave={addOrganization} />;
}
