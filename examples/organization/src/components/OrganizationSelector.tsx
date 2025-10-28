import { useAccount } from "jazz-tools/react";
import { UsersIcon } from "lucide-react";
import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router";
import { JazzAccount } from "../schema.ts";

export function OrganizationSelector({ className }: { className?: string }) {
  const { me } = useAccount(JazzAccount, {
    resolve: { root: { organizations: { $each: { $onError: null } } } },
  });

  const navigate = useNavigate();

  const paramOrganizationId = useParams<{ organizationId: string }>()
    .organizationId;

  const [organizationId, setOrganizationId] = useState<string | undefined>();

  useEffect(() => {
    if (paramOrganizationId) {
      setOrganizationId(paramOrganizationId);
    }
  }, [paramOrganizationId]);

  const onSelectOrganization = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const { value } = e.target;

    if (value === "add") {
      navigate("/#");
      return;
    }

    setOrganizationId(value);
    navigate(`/organizations/${value}`);
  };

  return (
    <div className={[className, "flex items-center gap-3"].join(" ")}>
      <label htmlFor="organization" className="md:sr-only">
        Organization
      </label>
      <UsersIcon size={32} strokeWidth={1.5} />
      <select
        id="organization"
        value={organizationId}
        onChange={onSelectOrganization}
        className="rounded-md shadow-sm dark:bg-transparent w-full"
      >
        {me?.root.organizations.map((organization) => {
          if (!organization) {
            return null;
          }

          return (
            <option key={organization.$jazz.id} value={organization.$jazz.id}>
              {organization.name}
            </option>
          );
        })}

        <option value="add">+ Create new organization</option>
      </select>
    </div>
  );
}
