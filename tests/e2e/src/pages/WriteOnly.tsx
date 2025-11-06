import { Account, CoList, CoMap, Group, ID, coField } from "jazz-tools";
import { createInviteLink } from "jazz-tools/react";
import { useAcceptInvite, useCoState } from "jazz-tools/react";
import { useState } from "react";

class SharedCoMap extends CoMap {
  value = coField.string;
}

class SharedCoList extends CoList.Of(coField.ref(SharedCoMap)) {}

export function WriteOnlyRole() {
  const [id, setId] = useState<ID<SharedCoList> | undefined>(undefined);
  const [inviteLinks, setInviteLinks] = useState<Record<string, string>>({});
  const coList = useCoState(SharedCoList, id);

  const createCoList = async () => {
    if (id) return;

    const group = Group.create();

    const coList = SharedCoList.create([], { owner: group });

    setInviteLinks({
      writer: createInviteLink(coList, "writer"),
      reader: createInviteLink(coList, "reader"),
      admin: createInviteLink(coList, "admin"),
      writeOnly: createInviteLink(coList, "writeOnly"),
    });

    await group.$jazz.waitForSync();

    setId(coList.$jazz.id);
  };

  const addNewItem = async () => {
    if (!coList.$isLoaded) return;

    const group = coList.$jazz.owner as Group;
    const coMap = SharedCoMap.create({ value: "" }, { owner: group });

    coList.$jazz.push(coMap);
  };

  const revokeAccess = () => {
    if (!coList.$isLoaded) return;

    const coListGroup = coList.$jazz.owner as Group;

    for (const member of coListGroup.members) {
      if (
        member.account &&
        member.role !== "admin" &&
        member.account.$jazz.id !== Account.getMe().$jazz.id
      ) {
        coListGroup.removeMember(member.account);
      }
    }
  };

  useAcceptInvite({
    invitedObjectSchema: SharedCoList,
    onAccept: (id) => {
      setId(id);
    },
  });

  return (
    <div>
      <h1>Sharing</h1>
      <p data-testid="id">{coList.$jazz.id}</p>
      {Object.entries(inviteLinks).map(([role, inviteLink]) => (
        <div key={role} style={{ display: "flex", gap: 5 }}>
          <p style={{ fontWeight: "bold" }}>{role} invitation:</p>
          <p data-testid={`invite-link-${role}`}>{inviteLink}</p>
        </div>
      ))}
      <pre data-testid="values">
        {coList.$isLoaded
          ? coList.map(
              (map) =>
                map && <EditSharedCoMap key={map.$jazz.id} id={map.$jazz.id} />,
            )
          : null}
      </pre>
      {!id && <button onClick={createCoList}>Create the list</button>}
      {id && <button onClick={addNewItem}>Add a new item</button>}
      {coList && <button onClick={revokeAccess}>Revoke access</button>}
    </div>
  );
}

function EditSharedCoMap(props: { id: ID<SharedCoMap> }) {
  const coMap = useCoState(SharedCoMap, props.id, {});

  if (!coMap.$isLoaded) return null;

  return (
    <>
      <div>{coMap.value}</div>
      <input
        value={coMap.value}
        onChange={(e) => coMap.$jazz.set("value", e.target.value)}
      />
    </>
  );
}
