import { Group, ID, co, z } from "jazz-tools";
import { createInviteLink } from "jazz-tools/react";
import { useAcceptInvite, useAccount, useCoState } from "jazz-tools/react";
import { useState } from "react";

const SharedCoMap = co.map({
  value: z.string(),
  get child() {
    return co.optional(SharedCoMap);
  },
});

type SharedCoMap = co.loaded<typeof SharedCoMap, true>;

export function Sharing() {
  const me = useAccount();
  const [id, setId] = useState<ID<SharedCoMap> | undefined>(undefined);
  const [revealLevels, setRevealLevels] = useState(1);
  const [inviteLinks, setInviteLinks] = useState<Record<string, string>>({});
  const coMap = useCoState(SharedCoMap, id, {});

  const createCoMap = async () => {
    if (!me.$isLoaded || id) return;

    const group = Group.create({ owner: me });

    const coMap = SharedCoMap.create(
      {
        value: "CoValue root",
      },
      { owner: group },
    );

    setInviteLinks({
      writer: createInviteLink(coMap, "writer"),
      reader: createInviteLink(coMap, "reader"),
      admin: createInviteLink(coMap, "admin"),
    });

    await group.$jazz.waitForSync();
    await group.$jazz.waitForSync();

    setId(coMap.$jazz.id);
  };

  const revokeAccess = () => {
    if (!coMap.$isLoaded) return;

    const coMapGroup = coMap.$jazz.owner as Group;

    for (const member of coMapGroup.members) {
      if (
        member.account &&
        member.role !== "admin" &&
        member.account.$jazz.id !== me.$jazz.id
      ) {
        coMapGroup.removeMember(member.account);
      }
    }
  };

  useAcceptInvite({
    invitedObjectSchema: SharedCoMap,
    onAccept: (id) => {
      setId(id);
    },
  });

  return (
    <div>
      <h1>Sharing</h1>
      <p data-testid="id">{coMap.$jazz.id}</p>
      {Object.entries(inviteLinks).map(([role, inviteLink]) => (
        <div key={role} style={{ display: "flex", gap: 5 }}>
          <p style={{ fontWeight: "bold" }}>{role} invitation:</p>
          <p data-testid={`invite-link-${role}`}>{inviteLink}</p>
        </div>
      ))}
      <pre data-testid="values">
        {coMap.$isLoaded && (
          <SharedCoMapWithChildren
            id={coMap.$jazz.id}
            level={0}
            revealLevels={revealLevels}
          />
        )}
      </pre>
      {!id && <button onClick={createCoMap}>Create the root</button>}
      {coMap.$isLoaded && <button onClick={revokeAccess}>Revoke access</button>}
      <button onClick={() => setRevealLevels(revealLevels + 1)}>
        Reveal next level
      </button>
    </div>
  );
}

function SharedCoMapWithChildren(props: {
  id: ID<SharedCoMap>;
  level: number;
  revealLevels: number;
}) {
  const coMap = useCoState(SharedCoMap, props.id);
  const nextLevel = props.level + 1;

  const addChild = () => {
    if (!coMap.$isLoaded) return;

    const group = Group.create();

    const child = SharedCoMap.create(
      { value: "CoValue child " + nextLevel },
      { owner: group },
    );
    coMap.$jazz.set("child", child);
  };

  const extendParentGroup = async () => {
    if (!coMap.$isLoaded || !coMap.child?.$isLoaded) return;

    let node: SharedCoMap = coMap;

    while (node.$jazz.refs.child?.id) {
      const parentGroup = node.$jazz.owner as Group;
      const child = await SharedCoMap.load(node.$jazz.refs.child.id);
      if (!child.$isLoaded) {
        break;
      }
      const childGroup = child.$jazz.owner as Group;
      childGroup.addMember(parentGroup);
      node = child;
    }
  };

  const shouldRenderChild = props.level < props.revealLevels;

  if (!coMap.$isLoaded) return null;

  return (
    <>
      {coMap.value}
      {coMap.$jazz.refs.child ? (
        shouldRenderChild ? (
          <>
            {" ---> "}
            <SharedCoMapWithChildren
              id={coMap.$jazz.refs.child.id}
              level={nextLevel}
              revealLevels={props.revealLevels}
            />
          </>
        ) : (
          " ---> Level hidden"
        )
      ) : (
        <button onClick={addChild}>Add a child</button>
      )}
      {props.level === 0 && (
        <button onClick={extendParentGroup}>Share the children</button>
      )}
    </>
  );
}
