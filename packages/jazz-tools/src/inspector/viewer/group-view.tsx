import {
  JsonObject,
  LocalNode,
  RawAccount,
  RawCoValue,
  RawGroup,
} from "cojson";
import { CoID } from "cojson";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../ui/table.js";
import { AccountOrGroupText } from "./account-or-group-text.js";
import { RawDataCard } from "./raw-data-card.js";
import { PageInfo, isCoId } from "./types.js";
import { Button } from "../ui/button.js";
import { Icon } from "../ui/icon.js";

function partitionMembers(data: Record<string, string>) {
  const everyone = Object.entries(data)
    .filter(([key]) => key === "everyone")
    .map(([key, value]) => ({
      id: key as CoID<RawCoValue>,
      role: value as string,
    }));

  const members = Object.entries(data)
    .filter(([key]) => isCoId(key))
    .map(([key, value]) => ({
      id: key as CoID<RawCoValue>,
      role: value,
    }));

  const parentGroups = Object.entries(data)
    .filter(([key]) => key.startsWith("parent_co_"))
    .map(([key, value]) => ({
      id: key.slice(7) as CoID<RawCoValue>,
      role: value,
    }));

  const childGroups = Object.entries(data)
    .filter(([key]) => key.startsWith("child_co_"))
    .map(([key, value]) => ({
      id: key.slice(6) as CoID<RawCoValue>,
      role: value,
    }));

  return { everyone, members, parentGroups, childGroups };
}

export function GroupView({
  coValue,
  data,
  onNavigate,
  node,
}: {
  coValue: RawCoValue;
  data: JsonObject;
  onNavigate: (pages: PageInfo[]) => void;
  node: LocalNode;
}) {
  const { everyone, members, parentGroups, childGroups } = partitionMembers(
    data as Record<string, string>,
  );

  const onRemoveMember = async (id: CoID<RawCoValue>) => {
    if (confirm("Are you sure you want to remove this member?") === false) {
      return;
    }
    try {
      const group = await node.load(coValue.id);
      if (group === "unavailable") {
        throw new Error("Group not found");
      }
      const rawGroup = group as RawGroup;
      rawGroup.removeMember(id as any);
    } catch (error) {
      console.error(error);
      throw error;
    }
  };

  const onRemoveGroup = async (id: CoID<RawCoValue>) => {
    if (confirm("Are you sure you want to remove this group?") === false) {
      return;
    }
    try {
      const group = await node.load(coValue.id);
      if (group === "unavailable") {
        throw new Error("Group not found");
      }
      const rawGroup = group as RawGroup;
      const targetGroup = await node.load(id);
      if (targetGroup === "unavailable") {
        throw new Error("Group not found");
      }
      const rawTargetGroup = targetGroup as RawGroup;
      rawGroup.revokeExtend(rawTargetGroup);
    } catch (error) {
      console.error(error);
      throw error;
    }
  };

  return (
    <>
      <Table>
        <TableHead>
          <TableRow>
            <TableHeader>Member</TableHeader>
            <TableHeader>Permission</TableHeader>
            <TableHeader></TableHeader>
          </TableRow>
        </TableHead>
        <TableBody>
          {everyone.map((member) => (
            <TableRow key={member.id}>
              <TableCell>{member.id}</TableCell>
              <TableCell>{member.role}</TableCell>
              <TableCell>
                {member.role !== "revoked" && (
                  <Button
                    variant="secondary"
                    onClick={() => onRemoveMember(member.id)}
                  >
                    <Icon name="delete" />
                  </Button>
                )}
              </TableCell>
            </TableRow>
          ))}
          {members.map((member) => (
            <TableRow key={member.id}>
              <TableCell>
                <AccountOrGroupText
                  coId={member.id}
                  node={node}
                  showId
                  onClick={() => {
                    onNavigate([{ coId: member.id, name: member.id }]);
                  }}
                />
              </TableCell>
              <TableCell>{member.role}</TableCell>
            </TableRow>
          ))}
          {parentGroups.map((group) => (
            <TableRow key={group.id}>
              <TableCell>
                <AccountOrGroupText
                  coId={group.id}
                  node={node}
                  showId
                  onClick={() => {
                    onNavigate([{ coId: group.id, name: group.id }]);
                  }}
                />
              </TableCell>
              <TableCell>{group.role}</TableCell>
              <TableCell>
                {group.role !== "revoked" && (
                  <Button
                    variant="secondary"
                    onClick={() => onRemoveGroup(group.id)}
                  >
                    <Icon name="delete" />
                  </Button>
                )}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>

      {childGroups.length > 0 && (
        <Table>
          <TableHead>
            <TableRow>
              <TableHeader>Children</TableHeader>
            </TableRow>
          </TableHead>
          <TableBody>
            {childGroups.map((group) => (
              <TableRow key={group.id}>
                <TableCell>
                  <AccountOrGroupText
                    coId={group.id}
                    node={node}
                    showId
                    onClick={() => {
                      onNavigate([{ coId: group.id, name: group.id }]);
                    }}
                  />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}

      <RawDataCard data={data} />
    </>
  );
}
