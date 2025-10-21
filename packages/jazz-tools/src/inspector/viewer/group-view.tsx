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
  data,
  onNavigate,
  node,
}: {
  data: JsonObject;
  onNavigate: (pages: PageInfo[]) => void;
  node: LocalNode;
}) {
  const { everyone, members, parentGroups, childGroups } = partitionMembers(
    data as Record<string, string>,
  );

  return (
    <>
      <Table>
        <TableHead>
          <TableRow>
            <TableHeader>Member</TableHeader>
            <TableHeader>Permission</TableHeader>
          </TableRow>
        </TableHead>
        <TableBody>
          {everyone.map((member) => (
            <TableRow key={member.id}>
              <TableCell>{member.id}</TableCell>
              <TableCell>{member.role}</TableCell>
            </TableRow>
          ))}
          {members.map((member) => (
            <TableRow key={member.id}>
              <TableCell>
                <span title="Account">👤</span>{" "}
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
                <span title="Group">👥</span>{" "}
                <Button
                  variant="link"
                  onClick={() => {
                    onNavigate([{ coId: group.id, name: group.id }]);
                  }}
                >
                  {group.id}
                </Button>
              </TableCell>
              <TableCell>{group.role}</TableCell>
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
                  <span title="Group">👥</span>{" "}
                  <Button
                    variant="link"
                    onClick={() => {
                      onNavigate([{ coId: group.id, name: group.id }]);
                    }}
                  >
                    {group.id}
                  </Button>
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
